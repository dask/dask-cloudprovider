from azureml.core import Experiment, RunConfiguration, ScriptRunConfig
from azureml.core.compute import AmlCompute
from azureml.train.estimator import Estimator
from azureml.core.runconfig import MpiConfiguration

import time, os, socket, subprocess, logging
import pathlib
import threading

from contextlib import suppress

import dask
from contextlib import suppress
from distributed.deploy.cluster import Cluster
from distributed.core import rpc
from distributed.utils import (
    LoopRunner,
    log_errors,
    format_bytes,
)
from tornado.ioloop import PeriodicCallback

logger = logging.getLogger(__name__)


class AzureMLCluster(Cluster):
    """ Deploy a Dask cluster using Azure ML

    This creates a dask scheduler and workers on an Azure ML Compute Target.

    Parameters
    ----------
    workspace: azureml.core.Workspace (required)
        Azure ML Workspace - see https://aka.ms/azureml/workspace

    compute_target: azureml.core.ComputeTarget (required)
        Azure ML Compute Target - see https://aka.ms/azureml/computetarget

    environment_definition: azureml.core.Environment (required)
        Azure ML Environment - see https://aka.ms/azureml/environments

    experiment_name: str (optional)
        The name of the Azure ML Experiment used to control the cluster.

        Defaults to ``dask-cloudprovider``.

    initial_node_count: int (optional)
        The initial number of nodes for the Dask Cluster.

        Defaults to ``1``.

    jupyter: bool (optional)
        Flag to start JupyterLab session on the headnode of the cluster.

        Defaults to ``False``.

    jupyter_port: int (optional)
        Port on headnode to use for hosting JupyterLab session.

        Defaults to ``9000``.

    dashboard_port: int (optional)
        Port on headnode to use for hosting Dask dashboard.

        Defaults to ``9001``.

    scheduler_port: int (optional)
        Port to map the scheduler port to via SSH-tunnel if machine not on the same VNET.

        Defaults to ``9002``.

    scheduler_idle_timeout: int (optional)
        Number of idle seconds leading to scheduler shut down.

        Defaults to ``1200`` (20 minutes).

    worker_death_timeout: int (optional)
        Number of seconds to wait for a worker to respond before removing it.

        Defaults to ``30``.

    additional_ports: list[tuple[int, int]] (optional)
        Additional ports to forward. This requires a list of tuples where the first element
        is the port to open on the headnode while the second element is the port to map to
        or forward via the SSH-tunnel.

        Defaults to ``[]``.

    admin_username: str (optional)
        Username of the admin account for the AzureML Compute.
        Required for runs that are not on the same VNET. Defaults to empty string.
        Throws Exception if machine not on the same VNET.

        Defaults to ``""``.

    admin_ssh_key: str (optional)
        Location of the SSH secret key used when creating the AzureML Compute.
        The key should be passwordless if run from a Jupyter notebook.
        The ``id_rsa`` file needs to have 0700 permissions set.
        Required for runs that are not on the same VNET. Defaults to empty string.
        Throws Exception if machine not on the same VNET.

        Defaults to ``""``.

    datastores: List[str] (optional)
        List of Azure ML Datastores to be mounted on the headnode -
        see https://aka.ms/azureml/data and https://aka.ms/azureml/datastores.

        Defaults to ``[]``. To mount all datastores in the workspace,
        set to ``[ws.datastores[datastore] for datastore in ws.datastores]``.

    telemetry_opt_out: bool (optional)
        A boolean parameter. Defaults to logging a version of AzureMLCluster
        with Microsoft. Set this flag to False if you do not want to share this
        information with Microsoft. Microsoft is not tracking anything else you
        do in your Dask cluster nor any other information related to your
        workload.

    asynchronous: bool (optional)
        Flag to run jobs asynchronously.

    **kwargs: dict
        Additional keyword arguments.
    """

    def __init__(
        self,
        workspace,
        compute_target,
        environment_definition,
        experiment_name=None,
        initial_node_count=None,
        jupyter=None,
        jupyter_port=None,
        dashboard_port=None,
        scheduler_port=None,
        scheduler_idle_timeout=None,
        worker_death_timeout=None,
        additional_ports=None,
        admin_username=None,
        admin_ssh_key=None,
        datastores=None,
        code_store=None,
        telemetry_opt_out=None,
        asynchronous=False,
        **kwargs,
    ):
        ### REQUIRED PARAMETERS
        self.workspace = workspace
        self.compute_target = compute_target
        self.environment_definition = environment_definition

        ### EXPERIMENT DEFINITION
        self.experiment_name = experiment_name
        self.tags = {"tag": "azureml-dask"}

        ### ENVIRONMENT AND VARIABLES
        self.initial_node_count = initial_node_count

        ### SEND TELEMETRY
        self.telemetry_opt_out = telemetry_opt_out
        self.telemetry_set = False

        ### GPU RUN INFO
        self.workspace_vm_sizes = AmlCompute.supported_vmsizes(self.workspace)
        self.workspace_vm_sizes = [
            (e["name"].lower(), e["gpus"]) for e in self.workspace_vm_sizes
        ]
        self.workspace_vm_sizes = dict(self.workspace_vm_sizes)

        self.compute_target_vm_size = self.compute_target.serialize()["properties"][
            "status"
        ]["vmSize"].lower()
        self.n_gpus_per_node = self.workspace_vm_sizes[self.compute_target_vm_size]
        self.use_gpu = True if self.n_gpus_per_node > 0 else False

        ### JUPYTER AND PORT FORWARDING
        self.jupyter = jupyter
        self.jupyter_port = jupyter_port
        self.dashboard_port = dashboard_port
        self.scheduler_port = scheduler_port
        self.scheduler_idle_timeout = scheduler_idle_timeout
        self.portforward_proc = None
        self.worker_death_timeout = worker_death_timeout
        self.end_logging = False  # FLAG FOR STOPPING THE port_forward_logger THREAD

        if additional_ports is not None:
            if type(additional_ports) != list:
                error_message = (
                    f"The additional_ports parameter is of {type(additional_ports)}"
                    " type but needs to be a list of int tuples."
                    " Check the documentation."
                )
                logger.exception(error_message)
                raise TypeError(error_message)

            if len(additional_ports) > 0:
                if type(additional_ports[0]) != tuple:
                    error_message = (
                        f"The additional_ports elements are of {type(additional_ports[0])}"
                        " type but needs to be a list of int tuples."
                        " Check the documentation."
                    )
                    raise TypeError(error_message)

                ### check if all elements are tuples of length two and int type
                all_correct = True
                for el in additional_ports:
                    if type(el) != tuple or len(el) != 2:
                        all_correct = False
                        break

                    if (type(el[0]), type(el[1])) != (int, int):
                        all_correct = False
                        break

                if not all_correct:
                    error_message = (
                        f"At least one of the elements of the additional_ports parameter"
                        " is wrong. Make sure it is a list of int tuples."
                        " Check the documentation."
                    )
                    raise TypeError(error_message)

        self.additional_ports = additional_ports

        self.admin_username = admin_username
        self.admin_ssh_key = admin_ssh_key
        self.scheduler_ip_port = (
            None  ### INIT FOR HOLDING THE ADDRESS FOR THE SCHEDULER
        )

        ### DATASTORES
        self.datastores = datastores

        ### FUTURE EXTENSIONS
        self.kwargs = kwargs

        ### RUNNING IN MATRIX OR LOCAL
        self.same_vnet = None

        ### GET RUNNING LOOP
        self._loop_runner = LoopRunner(loop=None, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop

        self.abs_path = pathlib.Path(__file__).parent.absolute()

        ### INITIALIZE CLUSTER
        super().__init__(asynchronous=asynchronous)

        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self.__get_defaults)

            if not self.telemetry_opt_out:
                self.__append_telemetry()

            self.sync(self.__create_cluster)

    async def __get_defaults(self):
        self.config = dask.config.get("cloudprovider.azure", {})

        if self.experiment_name is None:
            self.experiment_name = self.config.get("experiment_name")

        if self.initial_node_count is None:
            self.initial_node_count = self.config.get("initial_node_count")

        if self.jupyter is None:
            self.jupyter = self.config.get("jupyter")

        if self.jupyter_port is None:
            self.jupyter_port = self.config.get("jupyter_port")

        if self.dashboard_port is None:
            self.dashboard_port = self.config.get("dashboard_port")

        if self.scheduler_port is None:
            self.scheduler_port = self.config.get("scheduler_port")

        if self.scheduler_idle_timeout is None:
            self.scheduler_idle_timeout = self.config.get("scheduler_idle_timeout")

        if self.worker_death_timeout is None:
            self.worker_death_timeout = self.config.get("worker_death_timeout")

        if self.additional_ports is None:
            self.additional_ports = self.config.get("additional_ports")

        if self.admin_username is None:
            self.admin_username = self.config.get("admin_username")

        if self.admin_ssh_key is None:
            self.admin_ssh_key = self.config.get("admin_ssh_key")

        if self.datastores is None:
            self.datastores = self.config.get("datastores")

        if self.telemetry_opt_out is None:
            self.telemetry_opt_out = self.config.get("telemetry_opt_out")

        ### PARAMETERS TO START THE CLUSTER
        self.scheduler_params = {}
        self.worker_params = {}

        ### scheduler and worker parameters
        self.scheduler_params["--jupyter"] = self.jupyter
        self.scheduler_params["--scheduler_idle_timeout"] = self.scheduler_idle_timeout
        self.worker_params["--worker_death_timeout"] = self.worker_death_timeout

        if self.use_gpu:
            self.scheduler_params["--use_gpu"] = True
            self.scheduler_params["--n_gpus_per_node"] = self.n_gpus_per_node
            self.worker_params["--use_gpu"] = True
            self.worker_params["--n_gpus_per_node"] = self.n_gpus_per_node

        ### CLUSTER PARAMS
        self.max_nodes = self.compute_target.serialize()["properties"]["properties"][
            "scaleSettings"
        ]["maxNodeCount"]
        self.scheduler_ip_port = None
        self.workers_list = []
        self.URLs = {}

        ### SANITY CHECKS
        ###-----> initial node count
        if self.initial_node_count > self.max_nodes:
            self.initial_node_count = self.max_nodes

    def __append_telemetry(self):
        if not self.telemetry_set:
            self.telemetry_set = True
            try:
                from azureml._base_sdk_common.user_agent import append

                append("AzureMLCluster-DASK", "0.1")
            except ImportError:
                pass

    def __print_message(self, msg, length=80, filler="#", pre_post=""):
        logger.info(msg)
        print(f"{pre_post} {msg} {pre_post}".center(length, filler))

    async def __check_if_scheduler_ip_reachable(self):
        """
        Private method to determine if running in the cloud within the same VNET
        and the scheduler node is reachable
        """
        try:
            ip, port = self.scheduler_ip_port.split(":")
            socket.create_connection((ip, port), 10)
            self.same_vnet = True
            self.__print_message("On the same VNET")
            logger.info("On the same VNET")
        except socket.timeout as e:

            self.__print_message("Not on the same VNET")
            logger.info("On the same VNET")
            self.same_vnet = False
        except ConnectionRefusedError as e:
            logger.info(e)
            pass

    def __prepare_rpc_connection_to_headnode(self):
        if not self.same_vnet:
            if self.admin_username == "" or self.admin_ssh_key == "":
                message = "Your machine is not at the same VNET as the cluster. "
                message += "You need to set admin_username and admin_ssh_key. Check documentation."
                logger.exception(message)
                raise Exception(message)
            else:
                uri = f"localhost:{self.scheduler_port}"
                logger.info(f"Local connection: {uri}")
                return uri
        else:
            return self.run.get_metrics()["scheduler"]

    async def __create_cluster(self):
        self.__print_message("Setting up cluster")
        exp = Experiment(self.workspace, self.experiment_name)
        estimator = Estimator(
            os.path.join(self.abs_path, "setup"),
            compute_target=self.compute_target,
            entry_script="start_scheduler.py",
            environment_definition=self.environment_definition,
            script_params=self.scheduler_params,
            node_count=1,  ### start only scheduler
            distributed_training=MpiConfiguration(),
            use_docker=True,
            inputs=self.datastores,
        )

        run = exp.submit(estimator, tags=self.tags)

        self.__print_message("Waiting for scheduler node's IP")
        while (
            run.get_status() != "Canceled"
            and run.get_status() != "Failed"
            and "scheduler" not in run.get_metrics()
        ):
            print(".", end="")
            logger.info("Scheduler not ready")
            time.sleep(5)

        if run.get_status() == "Canceled" or run.get_status() == "Failed":
            logger.exception("Failed to start the AzureML cluster")
            raise Exception("Failed to start the AzureML cluster.")

        print("\n\n")

        ### SET FLAGS
        self.scheduler_ip_port = run.get_metrics()["scheduler"]
        self.worker_params["--scheduler_ip_port"] = self.scheduler_ip_port
        self.__print_message(f'Scheduler: {run.get_metrics()["scheduler"]}')
        self.run = run

        logger.info(f'Scheduler: {run.get_metrics()["scheduler"]}')

        ### CHECK IF ON THE SAME VNET
        while self.same_vnet is None:
            await self.sync(self.__check_if_scheduler_ip_reachable)
            time.sleep(1)

        ### REQUIRED BY dask.distributed.deploy.cluster.Cluster
        _scheduler = self.__prepare_rpc_connection_to_headnode()
        self.scheduler_comm = rpc(_scheduler)
        await self.sync(self.__setup_port_forwarding)
        await self.sync(super()._start)
        await self.sync(self.__update_links)

        self.__print_message("Connections established")
        self.__print_message(f"Scaling to {self.initial_node_count} workers")

        if self.initial_node_count > 1:
            self.scale(
                self.initial_node_count
            )  # LOGIC TO KEEP PROPER TRACK OF WORKERS IN `scale`
        self.__print_message(f"Scaling is done")

    async def __update_links(self):
        token = self.run.get_metrics()["token"]

        if self.same_vnet:
            hostname = socket.gethostname()
            location = self.workspace.get_details()["location"]

            self.scheduler_info[
                "dashboard_url"
            ] = f"https://{hostname}-{self.dashboard_port}.{location}.instances.azureml.net/status"

            self.scheduler_info[
                "jupyter_url"
            ] = f"https://{hostname}-{self.jupyter_port}.{location}.instances.azureml.net/lab?token={token}"
        else:
            hostname = "localhost"
            self.scheduler_info[
                "dashboard_url"
            ] = f"http://{hostname}:{self.dashboard_port}"
            self.scheduler_info[
                "jupyter_url"
            ] = f"http://{hostname}:{self.jupyter_port}/?token={token}"

        logger.info(f'Dashboard URL: {self.scheduler_info["dashboard_url"]}')
        logger.info(f'Jupyter URL:   {self.scheduler_info["jupyter_url"]}')

    def __port_forward_logger(self, portforward_proc):
        portforward_log = open("portforward_out_log.txt", "w")

        while True:
            portforward_out = portforward_proc.stdout.readline()
            if portforward_proc != "":
                portforward_log.write(portforward_out)
                portforward_log.flush()

            if self.end_logging:
                break
        return

    async def __setup_port_forwarding(self):
        dashboard_address = self.run.get_metrics()["dashboard"]
        jupyter_address = self.run.get_metrics()["jupyter"]
        scheduler_ip = self.run.get_metrics()["scheduler"].split(":")[0]

        if self.same_vnet:
            os.system(
                f"killall socat"
            )  # kill all socat processes - cleans up previous port forward setups
            os.system(
                f"setsid socat tcp-listen:{self.dashboard_port},reuseaddr,fork tcp:{dashboard_address} &"
            )
            os.system(
                f"setsid socat tcp-listen:{self.jupyter_port},reuseaddr,fork tcp:{jupyter_address} &"
            )

            ### map additional ports
            for port in self.additional_ports:
                os.system(
                    f"setsid socat tcp-listen:{self.port[1]},reuseaddr,fork tcp:{scheduler_ip}:{port[0]} &"
                )
        else:
            scheduler_public_ip = self.compute_target.list_nodes()[0]["publicIpAddress"]
            scheduler_public_port = self.compute_target.list_nodes()[0]["port"]
            self.__print_message("scheduler_public_ip: {}".format(scheduler_public_ip))
            self.__print_message(
                "scheduler_public_port: {}".format(scheduler_public_port)
            )

            cmd = (
                "ssh -vvv -o StrictHostKeyChecking=no -N"
                f" -i {os.path.expanduser(self.admin_ssh_key)}"
                f" -L 0.0.0.0:{self.jupyter_port}:{scheduler_ip}:8888"
                f" -L 0.0.0.0:{self.dashboard_port}:{scheduler_ip}:8787"
                f" -L 0.0.0.0:{self.scheduler_port}:{scheduler_ip}:8786"
            )

            for port in self.additional_ports:
                cmd += f" -L 0.0.0.0:{port[1]}:{scheduler_ip}:{port[0]}"

            cmd += f" {self.admin_username}@{scheduler_public_ip} -p {scheduler_public_port}"

            self.portforward_proc = subprocess.Popen(
                cmd.split(),
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

            ### Starting thread to keep the SSH tunnel open on Windows
            portforward_logg = threading.Thread(
                target=self.__port_forward_logger, args=[self.portforward_proc]
            )
            portforward_logg.start()

    @property
    def dashboard_link(self):
        """ Link to Dask dashboard.
        """
        try:
            link = self.scheduler_info["dashboard_url"]
        except KeyError:
            return ""
        else:
            return link

    @property
    def jupyter_link(self):
        """ Link to JupyterLab on running on the headnode of the cluster.
        Set ``jupyter=True`` when creating the ``AzureMLCluster``.
        """
        try:
            link = self.scheduler_info["jupyter_url"]
        except KeyError:
            return ""
        else:
            return link

    def _format_nodes(self, nodes, requested, use_gpu, n_gpus_per_node=None):
        if use_gpu:
            if nodes == requested:
                return f"{nodes}"
            else:
                return f"{nodes} / {requested}"
        else:
            if nodes == requested:
                return f"{nodes}"
            else:
                return f"{nodes} / {requested}"

    def _widget_status(self):
        ### reporting proper number of nodes vs workers in a multi-GPU worker scenario
        nodes = len(self.scheduler_info["workers"])

        if self.use_gpu:
            nodes = int(nodes / self.n_gpus_per_node)
        if hasattr(self, "worker_spec"):
            requested = sum(
                1 if "group" not in each else len(each["group"])
                for each in self.worker_spec.values()
            )

        elif hasattr(self, "nodes"):
            requested = len(self.nodes)
        else:
            requested = nodes

        nodes = self._format_nodes(nodes, requested, self.use_gpu, self.n_gpus_per_node)

        cores = sum(v["nthreads"] for v in self.scheduler_info["workers"].values())
        cores_or_gpus = "Workers (GPUs)" if self.use_gpu else "Workers (vCPUs)"

        memory = (
            sum(
                v["gpu"]["memory-total"][0]
                for v in self.scheduler_info["workers"].values()
            )
            if self.use_gpu
            else sum(v["memory_limit"] for v in self.scheduler_info["workers"].values())
        )
        memory = format_bytes(memory)

        text = """
<div>
  <style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }
    .dataframe tbody tr th {
        vertical-align: top;
    }
    .dataframe thead th {
        text-align: right;
    }
  </style>
  <table style="text-align: right;">
    <tr> <th>Nodes</th> <td>%s</td></tr>
    <tr> <th>%s</th> <td>%s</td></tr>
    <tr> <th>Memory</th> <td>%s</td></tr>
  </table>
</div>
""" % (
            nodes,
            cores_or_gpus,
            cores,
            memory,
        )
        return text

    def _widget(self):
        """ Create IPython widget for display within a notebook """
        try:
            return self._cached_widget
        except AttributeError:
            pass

        try:
            from ipywidgets import Layout, VBox, HBox, IntText, Button, HTML, Accordion
        except ImportError:
            self._cached_widget = None
            return None

        layout = Layout(width="150px")

        if self.dashboard_link:
            dashboard_link = (
                '<p><b>Dashboard: </b><a href="%s" target="_blank">%s</a></p>\n'
                % (self.dashboard_link, self.dashboard_link,)
            )
        else:
            dashboard_link = ""

        if self.jupyter_link:
            jupyter_link = (
                '<p><b>Jupyter: </b><a href="%s" target="_blank">%s</a></p>\n'
                % (self.jupyter_link, self.jupyter_link,)
            )
        else:
            jupyter_link = ""

        title = "<h2>%s</h2>" % self._cluster_class_name
        title = HTML(title)
        dashboard = HTML(dashboard_link)
        jupyter = HTML(jupyter_link)

        status = HTML(self._widget_status(), layout=Layout(min_width="150px"))

        if self._supports_scaling:
            request = IntText(
                self.initial_node_count, description="Nodes", layout=layout
            )
            scale = Button(description="Scale", layout=layout)

            minimum = IntText(0, description="Minimum", layout=layout)
            maximum = IntText(0, description="Maximum", layout=layout)
            adapt = Button(description="Adapt", layout=layout)

            accordion = Accordion(
                [HBox([request, scale]), HBox([minimum, maximum, adapt])],
                layout=Layout(min_width="500px"),
            )
            accordion.selected_index = None
            accordion.set_title(0, "Manual Scaling")
            accordion.set_title(1, "Adaptive Scaling")

            def adapt_cb(b):
                self.adapt(minimum=minimum.value, maximum=maximum.value)
                update()

            adapt.on_click(adapt_cb)

            def scale_cb(b):
                with log_errors():
                    n = request.value
                    with suppress(AttributeError):
                        self._adaptive.stop()
                    self.scale(n)
                    update()

            scale.on_click(scale_cb)
        else:
            accordion = HTML("")

        box = VBox([title, HBox([status, accordion]), jupyter, dashboard])

        self._cached_widget = box

        def update():
            self.close_when_disconnect()
            status.value = self._widget_status()

        pc = PeriodicCallback(update, 500)  # , io_loop=self.loop)
        self.periodic_callbacks["cluster-repr"] = pc
        pc.start()

        return box

    def close_when_disconnect(self):
        if (
            self.run.get_status() == "Canceled"
            or self.run.get_status() == "Completed"
            or self.run.get_status() == "Failed"
        ):
            self.scale_down(len(self.workers_list))

    def scale(self, workers=1):
        """ Scale the cluster. Scales to a maximum of the workers available in the cluster.
        """
        if workers <= 0:
            self.close()
            return

        count = len(self.workers_list) + 1  # one more worker in head node

        if count < workers:
            self.scale_up(workers - count)
        elif count > workers:
            self.scale_down(count - workers)
        else:
            self.__print_message(f"Number of workers: {workers}")

    # scale up
    def scale_up(self, workers=1):
        """ Scale up the number of workers.
        """
        run_config = RunConfiguration()
        run_config.target = self.compute_target
        run_config.environment = self.environment_definition

        scheduler_ip = self.run.get_metrics()["scheduler"]
        args = [
            f"--scheduler_ip_port={scheduler_ip}",
            f"--use_gpu={self.use_gpu}",
            f"--n_gpus_per_node={self.n_gpus_per_node}",
            f"--worker_death_timeout={self.worker_death_timeout}",
        ]

        child_run_config = ScriptRunConfig(
            source_directory=os.path.join(self.abs_path, "setup"),
            script="start_worker.py",
            arguments=args,
            run_config=run_config,
        )

        for i in range(workers):
            child_run = self.run.submit_child(child_run_config, tags=self.tags)
            self.workers_list.append(child_run)

    # scale down
    def scale_down(self, workers=1):
        """ Scale down the number of workers. Scales to minimum of 1.
        """
        for i in range(workers):
            if self.workers_list:
                child_run = self.workers_list.pop(0)  # deactive oldest workers
                child_run.complete()  # complete() will mark the run "Complete", but won't kill the process
                child_run.cancel()
            else:
                self.__print_message("All scaled workers are removed.")

    # close cluster
    async def _close(self):
        if self.status == "closed":
            return
        while self.workers_list:
            child_run = self.workers_list.pop()
            child_run.complete()
            child_run.cancel()

        if self.run:
            self.run.complete()
            self.run.cancel()

        self.status = "closed"
        self.__print_message("Scheduler and workers are disconnected.")

        if self.portforward_proc is not None:
            ### STOP LOGGING SSH
            self.portforward_proc.terminate()
            self.end_logging = True

        time.sleep(30)
        await super()._close()

    def close(self):
        """ Close the cluster. All Azure ML Runs corresponding to the scheduler
        and worker processes will be completed. The Azure ML Compute Target will
        return to its minimum number of nodes after its idle time before scaledown.
        """
        return self.sync(self._close)
