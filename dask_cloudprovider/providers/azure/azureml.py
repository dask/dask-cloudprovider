from azureml.core import Experiment, RunConfiguration, ScriptRunConfig
from azureml.train.estimator import Estimator
from azureml.core.runconfig import MpiConfiguration
import time, os, socket, subprocess

from distributed.deploy.cluster import Cluster
from distributed.core import rpc

import dask
import pathlib

from distributed.utils import (
    LoopRunner,
    PeriodicCallback,
    log_errors,
    ignoring,
    format_bytes
)

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

    use_gpu: bool (optional)
        Flag indicating whether to setup cluster for using GPUs.

        Defaults to ``False``.

    n_gpus_per_node: int (optional)
        Number of GPUs per node in the Azure ML Compute Target.

        Defaults to ``0``.

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
        set to ``list(workspace.datastores)``.

    asynchronous: bool (optional)
        Flag to run jobs asynchronously.

    **kwargs: dict
        Additional keyword arguments.

    Example | ``AzureMLCluster`` for Dask Client.
    See https://aka.ms/azureml/dask.
    ----------
    ```
    from azureml.core import Workspace
    from dask.distributed import Client
    from dask_cloudprovider import AzureMLCluster

    ws = Workspace.from_config()

    cluster = AzureMLCluster(ws,
                             ws.compute_targets['dask-ct'],
                             ws.environments['dask-env'])

    client = Client(cluster)
    ```

    Example | ``AzureMLCluster`` for interactive JupyterLab session.
    See https://aka.ms/azureml/dask.
    ----------
    ```
    from azureml.core import Workspace
    from dask_cloudprovider import AzureMLCluster

    ws = Workspace.from_config()

    cluster = AzureMLCluster(ws,
                             ws.compute_targets['dask-ct'],
                             ws.environments['dask-env'],
                             datastores=list(ws.datastores),
                             jupyter=True)

    print(cluster.jupyter_link)
    print(cluster.dashboard_link)
    ```
    """

    def __init__(
        self
        , workspace                     # AzureML workspace object
        , compute_target                # AzureML compute object
        , environment_definition        # AzureML Environment object
        , experiment_name=None          # name of the experiment to start
        , initial_node_count=None       # initial node count, must be less than
                                        # or equal to AzureML compute object max nodes
                                        # will default to max nodes if more than
        , use_gpu=None                  # flag to indicate GPU vs CPU cluster
        , n_gpus_per_node=None          # number of GPUs per node if use_GPU flag set
        , jupyter=None                  # start Jupyter lab process on headnode
        , jupyter_port=None             # port to forward the Jupyter process to
        , dashboard_port=None           # port to forward Dask dashboard to
        , scheduler_port=None           # port to map the scheduler port to for 'local' runs
        , additional_ports=None         # list of tuples of additional ports to forward
        , admin_username=None           # username to log in to the AzureML Training Cluster for 'local' runs
        , admin_ssh_key=None            # path to private SSH key used to log in to the
                                        # AzureML Training Cluster for 'local' runs
        , datastores=None               # datastores specs
        , code_store=None               # name of the code store if specified
        , asynchronous=False            # flag to run jobs in an asynchronous way
        , **kwargs
    ):
        ### REQUIRED PARAMETERS
        self.workspace = workspace
        self.compute_target = compute_target
        self.environment_definition = environment_definition

        ### EXPERIMENT DEFINITION
        self.experiment_name = experiment_name

        ### ENVIRONMENT AND VARIABLES
        self.initial_node_count = initial_node_count

        ### GPU RUN INFO
        self.use_gpu = use_gpu
        self.n_gpus_per_node = n_gpus_per_node

        ### JUPYTER AND PORT FORWARDING
        self.jupyter = jupyter
        self.jupyter_port = jupyter_port
        self.dashboard_port = dashboard_port
        self.scheduler_port = scheduler_port

        if additional_ports is not None:
            if type(additional_ports) != list:
                error_message = (
                    f'The additional_ports parameter is of {type(additional_ports)}'
                    ' type but needs to be a list of int tuples.'
                    ' Check the documentation.'
                )
                raise TypeError(error_message)

            if len(additional_ports) > 0:
                if type(additional_ports[0]) != tuple:
                    error_message = (
                        f'The additional_ports elements are of {type(additional_ports[0])}'
                        ' type but needs to be a list of int tuples.'
                        ' Check the documentation.'
                    )
                    raise TypeError(error_message)

                ### check if all elements are tuples of length two and int type
                all_correct = True
                for el in additional_ports:
                    if (
                        type(el) != tuple 
                        or len(el) != 2
                    ):
                        all_correct = False
                        break

                    if (type(el[0]), type(el[1])) != (int, int):
                        all_correct = False
                        break

                if not all_correct:
                    error_message = (
                        f'At least one of the elements of the additional_ports parameter'
                        ' is wrong. Make sure it is a list of int tuples.'
                        ' Check the documentation.'
                    )
                    raise TypeError(error_message)
        self.additional_ports = additional_ports

        self.admin_username = admin_username
        self.admin_ssh_key = admin_ssh_key
        self.scheduler_ip_port = None   ### INIT FOR HOLDING THE ADDRESS FOR THE SCHEDULER

        ### DATASTORES
        self.datastores = datastores
        self.code_store = code_store

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
            self.sync(self.get_defaults)
            self.sync(self.create_cluster)

    async def get_defaults(self):
        self.config = dask.config.get("cloudprovider.azure", {})

        if self.experiment_name is None:
            self.experiment_name = self.config.get("experiment_name")

        if self.initial_node_count is None:
            self.initial_node_count = self.config.get("initial_node_count")

        if self.use_gpu is None:
            self.use_gpu = self.config.get("use_gpu")

        if self.n_gpus_per_node is None:
            self.n_gpus_per_node = self.config.get("n_gpus_per_node")

        if self.jupyter is None:
            self.jupyter = self.config.get("jupyter")

        if self.jupyter_port is None:
            self.jupyter_port = self.config.get("jupyter_port")

        if self.dashboard_port is None:
            self.dashboard_port = self.config.get("dashboard_port")

        if self.scheduler_port is None:
            self.scheduler_port = self.config.get("scheduler_port")

        if self.additional_ports is None:
            self.additional_ports = self.config.get("additional_ports")

        if self.admin_username is None:
            self.admin_username = self.config.get("admin_username")

        if self.admin_ssh_key is None:
            self.admin_ssh_key = self.config.get("admin_ssh_key")

        if self.datastores is None:
            self.datastores = self.config.get("datastores")

        if self.code_store is None:
            self.code_store = self.config.get("code_store")

        ### PARAMETERS TO START THE CLUSTER
        self.scheduler_params = {}
        self.worker_params = {}

        ### scheduler and worker parameters
        self.scheduler_params['--jupyter'] = True
        if self.code_store is not None:
            self.scheduler_params['--code_store'] = self.code_store

        if self.use_gpu:
            self.scheduler_params['--use_gpu'] = True
            self.scheduler_params['--n_gpus_per_node'] = self.n_gpus_per_node
            self.worker_params['--use_gpu'] = True
            self.worker_params['--n_gpus_per_node'] = self.n_gpus_per_node

        ### CLUSTER PARAMS
        self.max_nodes = (
            self
            .compute_target
            .serialize()
            ['properties']
            ['properties']
            ['scaleSettings']
            ['maxNodeCount']
        )
        self.scheduler_ip_port = None
        self.workers_list = []
        self.URLs = {}

        ### SANITY CHECKS
        ###-----> initial node count
        if self.initial_node_count > self.max_nodes:
            self.initial_node_count = self.max_nodes

    def __print_message(self, msg, length=80, filler='#', pre_post=''):
        print(f'{pre_post} {msg} {pre_post}'.center(length, filler))

    def __get_estimator(self):
        raise NotImplementedError('This method has not been implemented yet.')

    async def __check_if_scheduler_ip_reachable(self):
        """
        Private method to determine if running in the cloud within the same VNET
        and the scheduler node is reachable
        """
        try:
            ip, port = self.scheduler_ip_port.split(':')
            socket.create_connection((ip, port), 10)
            self.same_vnet = True
            self.__print_message('On the same VNET')
        except socket.timeout as e:

            self.__print_message('Not on the same VNET')
            self.same_vnet = False
        except ConnectionRefusedError as e:
            pass

    def __prepare_rpc_connection_to_headnode(self):
        if not self.same_vnet:
            if (self.admin_username == "" or self.admin_ssh_key == ""):
                message = "Your machine is not at the same VNET as the cluster. "
                message += "You need to set admin_username and admin_ssh_key. Check documentation."
                raise Exception(message)
            else:
                return f"{socket.gethostname()}:{self.scheduler_port}"
        else:
            return self.run.get_metrics()["scheduler"]

    async def create_cluster(self):
        # set up environment
        self.__print_message('Setting up cluster')

        # submit run
        self.__print_message('Submitting the experiment')
        exp = Experiment(self.workspace, self.experiment_name)
        estimator = Estimator(
            os.path.join(self.abs_path, 'setup')
            , compute_target=self.compute_target
            , entry_script='start_scheduler.py'
            , environment_definition=self.environment_definition
            , script_params=self.scheduler_params
            , node_count=1 ### start only scheduler
            , distributed_training=MpiConfiguration()
            , use_docker=True
            , inputs=self.datastores
        )

        run = exp.submit(estimator)

        self.__print_message("Waiting for scheduler node's IP")
        while (
            run.get_status() != 'Canceled'
            and run.get_status() != 'Failed'
            and 'scheduler' not in run.get_metrics()
        ):
            print('.', end="")
            time.sleep(5)

        if run.get_status() == 'Canceled' or run.get_status() == 'Failed':
            raise Exception('Failed to start the AzureML cluster.')

        print('\n\n')

        ### SET FLAGS
        self.scheduler_ip_port = run.get_metrics()["scheduler"]
        self.worker_params['--scheduler_ip_port'] = self.scheduler_ip_port
        self.__print_message(f'Scheduler: {run.get_metrics()["scheduler"]}')
        self.run = run

        ### CHECK IF ON THE SAME VNET
        while(self.same_vnet is None):
            await self.sync(self.__check_if_scheduler_ip_reachable)
            time.sleep(1)

        ### REQUIRED BY dask.distributed.deploy.cluster.Cluster
        _scheduler = self.__prepare_rpc_connection_to_headnode()
        self.scheduler_comm = rpc(_scheduler)
        await self.sync(self.__setup_port_forwarding)
        await self.sync(super()._start)
        await self.sync(self.__update_links)

        self.__print_message('Connections established')

        self.__print_message(f'Scaling to {self.initial_node_count} workers')
        if self.initial_node_count > 1:
            self.scale(self.initial_node_count)   # LOGIC TO KEEP PROPER TRACK OF WORKERS IN `scale`
        self.__print_message(f'Scaling is done')

    async def __update_links(self):
        hostname = socket.gethostname()
        location = self.workspace.get_details()["location"]
        token = self.run.get_metrics()["token"]

        if self.same_vnet:
            self.scheduler_info['dashboard_url'] = (
                f'https://{hostname}-{self.dashboard_port}.{location}.instances.azureml.net/status'
            )

            self.scheduler_info['jupyter_url'] = (
                f'https://{hostname}-{self.jupyter_port}.{location}.instances.azureml.net/lab?token={token}'
            )
        else:
            self.scheduler_info['dashboard_url'] = f'http://{hostname}:{self.dashboard_port}'
            self.scheduler_info['jupyter_url'] = f'http://{hostname}:{self.jupyter_port}/?token={token}'

    async def __setup_port_forwarding(self):
        dashboard_address = self.run.get_metrics()["dashboard"]
        jupyter_address = self.run.get_metrics()["jupyter"]
        scheduler_ip = self.run.get_metrics()["scheduler"].split(':')[0]

        if self.same_vnet:
            os.system(f'killall socat') # kill all socat processes - cleans up previous port forward setups
            os.system(f'setsid socat tcp-listen:{self.dashboard_port},reuseaddr,fork tcp:{dashboard_address} &')
            os.system(f'setsid socat tcp-listen:{self.jupyter_port},reuseaddr,fork tcp:{jupyter_address} &')

            ### map additional ports
            for port in self.additional_ports:
                os.system(f'setsid socat tcp-listen:{self.port[1]},reuseaddr,fork tcp:{scheduler_ip}:{port[0]} &')
        else:
            scheduler_public_ip = self.compute_target.list_nodes()[0]['publicIpAddress']
            scheduler_public_port = self.compute_target.list_nodes()[0]['port']

            cmd = (
                "ssh -vvv -o StrictHostKeyChecking=no -N"
                f" -i {self.admin_ssh_key}"
                f" -L 0.0.0.0:{self.jupyter_port}:{scheduler_ip}:8888"
                f" -L 0.0.0.0:{self.dashboard_port}:{scheduler_ip}:8787"
                f" -L 0.0.0.0:{self.scheduler_port}:{scheduler_ip}:8786"
            )

            for port in self.additional_ports:
                cmd += f" -L 0.0.0.0:{port[1]}:{scheduler_ip}:{port[0]}"

            cmd += f" {self.admin_username}@{scheduler_public_ip} -p {scheduler_public_port}"

            portforward_log = open("portforward_out_log.txt", 'w')
            portforward_proc = (
                subprocess
                .Popen(
                    cmd.split()
                    , universal_newlines=True
                    , stdout=subprocess.PIPE
                    , stderr=subprocess.STDOUT
                )
            )

    @property
    def dashboard_link(self):
        """ Link to Dask dashboard.

        Example
        ----------
        print(cluster.dashboard_link)
        """
        try:
            link = self.scheduler_info['dashboard_url']
        except KeyError:
            return ""
        else:
            return link

    @property
    def jupyter_link(self):
        """ Link to JupyterLab on running on the headnode of the cluster.
        Set ``jupyter=True`` when creating the ``AzureMLCluster``.

        Example
        ----------
        print(cluster.jupyter_link)
        """
        try:
            link = self.scheduler_info['jupyter_url']
        except KeyError:
            return ""
        else:
            return link

    def _format_nodes(self, nodes, requested, use_gpu, n_gpus_per_node=None):

        if use_gpu:
            if nodes == requested:
                return f'{nodes}'
            else:
                return f'{nodes} / {requested}'

        else:
            if nodes == requested:
                return f'{nodes}'
            else:
                return f'{nodes} / {requested}'

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
        cores_or_gpus = 'Workers (GPUs)' if self.use_gpu else 'Workers (vCPUs)'

        memory = (
            sum(sum(v['gpu']["memory-total"]) for v in self.scheduler_info["workers"].values()) if self.use_gpu
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
            dashboard_link = '<p><b>Dashboard: </b><a href="%s" target="_blank">%s</a></p>\n' % (
                self.dashboard_link,
                self.dashboard_link,
            )
        else:
            dashboard_link = ""

        if self.jupyter_link:
            jupyter_link = '<p><b>Jupyter: </b><a href="%s" target="_blank">%s</a></p>\n' % (
                self.jupyter_link,
                self.jupyter_link,
            )
        else:
            jupyter_link = ""

        title = "<h2>%s</h2>" % self._cluster_class_name
        title = HTML(title)
        dashboard = HTML(dashboard_link)
        jupyter = HTML(jupyter_link)

        status = HTML(self._widget_status(), layout=Layout(min_width="150px"))

        if self._supports_scaling:
            request = IntText(self.initial_node_count, description="Nodes", layout=layout)
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
                    with ignoring(AttributeError):
                        self._adaptive.stop()
                    self.scale(n)
                    update()

            scale.on_click(scale_cb)
        else:
            accordion = HTML("")

        box = VBox([title, HBox([status, accordion]), jupyter, dashboard])

        self._cached_widget = box

        def update():
            status.value = self._widget_status()

        pc = PeriodicCallback(update, 500, io_loop=self.loop)
        self.periodic_callbacks["cluster-repr"] = pc
        pc.start()

        return box

    def print_links_ComputeVM(self):
        self.__print_message(f"DASHBOARD: {self.scheduler_info['dashboard_url']}")

        self.__print_message(f"NOTEBOOK: {self.scheduler_info['jupyter_url']}")

    def scale(self, workers=1):
        """ Scale the cluster. We can add or reduce the number workers
        of a given configuration

        Example
        ----------
        ```python
        cluster.scale(2)
        ```
        """
        if workers <= 0:
            self.close()
            return

        count = len(self.workers_list) + 1 # one more worker in head node

        if count < workers:
            self.scale_up(workers - count)
        elif count > workers:
            self.scale_down(count - workers)
        else:
            print(f'Number of workers: {workers}')

    # scale up
    def scale_up(self, workers=1):
        run_config = RunConfiguration()
        run_config.target=self.compute_target
        run_config.environment=self.environment_definition

        scheduler_ip=self.run.get_metrics()["scheduler"]
        args=[f'--scheduler_ip_port={scheduler_ip}', f'--use_gpu={self.use_gpu}', f'--n_gpus_per_node={self.n_gpus_per_node}']                    

        child_run_config=ScriptRunConfig(
            source_directory=os.path.join(self.abs_path, 'setup'),
            script='start_worker.py',
            arguments=args,
            run_config=run_config)

        for i in range(workers):
            child_run=self.run.submit_child(child_run_config)
            self.workers_list.append(child_run)

    # scale down
    def scale_down(self, workers=1):
        for i in range(workers):
            if self.workers_list:
                child_run = self.workers_list.pop(0) #deactive oldest workers
                child_run.complete() # complete() will mark the run "Complete", but won't kill the process
                child_run.cancel()
            else:
                print("All scaled workers are removed.")

    # close cluster
    async def _close(self):
        if(self.status == "closed"):
            return
        while self.workers_list:
            child_run = self.workers_list.pop()
            child_run.complete()
            child_run.cancel()

        if self.run:
            self.run.complete()
            self.run.cancel()

        await super()._close()
        self.status = "closed"
        self.__print_message("Scheduler and workers are disconnected.")

    def close(self):
        """ Close the cluster. All Azure ML Runs corresponding to the scheduler
        and worker processes will be completed. The Azure ML Compute Target will
        return to its minimum number of nodes after its idle time before scaledown.

        Example
        ----------
        cluster.close()
        """
        self.sync(self._close)
