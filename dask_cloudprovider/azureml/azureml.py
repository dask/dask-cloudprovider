import time, os, socket, subprocess, logging
import pathlib
import threading
import warnings

from contextlib import suppress

import dask
from contextlib import suppress
from distributed.deploy.cluster import Cluster
from distributed.core import rpc
from distributed.utils import LoopRunner, log_errors, format_bytes
from tornado.ioloop import PeriodicCallback

try:
    from azureml.core import Experiment, RunConfiguration, ScriptRunConfig
    from azureml.core.compute import AmlCompute, ComputeTarget
    from azureml.core.compute_target import ComputeTargetException
    from azureml.train.estimator import Estimator
    from azureml.core.runconfig import MpiConfiguration
except ImportError as e:
    msg = (
        "Dask Cloud Provider Azure ML requirements are not installed.\n\n"
        "Please either pip install as follows:\n\n"
        '  python -m pip install "dask-cloudprovider[azureml]" --upgrade  # or python -m pip install'
    )
    raise ImportError(msg) from e


logger = logging.getLogger(__name__)


class AzureMLCluster(Cluster):
    """Deploy a Dask cluster using Azure ML

    This creates a dask scheduler and workers on an Azure ML Compute Target.

    Parameters
    ----------
    workspace: azureml.core.Workspace (required)
        Azure ML Workspace - see https://aka.ms/azureml/workspace.

    vm_size: str (optional)
        Azure VM size to be used in the Compute Target - see https://aka.ms/azureml/vmsizes.

    datastores: List[Datastore] (optional)
        List of Azure ML Datastores to be mounted on the headnode -
        see https://aka.ms/azureml/data and https://aka.ms/azureml/datastores.

        Defaults to ``[]``. To mount all datastores in the workspace,
        set to ``ws.datastores.values()``.

    environment_definition: azureml.core.Environment (optional)
        Azure ML Environment - see https://aka.ms/azureml/environments.

        Defaults to the "AzureML-Dask-CPU" or "AzureML-Dask-GPU" curated environment.

    scheduler_idle_timeout: int (optional)
        Number of idle seconds leading to scheduler shut down.

        Defaults to ``1200`` (20 minutes).

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

    worker_death_timeout: int (optional)
        Number of seconds to wait for a worker to respond before removing it.

        Defaults to ``30``.

    additional_ports: list[tuple[int, int]] (optional)
        Additional ports to forward. This requires a list of tuples where the first element
        is the port to open on the headnode while the second element is the port to map to
        or forward via the SSH-tunnel.

        Defaults to ``[]``.

    compute_target: azureml.core.ComputeTarget (optional)
        Azure ML Compute Target - see https://aka.ms/azureml/computetarget.

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

    vnet: str (optional)
        Name of the virtual network.

    subnet: str (optional)
        Name of the subnet inside the virtual network ``vnet``.

    vnet_resource_group: str (optional)
        Name of the resource group where the virtual network ``vnet``
        is located. If not passed, but names for ``vnet`` and ``subnet`` are
        passed, ``vnet_resource_group`` is assigned with the name of resource
        group associated with ``workspace``

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

    Examples
    --------

    First, import all necessary modules.

    >>> from azureml.core import Workspace
    >>> from dask_cloudprovider.azure import AzureMLCluster

    Next, create the ``Workspace`` object given your AzureML ``Workspace`` parameters. Check
    more in the AzureML documentation for
    `Workspace <https://docs.microsoft.com/python/api/azureml-core/azureml.core.workspace.workspace?view=azure-ml-py>`_.

    You can use ``ws = Workspace.from_config()`` after downloading the config file from the
    `Azure Portal <https://portal.azure.com>`_ or `ML Studio <https://ml.azure.com>`_.

    >>> subscription_id = "<your-subscription-id-here>"
    >>> resource_group = "<your-resource-group>"
    >>> workspace_name = "<your-workspace-name>"

    >>> ws = Workspace(
    ...     workspace_name=workspace_name,
    ...     subscription_id=subscription_id,
    ...     resource_group=resource_group
    ... )

    Then create the cluster.

    >>> amlcluster = AzureMLCluster(
    ...     # required
    ...     ws,
    ...     # optional
    ...     vm_size="STANDARD_DS13_V2",                                 # Azure VM size for the Compute Target
    ...     datastores=ws.datastores.values(),                          # Azure ML Datastores to mount on the headnode
    ...     environment_definition=ws.environments['AzureML-Dask-CPU'], # Azure ML Environment to run on the cluster
    ...     jupyter=true,                                               # Start JupyterLab session on the headnode
    ...     initial_node_count=2,                                       # number of nodes to start
    ...     scheduler_idle_timeout=7200                                 # scheduler idle timeout in seconds
    ... )

    Once the cluster has started, the Dask Cluster widget will print out two links:

    1. Jupyter link to a Jupyter Lab instance running on the headnode.
    2. Dask Dashboard link.

    Note that ``AzureMLCluster`` uses IPython Widgets to present this information, so if you are working in Jupyter Lab
    and see text that starts with ``VBox(children=``..., make sure you have enabled the IPython Widget
    `extension <https://jupyterlab.readthedocs.io/en/stable/user/extensions.html>`_.

    To connect to the Jupyter Lab session running on the cluster from your own computer, click the link provided in the
    widget printed above, or if you need the link directly it is stored in ``amlcluster.jupyter_link``.

    Once connected, you'll be in an AzureML `Run` session. To connect Dask from within the session, just run to
    following code to connect dask to the cluster:

    .. code-block:: python

        from azureml.core import Run
        from dask.distributed import Client

        run = Run.get_context()
        c = Client(run.get_metrics()["scheduler"])


    You can stop the cluster with `amlcluster.close()`. The cluster will automatically spin down if unused for
    20 minutes by default. Alternatively, you can delete the Azure ML Compute Target or cancel the Run from the
    Python SDK or UI to stop the cluster.


    """

    def __init__(
        self,
        workspace,
        compute_target=None,
        environment_definition=None,
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
        vnet_resource_group=None,
        vnet=None,
        subnet=None,
        show_output=False,
        telemetry_opt_out=None,
        asynchronous=False,
        **kwargs,
    ):
        warnings.warn(
            "AzureMLCluster is deprecated and will be removed in a future release. Please use AzureVMCluster instead",
            category=DeprecationWarning,
        )

        ### REQUIRED PARAMETERS
        self.workspace = workspace
        self.compute_target = compute_target

        ### ENVIRONMENT
        self.environment_definition = environment_definition

        ### EXPERIMENT DEFINITION
        self.experiment_name = experiment_name
        self.tags = {"tag": "azureml-dask"}

        ### ENVIRONMENT AND VARIABLES
        self.initial_node_count = initial_node_count

        ### SEND TELEMETRY
        self.telemetry_opt_out = telemetry_opt_out
        self.telemetry_set = False

        ### FUTURE EXTENSIONS
        self.kwargs = kwargs
        self.show_output = show_output

        ## CREATE COMPUTE TARGET
        self.admin_username = admin_username
        self.admin_ssh_key = admin_ssh_key
        self.vnet_resource_group = vnet_resource_group
        self.vnet = vnet
        self.subnet = subnet
        self.compute_target_set = True
        self.pub_key_file = ""
        self.pri_key_file = ""
        if self.compute_target is None:
            try:
                self.compute_target = self.__create_compute_target()
                self.compute_target_set = False
            except Exception as e:
                logger.exception(e)
                return
        elif self.compute_target.admin_user_ssh_key is not None and (
            self.admin_ssh_key is None or self.admin_username is None
        ):
            logger.exception(
                "Please provide private key and admin username to access compute target {}".format(
                    self.compute_target.name
                )
            )
            return

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
        if self.environment_definition is None:
            if self.use_gpu:
                self.environment_definition = self.workspace.environments[
                    "AzureML-Dask-GPU"
                ]
            else:
                self.environment_definition = self.workspace.environments[
                    "AzureML-Dask-CPU"
                ]

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
                        "At least one of the elements of the additional_ports parameter"
                        " is wrong. Make sure it is a list of int tuples."
                        " Check the documentation."
                    )
                    raise TypeError(error_message)

        self.additional_ports = additional_ports
        self.scheduler_ip_port = (
            None  ### INIT FOR HOLDING THE ADDRESS FOR THE SCHEDULER
        )

        ### DATASTORES
        self.datastores = datastores

        ### RUNNING IN MATRIX OR LOCAL
        self.same_vnet = None
        self.is_in_ci = False

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
            self.experiment_name = self.config.get("azureml.experiment_name")

        if self.initial_node_count is None:
            self.initial_node_count = self.config.get("azureml.initial_node_count")

        if self.jupyter is None:
            self.jupyter = self.config.get("azureml.jupyter")

        if self.jupyter_port is None:
            self.jupyter_port = self.config.get("azureml.jupyter_port")

        if self.dashboard_port is None:
            self.dashboard_port = self.config.get("azureml.dashboard_port")

        if self.scheduler_port is None:
            self.scheduler_port = self.config.get("azureml.scheduler_port")

        if self.scheduler_idle_timeout is None:
            self.scheduler_idle_timeout = self.config.get(
                "azureml.scheduler_idle_timeout"
            )

        if self.worker_death_timeout is None:
            self.worker_death_timeout = self.config.get("azureml.worker_death_timeout")

        if self.additional_ports is None:
            self.additional_ports = self.config.get("azureml.additional_ports")

        if self.admin_username is None:
            self.admin_username = self.config.get("azureml.admin_username")

        if self.admin_ssh_key is None:
            self.admin_ssh_key = self.config.get("azureml.admin_ssh_key")

        if self.datastores is None:
            self.datastores = self.config.get("azureml.datastores")

        if self.telemetry_opt_out is None:
            self.telemetry_opt_out = self.config.get("azureml.telemetry_opt_out")

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
        if self.show_output:
            print(f"{pre_post} {msg} {pre_post}".center(length, filler))

    async def __check_if_scheduler_ip_reachable(self):
        """
        Private method to determine if running in the cloud within the same VNET
        and the scheduler node is reachable
        """
        try:
            ip, port = self.scheduler_ip_port.split(":")
            socket.create_connection((ip, port), 20)
            self.same_vnet = True
            self.__print_message("On the same VNET")
        except socket.timeout as e:
            self.__print_message("Not on the same VNET")
            self.same_vnet = False
        except ConnectionRefusedError as e:
            logger.info(e)
            self.__print_message(e)
            pass

    def __prepare_rpc_connection_to_headnode(self):
        if self.same_vnet:
            return self.run.get_metrics()["scheduler"]
        elif self.is_in_ci:
            uri = f"{self.hostname}:{self.scheduler_port}"
            return uri
        else:
            uri = f"localhost:{self.scheduler_port}"
            self.hostname = "localhost"
            logger.info(f"Local connection: {uri}")
            return uri

    def __get_ssh_keys(self):
        from cryptography.hazmat.primitives import serialization as crypto_serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.backends import (
            default_backend as crypto_default_backend,
        )

        dir_path = os.path.join(os.getcwd(), "tmp")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        pub_key_file = os.path.join(dir_path, "key.pub")
        pri_key_file = os.path.join(dir_path, "key")

        key = rsa.generate_private_key(
            backend=crypto_default_backend(), public_exponent=65537, key_size=2048
        )
        private_key = key.private_bytes(
            crypto_serialization.Encoding.PEM,
            crypto_serialization.PrivateFormat.PKCS8,
            crypto_serialization.NoEncryption(),
        )
        public_key = key.public_key().public_bytes(
            crypto_serialization.Encoding.OpenSSH,
            crypto_serialization.PublicFormat.OpenSSH,
        )

        with open(pub_key_file, "wb") as f:
            f.write(public_key)

        with open(pri_key_file, "wb") as f:
            f.write(private_key)

        os.chmod(pri_key_file, 0o600)

        with open(pub_key_file, "r") as f:
            pubkey = f.read()

        self.pub_key_file = pub_key_file
        self.pri_key_file = pri_key_file

        return pubkey, pri_key_file

    def __create_compute_target(self):
        import random

        tmp_name = "dask-ct-{}".format(random.randint(100000, 999999))
        ct_name = self.kwargs.get("ct_name", tmp_name)
        vm_name = self.kwargs.get("vm_size", "STANDARD_DS3_V2")
        min_nodes = int(self.kwargs.get("min_nodes", "0"))
        max_nodes = int(self.kwargs.get("max_nodes", "100"))
        idle_time = int(self.kwargs.get("idle_time", "300"))
        vnet_rg = None
        vnet_name = None
        subnet_name = None

        if self.admin_username is None:
            self.admin_username = "dask"
        ssh_key_pub, self.admin_ssh_key = self.__get_ssh_keys()

        if self.vnet and self.subnet:
            vnet_name = self.vnet
            subnet_name = self.subnet
            if self.vnet_resource_group:
                vnet_rg = self.vnet_resource_group
            else:
                vnet_rg = self.workspace.resource_group

        try:
            if ct_name not in self.workspace.compute_targets:
                config = AmlCompute.provisioning_configuration(
                    vm_size=vm_name,
                    min_nodes=min_nodes,
                    max_nodes=max_nodes,
                    vnet_resourcegroup_name=vnet_rg,
                    vnet_name=vnet_name,
                    subnet_name=subnet_name,
                    idle_seconds_before_scaledown=idle_time,
                    admin_username=self.admin_username,
                    admin_user_ssh_key=ssh_key_pub,
                    remote_login_port_public_access="Enabled",
                )

                self.__print_message("Creating new compute targe: {}".format(ct_name))
                ct = ComputeTarget.create(self.workspace, ct_name, config)
                ct.wait_for_completion(show_output=self.show_output)
            else:
                self.__print_message(
                    "Using existing compute target: {}".format(ct_name)
                )
                ct = self.workspace.compute_targets[ct_name]
        except Exception as e:
            logger.exception("Cannot create/get compute target. {}".format(e))
            raise e

        return ct

    def __delete_compute_target(self):
        try:
            self.compute_target.delete()
        except ComputeTargetException as e:
            logger.exception(
                "Compute target {} cannot be removed. You may need to delete it manually. {}".format(
                    self.compute_target.name, e
                )
            )

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
        status = run.get_status()
        while (
            status != "Canceled"
            and status != "Failed"
            and "scheduler" not in run.get_metrics()
        ):
            print(".", end="")
            logger.info("Scheduler not ready")
            time.sleep(5)
            status = run.get_status()

        if status == "Canceled" or status == "Failed":
            run_error = run.get_details().get("error")
            error_message = "Failed to start the AzureML cluster."

            if run_error:
                error_message = "{} {}".format(error_message, run_error)
            logger.exception(error_message)

            if not self.compute_target_set:
                self.__delete_compute_target()

            raise Exception(error_message)

        print("\n")

        ### SET FLAGS
        self.scheduler_ip_port = run.get_metrics()["scheduler"]
        self.worker_params["--scheduler_ip_port"] = self.scheduler_ip_port
        self.__print_message(f'Scheduler: {run.get_metrics()["scheduler"]}')
        self.run = run

        ### CHECK IF ON THE SAME VNET
        max_retry = 5
        while self.same_vnet is None and max_retry > 0:
            time.sleep(5)
            await self.sync(self.__check_if_scheduler_ip_reachable)
            max_retry -= 1

        if self.same_vnet is None:
            self.run.cancel()
            if not self.compute_target_set:
                self.__delete_compute_target()
            logger.exception(
                "Connection error after retrying. Failed to start the AzureML cluster."
            )
            return

        ### REQUIRED BY dask.distributed.deploy.cluster.Cluster
        self.hostname = socket.gethostname()
        self.is_in_ci = (
            f"/mnt/batch/tasks/shared/LS_root/mounts/clusters/{self.hostname}"
            in os.getcwd()
        )
        _scheduler = self.__prepare_rpc_connection_to_headnode()
        self.scheduler_comm = rpc(_scheduler)
        await self.sync(self.__setup_port_forwarding)

        try:
            await super()._start()
        except Exception as e:
            logger.exception(e)
            # CLEAN UP COMPUTE TARGET
            self.run.cancel()
            if not self.compute_target_set:
                self.__delete_compute_target()
            return

        await self.sync(self.__update_links)

        self.__print_message("Connections established")
        self.__print_message(f"Scaling to {self.initial_node_count} workers")

        if self.initial_node_count > 1:
            self.scale(
                self.initial_node_count
            )  # LOGIC TO KEEP PROPER TRACK OF WORKERS IN `scale`
        self.__print_message("Scaling is done")

    async def __update_links(self):
        token = self.run.get_metrics()["token"]

        if self.same_vnet or self.is_in_ci:
            location = self.workspace.get_details()["location"]

            self.scheduler_info[
                "dashboard_url"
            ] = f"https://{self.hostname}-{self.dashboard_port}.{location}.instances.azureml.net/status"

            self.scheduler_info[
                "jupyter_url"
            ] = f"https://{self.hostname}-{self.jupyter_port}.{location}.instances.azureml.net/lab?token={token}"
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

        self.__print_message("Running in compute instance? {}".format(self.is_in_ci))
        os.system(
            "killall socat"
        )  # kill all socat processes - cleans up previous port forward setups
        if self.same_vnet:
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

            host_ip = "0.0.0.0"
            if self.is_in_ci:
                host_ip = socket.gethostbyname(self.hostname)

            cmd = (
                "ssh -vvv -o StrictHostKeyChecking=no -N"
                f" -i {os.path.expanduser(self.admin_ssh_key)}"
                f" -L {host_ip}:{self.jupyter_port}:{scheduler_ip}:8888"
                f" -L {host_ip}:{self.dashboard_port}:{scheduler_ip}:8787"
                f" -L {host_ip}:{self.scheduler_port}:{scheduler_ip}:8786"
            )

            for port in self.additional_ports:
                cmd += f" -L {host_ip}:{port[1]}:{scheduler_ip}:{port[0]}"

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
        """Link to Dask dashboard."""
        try:
            link = self.scheduler_info["dashboard_url"]
        except KeyError:
            return ""
        else:
            return link

    @property
    def jupyter_link(self):
        """Link to JupyterLab on running on the headnode of the cluster.
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
                % (self.dashboard_link, self.dashboard_link)
            )
        else:
            dashboard_link = ""

        if self.jupyter_link:
            jupyter_link = (
                '<p><b>Jupyter: </b><a href="%s" target="_blank">%s</a></p>\n'
                % (self.jupyter_link, self.jupyter_link)
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
        status = self.run.get_status()
        if status == "Canceled" or status == "Completed" or status == "Failed":
            self.close()

    def scale(self, workers=1):
        """Scale the cluster. Scales to a maximum of the workers available in the cluster."""
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
        """Scale up the number of workers."""
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
        """Scale down the number of workers. Scales to minimum of 1."""
        for i in range(workers):
            if self.workers_list:
                child_run = self.workers_list.pop(0)  # deactivate oldest workers
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

        ### REMOVE TEMP FILE
        if os.path.isfile(self.pub_key_file):
            os.remove(self.pub_key_file)
        if os.path.isfile(self.pri_key_file):
            os.remove(self.pri_key_file)

        if not self.compute_target_set:
            ### REMOVE COMPUTE TARGET
            self.__delete_compute_target()

        time.sleep(30)
        await super()._close()

    def close(self):
        """Close the cluster. All Azure ML Runs corresponding to the scheduler
        and worker processes will be completed. The Azure ML Compute Target will
        return to its minimum number of nodes after its idle time before scaledown.
        """
        return self.sync(self._close)
