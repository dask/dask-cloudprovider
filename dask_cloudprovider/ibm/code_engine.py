import json
import time
import urllib3
import threading
import random

from kubernetes import client
from kubernetes.client.rest import ApiException

import dask
from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)

from distributed.core import Status
from distributed.security import Security

try:
    from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
    from ibm_code_engine_sdk.code_engine_v2 import CodeEngineV2
    from ibm_code_engine_sdk.ibm_cloud_code_engine_v1 import IbmCloudCodeEngineV1
except ImportError as e:
    msg = (
        "Dask Cloud Provider IBM requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install -c conda-forge dask-cloudprovider       # either conda install\n"
        '  pip install "dask-cloudprovider[ibm]" --upgrade       # or python -m pip install'
    )
    raise ImportError(msg) from e


urllib3.disable_warnings()


class IBMCodeEngine(VMInterface):
    def __init__(
        self,
        cluster,
        config,
        image: str = None,
        region: str = None,
        project_id: str = None,
        scheduler_cpu: str = None,
        scheduler_mem: str = None,
        scheduler_disk: str = None,
        scheduler_timeout: int = None,
        scheduler_command: str = None,
        worker_cpu: str = None,
        worker_mem: str = None,
        worker_disk: str = None,
        worker_threads: int = None,
        worker_command: str = None,
        api_key: str = None,
        docker_server: str = None,
        docker_username: str = None,
        docker_password: str = None,
        docker_registry_name: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster = cluster  # This is the IBMCodeEngineCluster instance
        self.config = config
        self.image = image
        self.region = region
        self.project_id = project_id
        self.scheduler_cpu = scheduler_cpu
        self.scheduler_mem = scheduler_mem
        self.scheduler_disk = scheduler_disk
        self.scheduler_timeout = scheduler_timeout
        self.scheduler_command = scheduler_command
        self.worker_cpu = worker_cpu
        self.worker_mem = worker_mem
        self.worker_disk = worker_disk
        self.worker_threads = worker_threads
        self.worker_command = worker_command
        self.api_key = api_key
        self.docker_server = docker_server
        self.docker_username = docker_username
        self.docker_password = docker_password
        self.docker_registry_name = docker_registry_name

        self.authenticator = IAMAuthenticator(
            self.api_key, url="https://iam.cloud.ibm.com"
        )
        self.authenticator.set_disable_ssl_verification(
            True
        )  # Disable SSL verification for the authenticator

        self.code_engine_service = CodeEngineV2(authenticator=self.authenticator)
        self.code_engine_service.set_service_url(
            "https://api." + self.region + ".codeengine.cloud.ibm.com/v2"
        )
        self.code_engine_service.set_disable_ssl_verification(
            True
        )  # Disable SSL verification for the service instance

    def _extract_k8s_config_details(self, project_id):
        delegated_refresh_token_payload = {
            "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
            "apikey": self.api_key,
            "response_type": "delegated_refresh_token",
            "receiver_client_ids": "ce",
            "delegated_refresh_token_expiry": "3600",
        }
        token_manager = self.code_engine_service.authenticator.token_manager
        original_request_payload = token_manager.request_payload
        token_manager.request_payload = delegated_refresh_token_payload
        try:
            iam_response = token_manager.request_token()
        finally:
            token_manager.request_payload = original_request_payload

        kc_resp = self.code_engine_service_v1.get_kubeconfig(
            iam_response["delegated_refresh_token"], project_id
        )
        kubeconfig_data = kc_resp.get_result()

        current_context_name = kubeconfig_data["current-context"]
        context_details = next(
            c["context"]
            for c in kubeconfig_data["contexts"]
            if c["name"] == current_context_name
        )

        namespace = context_details.get("namespace", "default")
        server_url = next(
            c["cluster"]
            for c in kubeconfig_data["clusters"]
            if c["name"] == context_details["cluster"]
        )["server"]
        return namespace, server_url

    def create_registry_secret(self):
        # Set up the authenticator and service instance
        self.code_engine_service_v1 = IbmCloudCodeEngineV1(
            authenticator=self.authenticator
        )
        self.code_engine_service_v1.set_service_url(
            "https://api." + self.region + ".codeengine.cloud.ibm.com/api/v1"
        )
        token = self.authenticator.token_manager.get_token()

        # Fetch K8s config details
        namespace, k8s_api_server_url = self._extract_k8s_config_details(
            self.project_id
        )

        # Create a new configuration instance
        configuration = client.Configuration()
        configuration.host = k8s_api_server_url
        configuration.api_key = {"authorization": "Bearer " + token}
        api_client_instance = client.ApiClient(configuration)
        core_api = client.CoreV1Api(api_client_instance)

        secret = client.V1Secret(
            metadata=client.V1ObjectMeta(
                name=self.docker_registry_name, namespace=namespace
            ),
            type="kubernetes.io/dockerconfigjson",
            string_data={
                ".dockerconfigjson": json.dumps(
                    {
                        "auths": {
                            self.docker_server: {
                                "username": self.docker_username,
                                "password": self.docker_password,
                            }
                        }
                    }
                )
            },
        )

        try:
            core_api.delete_namespaced_secret(
                self.docker_registry_name, namespace=namespace
            )
        except ApiException as e:
            if e.status == 404:  # Not Found, which is fine
                pass
            else:
                self.cluster._log(
                    f"Error deleting existing registry secret {self.docker_registry_name} in {namespace}: {e}"
                )
                pass

        try:
            core_api.create_namespaced_secret(namespace, secret)
            self.cluster._log(
                f"Successfully created registry secret '{self.docker_registry_name}'."
            )
        except ApiException as e:
            if e.status == 409:  # Conflict, secret already exists
                self.cluster._log(
                    f"Registry secret '{self.docker_registry_name}' already exists."
                )
            else:
                self.cluster._log(
                    f"Error creating registry secret '{self.docker_registry_name}': {e}"
                )
                raise e

    async def create_vm(self):
        # Deploy a scheduler on a Code Engine application
        # It allows listening on a specific port and exposing it to the public
        if "scheduler" in self.name:
            self.code_engine_service.create_app(
                project_id=self.project_id,
                image_reference=self.image,
                name=self.name,
                run_commands=self.command,
                image_port=8786,
                scale_cpu_limit=self.cpu,
                scale_min_instances=1,
                scale_concurrency=1000,
                scale_memory_limit=self.memory,
                scale_ephemeral_storage_limit=self.disk,
                scale_request_timeout=self.cluster.scheduler_timeout,
                image_secret=self.docker_registry_name,
                run_env_variables=[
                    {
                        "type": "literal",
                        "name": "DASK_INTERNAL_INHERIT_CONFIG",
                        "key": "DASK_INTERNAL_INHERIT_CONFIG",
                        "value": dask.config.serialize(dask.config.global_config),
                    }
                ],
            )

            # Create a ConfigMap with the Dask configuration once time
            self.code_engine_service.create_config_map(
                project_id=self.project_id,
                name=self.cluster.uuid,
                data={
                    "DASK_INTERNAL_INHERIT_CONFIG": dask.config.serialize(
                        dask.config.global_config
                    ),
                },
            )

            # This loop waits for the app to be ready, then returns the internal and public URLs
            while True:
                response = self.code_engine_service.get_app(
                    project_id=self.project_id,
                    name=self.name,
                )
                app = response.get_result()
                if app["status"] == "ready":
                    break

                time.sleep(0.5)

            internal_url = app["endpoint_internal"].split("//")[1]
            public_url = app["endpoint"].split("//")[1]

            return internal_url, public_url

        # Deploy a worker on a Code Engine job run
        else:

            def create_job_run_thread():
                retry_delay = 1

                # Add an exponential sleep to avoid overloading the Code Engine API
                for attempt in range(5):
                    try:
                        self.code_engine_service.create_job_run(
                            project_id=self.project_id,
                            image_reference=self.image,
                            name=self.name,
                            run_commands=self.command,
                            scale_cpu_limit=self.cpu,
                            scale_memory_limit=self.memory,
                            scale_ephemeral_storage_limit=self.disk,
                            image_secret=self.docker_registry_name,
                            run_env_variables=[
                                {
                                    "type": "config_map_key_reference",
                                    "reference": self.cluster.uuid,
                                    "name": "DASK_INTERNAL_INHERIT_CONFIG",
                                    "key": "DASK_INTERNAL_INHERIT_CONFIG",
                                }
                            ],
                        )
                        return
                    except Exception:
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        retry_delay += random.uniform(0, 1)

                raise Exception("Maximum retry attempts reached")

            # Create a thread to create multiples job runs in parallel
            job_run_thread = threading.Thread(target=create_job_run_thread)
            job_run_thread.start()

    async def destroy_vm(self):
        self.cluster._log(f"Deleting Instance: {self.name}")

        if "scheduler" in self.name:
            self.code_engine_service.delete_app(
                project_id=self.project_id,
                name=self.name,
            )
        else:
            self.code_engine_service.delete_job_run(
                project_id=self.project_id,
                name=self.name,
            )
            try:
                self.code_engine_service.delete_config_map(
                    project_id=self.project_id,
                    name=self.cluster.uuid,
                )
            except Exception:
                pass


class IBMCodeEngineScheduler(SchedulerMixin, IBMCodeEngine):
    """Scheduler running in a GCP instance."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cpu = self.cluster.scheduler_cpu
        self.memory = self.cluster.scheduler_mem
        self.disk = self.cluster.scheduler_disk

        if self.scheduler_command:
            self.command = self.scheduler_command.split()
        else:
            # The scheduler must run on the public URL with the "ws" protocol
            self.command = [
                "python",
                "-m",
                "distributed.cli.dask_scheduler",
                "--protocol",
                "ws",
            ]

    async def start(self):
        if self.docker_server and self.docker_username and self.docker_password:
            self.docker_registry_name = "dask-" + self.docker_server.split(".")[0]
            self.cluster._log(
                f"Creating registry secret for {self.docker_registry_name}"
            )
            self.create_registry_secret()

        self.cluster._log(
            f"Launching cluster with the following configuration: "
            f"\n  Source Image: {self.image} "
            f"\n  Region: {self.region} "
            f"\n  Project id: {self.project_id} "
            f"\n  Scheduler CPU: {self.cpu} "
            f"\n  Scheduler Memory: {self.memory} "
            f"\n  Scheduler Disk: {self.disk} "
            f"\n  Scheduler Timeout: {self.cluster.scheduler_timeout} "
            f"\n  Worker CPU: {self.cluster.worker_cpu} "
            f"\n  Worker Memory: {self.cluster.worker_mem} "
            f"\n  Worker Disk: {self.cluster.worker_disk} "
            f"\n  Worker Threads: {self.cluster.worker_threads} "
        )
        self.cluster._log(f"Creating scheduler instance {self.name}")

        # It must use the external URL with the "wss" protocol and port 443 to establish a
        # secure WebSocket connection between the client and the scheduler.
        self.internal_ip, self.external_ip = await self.create_vm()
        self.address = f"wss://{self.external_ip}:443"

        await self.wait_for_scheduler()

        self.cluster.scheduler_internal_ip = self.internal_ip
        self.cluster.scheduler_external_ip = self.external_ip
        self.cluster.scheduler_port = self.port
        self.status = Status.running


class IBMCodeEngineWorker(WorkerMixin, IBMCodeEngine):
    def __init__(
        self,
        *args,
        worker_class: str = "distributed.cli.Nanny",
        worker_options: dict = {},
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.worker_class = worker_class
        self.worker_options = worker_options
        self.cpu = self.cluster.worker_cpu
        self.memory = self.cluster.worker_mem
        self.disk = self.cluster.worker_disk

        # On this case, the worker must connect to the scheduler internal URL with the "ws" protocol and port 80
        internal_scheduler = f"ws://{self.cluster.scheduler_internal_ip}:80"

        self.command = [
            "python",
            "-m",
            "distributed.cli.dask_spec",
            internal_scheduler,
            "--spec",
            json.dumps(
                {
                    "cls": self.worker_class,
                    "opts": {
                        **worker_options,
                        "name": self.name,
                        "nthreads": self.cluster.worker_threads,
                    },
                }
            ),
        ]

        # To work with Code Engine, we need to use the extra arguments
        if self.worker_command:
            custom_command_prefix = self.worker_command.split()
            original_command_suffix = self.command[3:]
            self.command = custom_command_prefix + original_command_suffix

        if self.docker_server and self.docker_username and self.docker_password:
            self.docker_registry_name = "dask-" + self.docker_server.split(".")[0]

    async def start(self):
        self.cluster._log(f"Creating worker instance {self.name}")
        await self.create_vm()
        self.status = Status.running


class IBMCodeEngineCluster(VMCluster):
    """Cluster running on IBM Code Engine.

    This cluster manager builds a Dask cluster running on IBM Code Engine.

    When configuring your cluster, you may find it useful to refer to the IBM Cloud documentation for available options.

    https://cloud.ibm.com/docs/codeengine

    Parameters
    ----------
    image: str
        The Docker image to run on all instances. This image must have a valid Python environment and have ``dask``
        installed in order for the ``dask-scheduler`` and ``dask-worker`` commands to be available.
    region: str
        The IBM Cloud region to launch your cluster in.

        See: https://cloud.ibm.com/docs/codeengine?topic=codeengine-regions
    project_id: str
        Your IBM Cloud project ID. This must be set either here or in your Dask config.
    scheduler_cpu: str
        The amount of CPU to allocate to the scheduler.

        See: https://cloud.ibm.com/docs/codeengine?topic=codeengine-mem-cpu-combo
    scheduler_mem: str
        The amount of memory to allocate to the scheduler.

        See: https://cloud.ibm.com/docs/codeengine?topic=codeengine-mem-cpu-combo
    scheduler_disk: str
        The amount of ephemeral storage to allocate to the scheduler. This value must be lower than scheduler_mem.
    scheduler_timeout: int
        The timeout for the scheduler in seconds.
    scheduler_command: str
        The command to run the scheduler. This should be a string that is passed to the ``dask-scheduler`` command.
        The default is ``dask-scheduler --protocol ws``.
    worker_cpu: str
        The amount of CPU to allocate to each worker.

        See: https://cloud.ibm.com/docs/codeengine?topic=codeengine-mem-cpu-combo
    worker_mem: str
        The amount of memory to allocate to each worker.

        See: https://cloud.ibm.com/docs/codeengine?topic=codeengine-mem-cpu-combo
    worker_disk: str
        The amount of ephemeral storage to allocate to each worker. This value must be lower than worker_mem.
    worker_threads: int
        The number of threads to use on each worker.
    worker_command: str
        The command to run the worker. This should be a string that is passed to the ``dask-worker`` command.
        The default is ``python -m distributed.cli.dask_spec``.
    docker_server: str
        The Docker registry server (e.g., "docker.io", "gcr.io"). Required if using private Docker images.
    docker_username: str
        The username for authenticating with the Docker registry. Required if using private Docker images.
    docker_password: str
        The password or access token for authenticating with the Docker registry.
        Required if using private Docker images.
    debug: bool, optional
        More information will be printed when constructing clusters to enable debugging.

    Notes
    -----

    **Credentials**

    In order to use the IBM Cloud API, you will need to set up an API key. You can create an API key in the IBM Cloud
    console.

    The best practice way of doing this is to pass an API key to be used by workers. You can set this API key as an
    environment variable. Here is a small example to help you do that.

    To expose your IBM API KEY, use this command:
    export DASK_CLOUDPROVIDER__IBM__API_KEY=xxxxx

    **Docker Registry Authentication**

    If you need to use private Docker images, you can configure Docker registry credentials using the docker_server,
    docker_username, and docker_password parameters. These credentials will be used to create a Kubernetes secret
    for image pulling in Code Engine.

    **Certificates**

    This backend will need to use a Let's Encrypt certificate (ISRG Root X1) to connect the client to the scheduler
    between websockets. More information can be found here: https://letsencrypt.org/certificates/

    Examples
    --------

    Create the cluster.

    >>> from dask_cloudprovider.ibm import IBMCodeEngineCluster
    >>> cluster = IBMCodeEngineCluster(n_workers=1)
    Launching cluster with the following configuration:
        Source Image: daskdev/dask:latest
        Region: eu-de
        Project id: f21626f6-54f7-4065-a038-75c8b9a0d2e0
        Scheduler CPU: 0.25
        Scheduler Memory: 1G
        Scheduler Disk: 400M
        Scheduler Timeout: 600
        Worker CPU: 2
        Worker Memory: 4G
        Worker Disk: 400M
    Creating scheduler dask-xxxxxxxx-scheduler
    Waiting for scheduler to run at dask-xxxxxxxx-scheduler.xxxxxxxxxxxx.xx-xx.codeengine.appdomain.cloud:443
    Scheduler is running
    Creating worker instance dask-xxxxxxxx-worker-xxxxxxxx

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    Do some work.

    >>> import dask.array as da
    >>> arr = da.random.random((1000, 1000), chunks=(100, 100))
    >>> arr.mean().compute()
    0.5001550986751964

    Close the cluster

    >>> cluster.close()
    Deleting Instance: dask-xxxxxxxx-worker-xxxxxxxx
    Deleting Instance: dask-xxxxxxxx-scheduler

    You can also do this all in one go with context managers to ensure the cluster is created and cleaned up.

    >>> with IBMCodeEngineCluster(n_workers=1) as cluster:
    ...     with Client(cluster) as client:
    ...         print(da.random.random((1000, 1000), chunks=(100, 100)).mean().compute())
    Launching cluster with the following configuration:
        Source Image: daskdev/dask:latest
        Region: eu-de
        Project id: f21626f6-54f7-4065-a038-75c8b9a0d2e0
        Scheduler CPU: 0.25
        Scheduler Memory: 1G
        Scheduler Disk: 400M
        Scheduler Timeout: 600
        Worker CPU: 2
        Worker Memory: 4G
        Worker Disk: 400M
        Worker Threads: 1
    Creating scheduler dask-xxxxxxxx-scheduler
    Waiting for scheduler to run at dask-xxxxxxxx-scheduler.xxxxxxxxxxxx.xx-xx.codeengine.appdomain.cloud:443
    Scheduler is running
    Creating worker instance dask-xxxxxxxx-worker-xxxxxxxx
    0.5000812282861661
    Deleting Instance: dask-xxxxxxxx-worker-xxxxxxxx
    Deleting Instance: dask-xxxxxxxx-scheduler

    """

    def __init__(
        self,
        image: str = None,
        region: str = None,
        project_id: str = None,
        scheduler_cpu: str = None,
        scheduler_mem: str = None,
        scheduler_disk: str = None,
        scheduler_timeout: int = None,
        scheduler_command: str = None,
        worker_cpu: str = None,
        worker_mem: str = None,
        worker_disk: str = None,
        worker_threads: int = 1,
        worker_command: str = None,
        docker_server: str = None,
        docker_username: str = None,
        docker_password: str = None,
        debug: bool = False,
        **kwargs,
    ):
        self.config = dask.config.get("cloudprovider.ibm", {})
        self.scheduler_class = IBMCodeEngineScheduler
        self.worker_class = IBMCodeEngineWorker

        self.image = image or self.config.get("image")
        self.region = region or self.config.get("region")
        self.project_id = project_id or self.config.get("project_id")
        api_key = self.config.get("api_key")
        self.scheduler_cpu = scheduler_cpu or self.config.get("scheduler_cpu")
        self.scheduler_mem = scheduler_mem or self.config.get("scheduler_mem")
        self.scheduler_disk = scheduler_disk or self.config.get("scheduler_disk")
        self.scheduler_timeout = scheduler_timeout or self.config.get(
            "scheduler_timeout"
        )
        self.scheduler_command = scheduler_command or self.config.get(
            "scheduler_command"
        )
        self.worker_cpu = worker_cpu or self.config.get("worker_cpu")
        self.worker_mem = worker_mem or self.config.get("worker_mem")
        self.worker_disk = worker_disk or self.config.get("worker_disk")
        self.worker_threads = worker_threads or self.config.get("worker_threads")
        self.worker_command = worker_command or self.config.get("worker_command")
        self.docker_server = docker_server or self.config.get("docker_server")
        self.docker_username = docker_username or self.config.get("docker_username")
        self.docker_password = docker_password or self.config.get("docker_password")

        self.debug = debug

        self.options = {
            "cluster": self,
            "config": self.config,
            "image": self.image,
            "region": self.region,
            "project_id": self.project_id,
            "scheduler_cpu": self.scheduler_cpu,
            "scheduler_mem": self.scheduler_mem,
            "scheduler_disk": self.scheduler_disk,
            "scheduler_timeout": self.scheduler_timeout,
            "scheduler_command": self.scheduler_command,
            "worker_cpu": self.worker_cpu,
            "worker_mem": self.worker_mem,
            "worker_disk": self.worker_disk,
            "worker_threads": self.worker_threads,
            "worker_command": self.worker_command,
            "docker_server": self.docker_server,
            "docker_username": self.docker_username,
            "docker_password": self.docker_password,
            "api_key": api_key,
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {**self.options}

        # https://letsencrypt.org/certificates/ --> ISRG Root X1
        sec = Security(
            require_encryption=False,
            tls_ca_file=(
                "-----BEGIN CERTIFICATE-----\n"
                "MIIFazCCA1OgAwIBAgIRAIIQz7DSQONZRGPgu2OCiwAwDQYJKoZIhvcNAQELBQAw\n"
                "TzELMAkGA1UEBhMCVVMxKTAnBgNVBAoTIEludGVybmV0IFNlY3VyaXR5IFJlc2Vh\n"
                "cmNoIEdyb3VwMRUwEwYDVQQDEwxJU1JHIFJvb3QgWDEwHhcNMTUwNjA0MTEwNDM4\n"
                "WhcNMzUwNjA0MTEwNDM4WjBPMQswCQYDVQQGEwJVUzEpMCcGA1UEChMgSW50ZXJu\n"
                "ZXQgU2VjdXJpdHkgUmVzZWFyY2ggR3JvdXAxFTATBgNVBAMTDElTUkcgUm9vdCBY\n"
                "MTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAK3oJHP0FDfzm54rVygc\n"
                "h77ct984kIxuPOZXoHj3dcKi/vVqbvYATyjb3miGbESTtrFj/RQSa78f0uoxmyF+\n"
                "0TM8ukj13Xnfs7j/EvEhmkvBioZxaUpmZmyPfjxwv60pIgbz5MDmgK7iS4+3mX6U\n"
                "A5/TR5d8mUgjU+g4rk8Kb4Mu0UlXjIB0ttov0DiNewNwIRt18jA8+o+u3dpjq+sW\n"
                "T8KOEUt+zwvo/7V3LvSye0rgTBIlDHCNAymg4VMk7BPZ7hm/ELNKjD+Jo2FR3qyH\n"
                "B5T0Y3HsLuJvW5iB4YlcNHlsdu87kGJ55tukmi8mxdAQ4Q7e2RCOFvu396j3x+UC\n"
                "B5iPNgiV5+I3lg02dZ77DnKxHZu8A/lJBdiB3QW0KtZB6awBdpUKD9jf1b0SHzUv\n"
                "KBds0pjBqAlkd25HN7rOrFleaJ1/ctaJxQZBKT5ZPt0m9STJEadao0xAH0ahmbWn\n"
                "OlFuhjuefXKnEgV4We0+UXgVCwOPjdAvBbI+e0ocS3MFEvzG6uBQE3xDk3SzynTn\n"
                "jh8BCNAw1FtxNrQHusEwMFxIt4I7mKZ9YIqioymCzLq9gwQbooMDQaHWBfEbwrbw\n"
                "qHyGO0aoSCqI3Haadr8faqU9GY/rOPNk3sgrDQoo//fb4hVC1CLQJ13hef4Y53CI\n"
                "rU7m2Ys6xt0nUW7/vGT1M0NPAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNV\n"
                "HRMBAf8EBTADAQH/MB0GA1UdDgQWBBR5tFnme7bl5AFzgAiIyBpY9umbbjANBgkq\n"
                "hkiG9w0BAQsFAAOCAgEAVR9YqbyyqFDQDLHYGmkgJykIrGF1XIpu+ILlaS/V9lZL\n"
                "ubhzEFnTIZd+50xx+7LSYK05qAvqFyFWhfFQDlnrzuBZ6brJFe+GnY+EgPbk6ZGQ\n"
                "3BebYhtF8GaV0nxvwuo77x/Py9auJ/GpsMiu/X1+mvoiBOv/2X/qkSsisRcOj/KK\n"
                "NFtY2PwByVS5uCbMiogziUwthDyC3+6WVwW6LLv3xLfHTjuCvjHIInNzktHCgKQ5\n"
                "ORAzI4JMPJ+GslWYHb4phowim57iaztXOoJwTdwJx4nLCgdNbOhdjsnvzqvHu7Ur\n"
                "TkXWStAmzOVyyghqpZXjFaH3pO3JLF+l+/+sKAIuvtd7u+Nxe5AW0wdeRlN8NwdC\n"
                "jNPElpzVmbUq4JUagEiuTDkHzsxHpFKVK7q4+63SM1N95R1NbdWhscdCb+ZAJzVc\n"
                "oyi3B43njTOQ5yOf+1CceWxG1bQVs5ZufpsMljq4Ui0/1lvh+wjChP4kqKOJ2qxq\n"
                "4RgqsahDYVvTH9w7jXbyLeiNdd8XM2w9U/t7y0Ff/9yi0GE44Za4rF2LN9d11TPA\n"
                "mRGunUHBcnWEvgJBQl9nJEiU0Zsnvgc/ubhPgXRR4Xq37Z0j4r7g1SgEEzwxA57d\n"
                "emyPxgcYxn/eR44/KJ4EBs+lVDR3veyJm+kXQ99b21/+jh5Xos1AnX5iItreGCc=\n"
                "-----END CERTIFICATE-----"
            ),
        )
        super().__init__(security=sec, debug=debug, **kwargs)
