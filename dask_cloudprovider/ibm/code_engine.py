import json
import time
import urllib3
import threading
import random

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
    from ibm_code_engine_sdk.code_engine_v2 import CodeEngineV2
    from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
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
        cluster: str,
        config,
        image: str = None,
        region: str = None,
        project_id: str = None,
        scheduler_cpu: str = None,
        scheduler_mem: str = None,
        scheduler_timeout: int = None,
        worker_cpu: str = None,
        worker_mem: str = None,
        worker_threads: int = None,
        api_key: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster = cluster
        self.config = config
        self.image = image
        self.region = region
        self.project_id = project_id
        self.scheduler_cpu = scheduler_cpu
        self.scheduler_mem = scheduler_mem
        self.scheduler_timeout = scheduler_timeout
        self.worker_cpu = worker_cpu
        self.worker_mem = worker_mem
        self.worker_threads = worker_threads
        self.api_key = api_key

        authenticator = IAMAuthenticator(self.api_key, url="https://iam.cloud.ibm.com")
        authenticator.set_disable_ssl_verification(
            True
        )  # Disable SSL verification for the authenticator

        self.code_engine_service = CodeEngineV2(authenticator=authenticator)
        self.code_engine_service.set_service_url(
            "https://api." + self.region + ".codeengine.cloud.ibm.com/v2"
        )
        self.code_engine_service.set_disable_ssl_verification(
            True
        )  # Disable SSL verification for the service instance

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
                scale_request_timeout=self.cluster.scheduler_timeout,
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

        self.command = [
            "python",
            "-m",
            "distributed.cli.dask_scheduler",
            "--protocol",
            "ws",
        ]

    async def start(self):
        self.cluster._log(
            f"Launching cluster with the following configuration: "
            f"\n  Source Image: {self.image} "
            f"\n  Region: {self.region} "
            f"\n  Project id: {self.project_id} "
            f"\n  Scheduler CPU: {self.cpu} "
            f"\n  Scheduler Memory: {self.memory} "
            f"\n  Scheduler Timeout: {self.cluster.scheduler_timeout} "
            f"\n  Worker CPU: {self.cluster.worker_cpu} "
            f"\n  Worker Memory: {self.cluster.worker_mem} "
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
    scheduler_timeout: int
        The timeout for the scheduler in seconds.
    worker_cpu: str
        The amount of CPU to allocate to each worker.

        See: https://cloud.ibm.com/docs/codeengine?topic=codeengine-mem-cpu-combo
    worker_mem: str
        The amount of memory to allocate to each worker.

        See: https://cloud.ibm.com/docs/codeengine?topic=codeengine-mem-cpu-combo
    worker_threads: int
        The number of threads to use on each worker.
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
        Scheduler Timeout: 600
        Worker CPU: 2
        Worker Memory: 4G
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
        Scheduler Timeout: 600
        Worker CPU: 2
        Worker Memory: 4G
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
        scheduler_timeout: int = None,
        worker_cpu: str = None,
        worker_mem: str = None,
        worker_threads: int = 1,
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
        self.scheduler_timeout = scheduler_timeout or self.config.get(
            "scheduler_timeout"
        )
        self.worker_cpu = worker_cpu or self.config.get("worker_cpu")
        self.worker_mem = worker_mem or self.config.get("worker_mem")
        self.worker_threads = worker_threads or self.config.get("worker_threads")

        self.debug = debug

        self.options = {
            "cluster": self,
            "config": self.config,
            "image": self.image,
            "region": self.region,
            "project_id": self.project_id,
            "scheduler_cpu": self.scheduler_cpu,
            "scheduler_mem": self.scheduler_mem,
            "scheduler_timeout": self.scheduler_timeout,
            "worker_cpu": self.worker_cpu,
            "worker_mem": self.worker_mem,
            "worker_threads": self.worker_threads,
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
