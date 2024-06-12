import dask
from dask_cloudprovider.config import ClusterConfig
from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)
from typing import Optional
import os


try:
    from ibm_code_engine_sdk.code_engine_v2 import CodeEngineV2
    from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
    from ibm_code_engine_sdk.code_engine_v2 import ProjectsPager
except ImportError as e:
    msg = (
        "Dask Cloud Provider IBM requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install -c conda-forge dask-cloudprovider       # either conda install\n"
        '  pip install "dask-cloudprovider[ibm]" --upgrade       # or python -m pip install'
    )
    raise ImportError(msg) from e
import json



class IBMCodeEngine(VMInterface):
    def __init__(
        self,
        cluster: str,
        config,
        image: str = None,
        region: str = None,
        project_id: str = None,
        api_key: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster = cluster
        self.config = config

        self.region = region
        self.project_id = project_id
        self.api_key = api_key


    async def create_vm(self):
        authenticator = IAMAuthenticator(self.api_key, url='https://iam.cloud.ibm.com')
        authenticator.set_disable_ssl_verification(True)  # Disable SSL verification for the authenticator
        code_engine_service = CodeEngineV2(authenticator=authenticator)
        code_engine_service.set_service_url('https://api.'+self.region+'.codeengine.cloud.ibm.com/v2')
        code_engine_service.set_disable_ssl_verification(True)  # Disable SSL verification for the service instance

        """response = code_engine_service.create_job(
            project_id='',
            image_reference='icr.io/codeengine/helloworld',
            name='my-job',
        )
        job = response.get_result()
        print(job)"""


        return private_ip_address, None



    async def destroy_vm(self):
        pass

class IBMCodeEngineScheduler(SchedulerMixin, IBMCodeEngine):
    pass


class IBMCodeEngineWorker(WorkerMixin, IBMCodeEngine):
    pass


class IBMCodeEngineCluster(VMCluster):
     def __init__(
        self,
        image: str = None,
        region: str = None,
        project_id: str = None,
        debug: bool = False,
        **kwargs,
    ):
        self.config = ClusterConfig(dask.config.get("cloudprovider.ibm", {}))
        self.scheduler_class = IBMCodeEngineScheduler
        self.worker_class = IBMCodeEngineWorker
        
        self.region = region or self.config.get("region")
        self.project_id = project_id or self.config.get("project_id")

        api_key=self.config.get("api_key")

        self.debug = debug
        
        self.options = {
            "cluster": self,
            "config": self.config,
            "region": self.region,
            "project_id": self.project_id,
            "api_key": api_key,
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {**self.options}
        super().__init__(debug=debug, **kwargs)
