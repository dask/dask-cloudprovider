from azureml.core import Workspace, Experiment, Datastore, Dataset, Environment
from azureml.core.runconfig import MpiConfiguration
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.authentication import InteractiveLoginAuthentication
from dask_cloudprovider import AzureMLCluster
from ..azureml_configs import AzureMLConfigurations
import os

import asyncio
class aml_test:
    def __init__(self):
        # interactive_auth = InteractiveLoginAuthentication(
        #     tenant_id='72f988bf-86f1-41af-91ab-2d7cd011db47'
        # )

        subscription_id = '6560575d-fa06-4e7d-95fb-f962e74efd7a'
        resource_group = 'azure-sandbox'
        workspace_name = 'todrabas_UK_STH'

        self.ws = Workspace(
            workspace_name=workspace_name
            , subscription_id=subscription_id
            , resource_group=resource_group
            # , auth=interactive_auth
        )

        ### name
        name = 'todrabas'             # REPLACE

        ### vnet settings
        vnet_rg = self.ws.resource_group  # replace if needed
        vnet_name = 'todrabas_UK_STH_VN'     # replace if needed
        subnet_name = 'default'          # replace if needed

        ### azure ml names
        self.ct_name  = f'{name}-gpu'
        self.exp_name = f'{name}-dask-demo'

        ### trust but verify
        verify = f'''
        Name: {name}

        vNET RG: {vnet_rg}
        vNET name: {vnet_name}
        vNET subnet name: {subnet_name}

        Compute target: {self.ct_name}
        Experiment name: {self.exp_name}
        '''

        print(verify)

        self.vm_name = list(AzureMLConfigurations.azure_gpu_vms.keys())[4]
        self.gpus_per_node = AzureMLConfigurations.azure_gpu_vms[self.vm_name]

        if self.ct_name not in self.ws.compute_targets:
            # create config for Azure ML cluster
            # change properties as needed
            config = AmlCompute.provisioning_configuration(
                  vm_size=self.vm_name
                , min_nodes=0
                , max_nodes=4
                , vnet_resourcegroup_name=vnet_rg
                , vnet_name=vnet_name
                , subnet_name=subnet_name
                , idle_seconds_before_scaledown=300
            )
            self.ct = ComputeTarget.create(self.ws, self.ct_name, config)
            self.ct.wait_for_completion(show_output=True)
        else:
            self.ct = self.ws.compute_targets[self.ct_name]

        ## specify the data and code stores

        self.codefileshare = 'codefileshare'
        self.datafileshare = 'datafiles'

        if self.codefileshare not in self.ws.datastores:
            print('Registering codeshare...')
            Datastore.register_azure_file_share(
                self.ws
                , codefileshare
                , account_name=self.ws.datastores['workspacefilestore'].account_name # less stupid
                , account_key=self.ws.datastores['workspacefilestore'].account_key   # less less stupid
            )

        if self.datafileshare not in self.ws.datasets:
            print('Registering dataset...')
            ds = Dataset.File.from_files(
                'https://azureopendatastorage.blob.core.windows.net/isdweatherdatacontainer/ISDWeather/*/*/*.parquet', validate=False)
            os.system('sudo chmod 777 /mnt')
            ds.download('/mnt/data/isd')
            self.ws.datastores[datafileshare].upload('/mnt/data/isd', '/noaa-isd')
            ds = ds.register(self.ws, datafileshare)

        ### CREATE ENVIRONMENT DEFINITION
        self.environment_name='todrabas_GPU_ENV'
        self.update_environment = False
        self.docker_image='todrabas/aml_rapids:latest'
        self.use_GPU = True
        self.python_interpreter = '/opt/conda/envs/rapids/bin/python'
        self.pip_packages = ['azureml-sdk']
        self.conda_packages = ['matplotlib']

        

        if (
                self.environment_name not in self.ws.environments
            or self.update_environment
        ):
            print('Rebuilding environment...')
            env = Environment(name=self.environment_name)
            env.docker.enabled = True
            env.docker.base_image = self.docker_image

            if use_GPU:
                env.python.interpreter_path = self.python_interpreter   ### NEEDED FOR RAPIDS
                env.python.user_managed_dependencies = True

            ### CHECK IF pip_packages or conda_packages defined
            conda_dep = None
            if (
                   (type(self.pip_packages)   is list and len(self.pip_packages)   > 0) 
                or (type(self.conda_packages) is list and len(self.conda_packages) > 0) 
            ):
                conda_dep = CondaDependencies()

            if (type(self.pip_packages)   is list and len(self.pip_packages)   > 0) :
                for pip_package in self.pip_packages:
                    conda_dep.add_pip_package(pip_package)

            if (type(self.conda_packages) is list and len(self.conda_packages) > 0) :
                for conda_package in self.conda_packages:
                    conda_dep.add_conda_package(conda_package)

            if conda_dep is not None:
                env.python.conda_dependencies = conda_dep

            self.evn = env
        else:
            self.env = self.ws.environments[self.environment_name]
        
        print(f'''
        Environment: {self.env.name}
        ''')
        self.datastores = [self.codefileshare, self.datafileshare]

    def environmentDefinedNoPackages_Start(self):
        amlcluster = AzureMLCluster(
              workspace=self.ws
            , compute_target=self.ct
            , initial_node_count=2
            , experiment_name=self.exp_name
            , environment_definition=self.env
            , use_gpu=True
            , n_gpus_per_node=4
            , datastores=self.datastores#[self.codefileshare, self.datafileshare]
        )

        # return amlcluster

        # amlcluster.create_cluster()

if __name__ == '__main__':
    amlcl = aml_test()
    amlcl.environmentDefinedNoPackages_Start()
    # asyncio.run(asyncio.gather(amlcl.environmentDefinedNoPackages_Start()))
    # amlcl