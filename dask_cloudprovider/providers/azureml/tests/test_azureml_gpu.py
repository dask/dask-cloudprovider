from azureml.core import Workspace, Experiment, Datastore, Dataset, Environment
from azureml.core.runconfig import MpiConfiguration
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.authentication import InteractiveLoginAuthentication
from dask_cloudprovider import AzureMLCluster
# from dask_cloudprovider.providers.azureml_configs import AzureMLConfigs
from ..azureml_configs import AzureMLConfigurations
import os

# azure_gpu_vm_names = [
#   'Standard_NC6s_v2',
#   'Standard_NC12s_v2',
#   'Standard_NC24s_v2',
#   'Standard_NC24sr_v2',
#   'Standard_NC6s_v3',
#   'Standard_NC12s_v3',
#   'Standard_NC24s_v3',
#   'Standard_NC24sr_v3'
# ]

# azure_gpu_vm_sizes = {
#   'Standard_NC6s_v2'    : 1,
#   'Standard_NC12s_v2'   : 2,
#   'Standard_NC24s_v2'   : 4,
#   'Standard_NC24sr_v2'  : 4,
#   'Standard_NC6s_v3'    : 1,
#   'Standard_NC12s_v3'   : 2,
#   'Standard_NC24s_v3'   : 4,
#   'Standard_NC24sr_v3'  : 4
# }

if __name__ == '__main__':

    interactive_auth = InteractiveLoginAuthentication(
        tenant_id='72f988bf-86f1-41af-91ab-2d7cd011db47'
    )

    subscription_id = '6560575d-fa06-4e7d-95fb-f962e74efd7a'
    resource_group = 'azure-sandbox'
    workspace_name = 'todrabas_UK_STH'

    ws = Workspace(
          workspace_name=workspace_name
        , subscription_id=subscription_id
        , resource_group=resource_group
        , auth=interactive_auth
    )

    # ws = Workspace.from_config()

    ### name
    name = 'todrabas'             # REPLACE

    ### vnet settings
    vnet_rg = ws.resource_group  # replace if needed
    vnet_name = 'todrabas_UK_STH_VN'     # replace if needed
    subnet_name = 'default'          # replace if needed

    ### azure ml names
    ct_name = f'{name}-dask-ct'
    exp_name = f'{name}-dask-demo'

    ### trust but verify
    verify = f'''
    Name: {name}

    vNET RG: {vnet_rg}
    vNET name: {vnet_name}
    vNET subnet name: {subnet_name}

    Compute target: {ct_name}
    Experiment name: {exp_name}
    '''

    print(verify)

    vm_name = list(AzureMLConfigurations.azure_gpu_vms.keys())[6]
    gpus_per_node = AzureMLConfigurations.azure_gpu_vms[vm_name]

    if ct_name not in ws.compute_targets:
        # create config for Azure ML cluster
        # change properties as needed
        config = AmlCompute.provisioning_configuration(
              vm_size=vm_name
            , min_nodes=0
            , max_nodes=4
            , vnet_resourcegroup_name=vnet_rg
            , vnet_name=vnet_name
            , subnet_name=subnet_name
            , idle_seconds_before_scaledown=120
        )
        ct = ComputeTarget.create(ws, ct_name, config)
        ct.wait_for_completion(show_output=True)
    else:
        ct = ws.compute_targets[ct_name]

    ## specify the data and code stores

    codefileshare = 'codefileshare'
    datafileshare = 'datafiles'

    if codefileshare not in ws.datastores:
        print('Registering codeshare...')
        Datastore.register_azure_file_share(
            ws
            , codefileshare
            , account_name=ws.datastores['workspacefilestore'].account_name # less stupid
            , account_key=ws.datastores['workspacefilestore'].account_key   # less less stupid
        )

    if datafileshare not in ws.datasets:
        print('Registering dataset...')
        ds = Dataset.File.from_files(
            'https://azureopendatastorage.blob.core.windows.net/isdweatherdatacontainer/ISDWeather/*/*/*.parquet', validate=False)
        # os.system('sudo chmod 777 /mnt')
        # ds.download('/mnt/data/isd')
        ws.datastores[datafileshare].upload('/mnt/data/isd', '/noaa-isd')
        ds = ds.register(ws, datafileshare)

    amlcluster = AzureMLCluster(
          workspace=ws
        , compute=ct
        , codefileshare=codefileshare
        , datafileshare=datafileshare
        , initial_node_count=2
        , docker_image='todrabas/aml_rapids:latest'
        , use_GPU=True
        , gpus_per_node=gpus_per_node
        , environment_name='todrabas_GPU_ENV'
        , experiment_name=exp_name
        , use_existing_run=False
        , update_environment=False
        , python_interpreter='/opt/conda/envs/rapids/bin/python'
        # , pip_packages=['foo']
        # , conda_packages=['bar']
    )

    ### GET THE CLIENT
    client = amlcluster.connect_cluster()

    # # for k in Environment.list(workspace=ws):
    # #     print(k)
