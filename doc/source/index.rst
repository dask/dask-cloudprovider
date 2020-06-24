Dask Cloud Provider
===================

*Native Cloud integration for Dask.*

This library creates Dask clusters on a given cloud provider
with no set up other than having credentials.
Currently, it supports AWS and AzureML.

Installation
------------

Pip
^^^

.. code-block:: console

   $ pip install dask-cloudprovider

Conda
^^^^^

.. code-block:: console

   $ conda install -c conda-forge dask-cloudprovider

-----

Below are the different modules for creating clusters on various cloud
providers.

AWS
---

In order to create clusters on AWS you need to set your access key, secret key
and region. The simplest way is to use the aws command line tool.

.. code-block:: console

   $ pip install awscli
   $ aws configure

Fargate/ECS
^^^^^^^^^^^

The ``FargateCluster`` will create a new Fargate ECS cluster by default along
with all the IAM roles, security groups, and so on that it needs to function.

.. code-block:: python

   from dask_cloudprovider import FargateCluster
   cluster = FargateCluster()

..

   ⚠ All AWS resources created by ``FargateCluster`` should be removed on
   garbage collection. If the process is killed harshly this will not happen.

Note that in many cases you will want to specify a custom Docker image to ``FargateCluster`` so that Dask has the packages it needs to execute your workflow. 

.. code-block:: python

   from dask_cloudprovider import FargateCluster
   cluster = FargateCluster(image="<hub-user>/<repo-name>[:<tag>]")

..
One strategy to ensure that package versions match between your custom environment and the Docker container is to create your environment from an ``environment.yml`` file, export the exact package list for that environment using ``conda list --export > package-list.txt``, and then use the pinned package versions contained in ``package-list.txt`` in your Dockerfile.  You could use the default `Dask Dockerfile`_ as a template and simply add your pinned additional packages. 

.. _`Dask Dockerfile`: https://github.com/dask/dask-docker/blob/master/base/Dockerfile

You can also create Dask clusters using EC2 based ECS clusters using ``ECSCluster``.

Creating the ECS cluster is out of scope for this library but you can pass in
the ARN of an existing one like this:

.. code-block:: python

   from dask_cloudprovider import ECSCluster
   cluster = ECSCluster(cluster_arn="arn:aws:ecs:<region>:<acctid>:cluster/<clustername>")

All the other required resources such as roles, task definitions, tasks, etc
will be created automatically like in ``FargateCluster``.

IAM Permissions
~~~~~~~~~~~~~~~

To create a ``FargateCluster`` the cluster manager will need to various AWS resources ranging from IAM roles to VPCs to ECS tasks. Depending on your use case you may want the cluster to create all of these for you, or you may wish to specify them youself ahead of time.

Here is the full minimal IAM policy that you need to create the whole cluster:

.. code-block:: json
   
   {
       "Statement": [
           {
               "Action": [
                   "ec2:AuthorizeSecurityGroupIngress",
                   "ec2:CreateSecurityGroup",
                   "ec2:CreateTags",
                   "ec2:DescribeNetworkInterfaces",
                   "ec2:DescribeSubnets",
                   "ec2:DescribeVpcs",
                   "ec2:DeleteSecurityGroup",
                   "ecs:CreateCluster",
                   "ecs:DescribeTasks",
                   "ecs:ListAccountSettings",
                   "ecs:RegisterTaskDefinition",
                   "ecs:RunTask",
                   "ecs:StopTask",
                   "ecs:ListClusters",
                   "ecs:DescribeClusters",
                   "ecs:DeleteCluster",
                   "ecs:ListTaskDefinitions",
                   "ecs:DescribeTaskDefinition",
                   "ecs:DeregisterTaskDefinition",
                   "iam:AttachRolePolicy",
                   "iam:CreateRole",
                   "iam:TagRole",
                   "iam:PassRole",
                   "iam:DeleteRole",
                   "iam:ListRoleTags",
                   "iam:ListAttachedRolePolicies",
                   "iam:DetachRolePolicy",
                   "logs:DescribeLogGroups"
               ],
               "Effect": "Allow",
               "Resource": [
                   "*"
               ]
           }
       ],
       "Version": "2012-10-17"
   } 
   
If you specify all of the resources yourself you will need a minimal policy of:

.. code-block:: json
   
   {
       "Statement": [
           {
               "Action": [
                   "ec2:CreateTags",
                   "ec2:DescribeNetworkInterfaces",
                   "ec2:DescribeSubnets",
                   "ec2:DescribeVpcs",
                   "ecs:DescribeTasks",
                   "ecs:ListAccountSettings",
                   "ecs:RegisterTaskDefinition",
                   "ecs:RunTask",
                   "ecs:StopTask",
                   "ecs:ListClusters",
                   "ecs:DescribeClusters",
                   "ecs:ListTaskDefinitions",
                   "ecs:DescribeTaskDefinition",
                   "ecs:DeregisterTaskDefinition",
                   "iam:ListRoleTags",
                   "logs:DescribeLogGroups"
               ],
               "Effect": "Allow",
               "Resource": [
                   "*"
               ]
           }
       ],
       "Version": "2012-10-17"
   } 

GPU Support
~~~~~~~~~~~

There is also support in ``ECSCluster`` for GPU aware Dask clusters. To do
this you need to create an ECS cluster with GPU capable instances (from the
``g3``, ``p3`` or ``p3dn`` families) and specify the number of GPUs each worker task
should have.

.. code-block:: python

   from dask_cloudprovider import ECSCluster
   cluster = ECSCluster(
       cluster_arn="arn:aws:ecs:<region>:<acctid>:cluster/<gpuclustername>",
       worker_gpu=1)

By setting the ``worker_gpu`` option to something other than ``None`` will cause the cluster
to run ``dask-cuda-worker`` as the worker startup command. Setting this option will also change
the default Docker image to ``rapidsai/rapidsai:latest``, if you're using a custom image
you must ensure the NVIDIA CUDA toolkit is installed with a version that matches the host machine
along with ``dask-cuda``.


Azure
-----

In order to start using ``dask_cloudprovider.AzureMLCluster`` you need, at a minimum,
an `Azure subscription <https://azure.microsoft.com/free/services/machine-learning/>`_,
an `AzureML workspace <https://docs.microsoft.com/python/api/azureml-core/azureml.core.workspace.workspace?view=azure-ml-py>`_, and
a `quota <https://docs.microsoft.com/azure/azure-resource-manager/management/azure-subscription-service-limits>`_ to create your compute target.

Getting started
^^^^^^^^^^^^^^^

Imports
~~~~~~~

First, import all necessary modules.

.. code-block:: python

   from azureml.core import Workspace, Environment
   from azureml.core.compute import ComputeTarget, AmlCompute
   
   from dask_cloudprovider import AzureMLCluster

   import os

Setup
~~~~~

Next, create the ``Workspace`` object given your AzureML ``Workspace`` parameters. Check
more in the AzureML documentation for `Workspace <https://docs.microsoft.com/python/api/azureml-core/azureml.core.workspace.workspace?view=azure-ml-py>`_.

You can use ``ws = Workspace.from_config()`` after downloading the config file from the Azure Portal or ML studio. 

.. code-block:: python

   subscription_id = "<your-subscription-id-here>"
   resource_group = "<your-resource-group>"
   workspace_name = "<your-workspace-name>"

   ws = Workspace(
      workspace_name=workspace_name,
      subscription_id=subscription_id,
      resource_group=resource_group
   )

Configure parameters
~~~~~~~~~~~~~~~~~~~~

Let's keep everything in one place so it's easy to maintain.

.. code-block:: python

   name = "dask-azureml"

   ### vnet settings
   # vnet_rg = ws.resource_group
   # vnet_name = "dask_azureml_vnet"
   # subnet_name = "default"

   ### azure ml names: ct - compute target, env - environment
   ct_name = "dask-ct"
   env_name = "AzureML-Dask-CPU"

If you are running from an AzureML Compute Instance (Jupyter Lab) you should put both,
the ``ComputeTarget`` (see `Compute Target <https://docs.microsoft.com/python/api/azureml-core/azureml.core.compute.computetarget?view=azure-ml-py>`_ documentation page) 
and the Compute Instance on the
same `virtual network <https://docs.microsoft.com/azure/virtual-network/virtual-networks-overview>`_.

The ``AzureMLCluster`` class allows you to submit the job from your local machine as well (tested on DOS CLI and bash). In such case it is not necesary to 
for the Compute Target to be on a virtual network (it is still preferred to hide the cluster in a virtual network). 
However, you need to provide some administator name (other than ``admin``) and private and public SSH keys.

.. code-block:: python

   ### credentials
   admin_username = name.split("-")[0]   ### dask
   admin_ssh_key_pub = "<path-to-public-key>"
   admin_ssh_key_priv = "<path-to-private-key>"

The above credentials will be used to create an SSH tunnel between the headnode and your local machine
so you can communicate with the Dask cluster.

Create Compute Target
~~~~~~~~~~~~~~~~~~~~~

Next, let's create or retrieve already existing compute target. For a full list of
VMs check here: `Windows <https://azure.microsoft.com/pricing/details/virtual-machines/windows/>`_ 
and `Linux <https://azure.microsoft.com/pricing/details/virtual-machines/linux/>`_.

.. code-block:: python

   # In this example, we will use ``STANDARD_DS12_V2`` VM because it is cheaper than others
   vm_name = "STANDARD_DS12_V2"

   with open(admin_ssh_key_pub, "r") as f:
      ssh_key_pub = f.read().strip()

   if ct_name not in ws.compute_targets:
      # create config for Azure ML cluster
      # change properties as needed
      config = AmlCompute.provisioning_configuration(
         vm_size=vm_name
         , min_nodes=0
         , max_nodes=2
         , idle_seconds_before_scaledown=300
         , admin_username=admin_username
         , admin_user_ssh_key=ssh_key_pub
         , remote_login_port_public_access='Enabled'

         ## UNCOMMENT TO SETUP VIRTUAL NETWORK
         # , vnet_resourcegroup_name=vnet_rg
         # , vnet_name=vnet_name
         # , subnet_name=subnet_name
      )
      ct = ComputeTarget.create(ws, ct_name, config)
      ct.wait_for_completion(show_output=True)
   else:
      ct = ws.compute_targets[ct_name]

If your compute target already exists you can call ``ct = ws.compute_targets[ct_name]``.

Setting up vnet
"""""""""""""""
If you do not have a virtual network yet there are two ways to create one.

1. Using `https://portal.azure.com <https://portal.azure.com>`_:

   a. On the home page click on `+ Create a resource` on the top-left portion of the page.
   b. Search for `Virtual Network`.
   c. Click on `Create` and follow the instructions: select the `Subscription` and the 
      `Resource group` you will create the vnet in, and provide a `Name` and the `Location`. 
      **NOTE:** keep the location the same as your AzureML Workspace e.g. if your AzureML Workspace 
      location is `eastus` you should create the Virtual Network in `East US`.
   d. Click `Review+Create` and follow through with the rest of the instructions.

2. Using Azure CLI:

   a. Install the AzureCLI; instructions are here: 
      `https://docs.microsoft.com/cli/azure/install-azure-cli?view=azure-cli-latest 
      <https://docs.microsoft.com/cli/azure/install-azure-cli?view=azure-cli-latest>`_
   b. Open terminal and login to your Azure Subscription: ``az login``. This should 
      automatically log you in into your Azure subscription. **NOTE:** If you have more than 
      one subscription you will need to set the right subscription to use:
      ``az account set --subscription "<name-of-sub-to-use>"``
   c. Create the virtual network: 
      ``az network vnet create -g <resource-group> -n <vnet-name> --location <location> --subnet-name default``

Define Environment
~~~~~~~~~~~~~~~~~~

For the `Environment <https://docs.microsoft.com/python/api/azureml-core/azureml.core.environment.environment?view=azure-ml-py>`_  
we will use a AzureML curated environment for running Dask CPU cluster, the `AzureML-Dask-CPU`. 
However, the ``Environment`` class allows you to specify your own docker image and additional packages to install.

.. code-block:: python

   env = ws.environments[env_name]

Create cluster
~~~~~~~~~~~~~~

To create cluster:

.. code-block:: python

   amlcluster = AzureMLCluster(
      workspace=ws
      , compute_target=ct
      , environment_definition=env
      , initial_node_count=2
      , admin_username=admin_username
      , admin_ssh_key=admin_ssh_key_priv   ### path, not contents of the key
   )

Once the cluster has started, the Dask Cluster widget will print out two links:

1. Jupyter link to a Jupyter Lab instance running on the headnode.
2. Dask Dashboard link.

You can stop the cluster with `amlcluster.close()`. The cluster will automatically spin down if unused for 20 minutes by default.
Alternatively, you can delete the Azure ML Compute Target or cancel the Run from the Python SDK or UI to stop the cluster.  

.. toctree::
   :maxdepth: 3
   :hidden:

   api