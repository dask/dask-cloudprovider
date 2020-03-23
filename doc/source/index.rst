Dask Cloud Provider
===================

*Native Cloud integration for Dask.*

This library creates Dask clusters on a given cloud provider
with no set up other than having credentials.
Currently, it only supports AWS.

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

You can also create Dask clusters using EC2 based ECS clusters using
``ECSCluster``.

Creating the ECS cluster is out of scope for this library but you can pass in
the ARN of an existing one like this:

.. code-block:: python

   from dask_cloudprovider import ECSCluster
   cluster = ECSCluster(cluster_arn="arn:aws:ecs:<region>:<acctid>:cluster/<clustername>")

All the other required resources such as roles, task definitions, tasks, etc
will be created automatically like in ``FargateCluster``.

GPU Support
~~~~~~~~~~~

There is also support in ``ECSCLuster`` for GPU aware Dask clusters. To do
this you need to create an ECS cluster with GPU capable instances (from the
``p3`` or ``p3dn`` families) and specify the number of GPUs each worker task
should have.

.. code-block:: python

   from dask_cloudprovider import ECSCluster
   cluster = ECSCluster(
       cluster_arn="arn:aws:ecs:<region>:<acctid>:cluster/<gpuclustername>",
       worker_gpu=1)

Azure
-----

In order to start using ``dask_cloudprovider.AzureMLCluster`` you need, at a minimum,
an `Azure subscription <https://azure.microsoft.com/en-us/free/services/machine-learning/>`_,
an `AzureML workspace <https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.workspace.workspace?view=azure-ml-py>`_, and
a `quota <https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/azure-subscription-service-limits>`_ to create your compute target.

Getting started
^^^^^^^^^^^^^^^

Imports
~~~~~~~

First, import all necessary modules.

.. code-block:: python

   from azureml.core import Workspace, Environment
   from azureml.core.conda_dependencies import CondaDependencies
   from azureml.core.compute import ComputeTarget, AmlCompute
   
   from dask_cloudprovider import AzureMLCluster

   import os

Setup
~~~~~

Next, create the ``Workspace`` object given your AzureML ``Workspace`` parameters. Check
more in the AzureML documentation for `Workspace <https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.workspace.workspace?view=azure-ml-py>`_.

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
   vnet_rg = ws.resource_group
   vnet_name = "dask_azureml_vnet"
   subnet_name = "default"

   ### azure ml names: ct - compute target, env - environment
   ct_name  = "dask-ct"
   env_name = "AzureML-Dask-CPU"

If you are running from an AzureML Compute Instance (Jupyter Lab) you should put both,
the ``ComputeTarget`` (see `Compute Target <https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.compute.computetarget?view=azure-ml-py>`_ documentation page) 
and the Compute Instance on the
same `virtual network <https://docs.microsoft.com/en-us/azure/virtual-network/virtual-networks-overview>`_.

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
VMs check here: `Windows <https://azure.microsoft.com/en-us/pricing/details/virtual-machines/windows/>`_ 
and `Linux <https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/>`_.

.. code-block:: python

   vm_name = "STANDARD_DS13_V2"

   ### UNCOMMENT BELOW FOR LOCAL RUNS
   # with open(admin_ssh_key_pub, "r") as f:
   #    ssh_key_pub = f.read().strip()

   if ct_name not in ws.compute_targets:
      # create config for Azure ML cluster
      # change properties as needed
      config = AmlCompute.provisioning_configuration(
         vm_size=vm_name,
         min_nodes=0,
         max_nodes=2,
         vnet_resourcegroup_name=vnet_rg,
         vnet_name=vnet_name,
         subnet_name=subnet_name,
         idle_seconds_before_scaledown=300,

         ### UNCOMMENT BELOW FOR LOCAL RUNS
         # admin_username=admin_username,
         # admin_user_ssh_key=ssh_key_pub,
         # remote_login_port_public_access='Enabled',
      )
      ct = ComputeTarget.create(ws, ct_name, config)
      ct.wait_for_completion(show_output=True)
   else:
      ct = ws.compute_targets[ct_name]

If your compute target already exists you can call ``ct = ws.compute_targets[ct_name]``.

Define Environment
~~~~~~~~~~~~~~~~~~

For the `Environment <https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.environment.environment?view=azure-ml-py>`_  
we will use a AzureML curated environment for running Dask CPU cluster, the `AzureML-Dask-CPU`. 
However, the ``Environment`` class allows you to specify your own docker image and additional packages to install.

.. code-block:: python

   packages = ["matplotlib"]

   env = Environment(name=env_name)

   for package in packages:
      env.python.conda_dependencies.add_pip_package(package)

Create cluster
~~~~~~~~~~~~~~

To create cluster:

.. code-block:: python

   amlcluster = AzureMLCluster(
      workspace=ws,
      compute_target=ct,
      environment_definition=env,
      initial_node_count=2,

      ### UNCOMMENT BELOW FOR LOCAL RUNS
      # admin_username=admin_username,
      # admin_ssh_key=admin_ssh_key_priv,
   )

Once the cluster has started, the Dask Cluster widget will print out two links:

1. Jupyter link to a Jupyter Lab instance running on the headnode.
2. Dask Dashboard link.

.. toctree::
   :maxdepth: 3
   :hidden:

   api