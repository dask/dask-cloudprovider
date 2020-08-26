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

For AWS

.. code-block:: console

   $ pip install dask-cloudprovider[aws]

For Azure

.. code-block:: console

   $ pip install dask-cloudprovider[azure]

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
an `Azure subscription <https://azure.microsoft.com/free/services/machine-learning/>`_ and
an `AzureML Workspace <https://docs.microsoft.com/python/api/azureml-core/azureml.core.workspace.workspace?view=azure-ml-py>`_.

Getting started
^^^^^^^^^^^^^^^

Imports
~~~~~~~

First, import all necessary modules.

.. code-block:: python

   from azureml.core import Workspace
   from dask_cloudprovider import AzureMLCluster

Setup
~~~~~

Next, create the ``Workspace`` object given your AzureML ``Workspace`` parameters. Check
more in the AzureML documentation for `Workspace <https://docs.microsoft.com/python/api/azureml-core/azureml.core.workspace.workspace?view=azure-ml-py>`_.

You can use ``ws = Workspace.from_config()`` after downloading the config file from the `Azure Portal <https://portal.azure.com>`_ or `ML Studio <https://ml.azure.com>`_. 

.. code-block:: python

   subscription_id = "<your-subscription-id-here>"
   resource_group = "<your-resource-group>"
   workspace_name = "<your-workspace-name>"

   ws = Workspace(
      workspace_name=workspace_name,
      subscription_id=subscription_id,
      resource_group=resource_group
   )

Create cluster
~~~~~~~~~~~~~~

To create cluster:

.. code-block:: python

   amlcluster = AzureMLCluster(
   # required
   ws,

   # optional
   vm_size="STANDARD_DS13_V2",                                 # Azure VM size for the Compute Target
   datastores=ws.datastores.values(),                          # Azure ML Datastores to mount on the headnode
   environment_definition=ws.environments['AzureML-Dask-CPU'], # Azure ML Environment to run on the cluster
   jupyter=true,                                               # Flag to start JupyterLab session on the headnode
   initial_node_count=2,                                       # number of nodes to start 
   scheduler_idle_timeout=7200                                 # scheduler idle timeout in seconds 
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
