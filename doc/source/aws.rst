Amazon Web Services (AWS)
=========================

.. currentmodule:: dask_cloudprovider

.. autosummary::
   EC2Cluster
   ECSCluster
   FargateCluster

Overview
--------

Authentication
^^^^^^^^^^^^^^

In order to create clusters on AWS you need to set your access key, secret key
and region. The simplest way is to use the aws command line tool.

.. code-block:: console

   $ pip install awscli
   $ aws configure

Elastic Compute Cloud (EC2)
---------------------------

TODO

.. autoclass:: EC2Cluster
   :members:

Elastic Container Service (ECS)
-------------------------------

You can also create Dask clusters using EC2 based ECS clusters using ``ECSCluster``.

Creating the ECS cluster is out of scope for this library but you can pass in
the ARN of an existing one like this:

.. code-block:: python

   from dask_cloudprovider import ECSCluster
   cluster = ECSCluster(cluster_arn="arn:aws:ecs:<region>:<acctid>:cluster/<clustername>")

All the other required resources such as roles, task definitions, tasks, etc
will be created automatically like in ``FargateCluster``.

.. autoclass:: ECSCluster
   :members:

GPU Support
^^^^^^^^^^^

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

Fargate
-------

The ``FargateCluster`` will create a new Fargate ECS cluster by default along
with all the IAM roles, security groups, and so on that it needs to function.

.. code-block:: python

   from dask_cloudprovider import FargateCluster
   cluster = FargateCluster()

Note that in many cases you will want to specify a custom Docker image to ``FargateCluster`` so that Dask has the packages it needs to execute your workflow.

.. code-block:: python

   from dask_cloudprovider import FargateCluster
   cluster = FargateCluster(image="<hub-user>/<repo-name>[:<tag>]")

..
One strategy to ensure that package versions match between your custom environment and the Docker container is to create your environment from an ``environment.yml`` file, export the exact package list for that environment using ``conda list --export > package-list.txt``, and then use the pinned package versions contained in ``package-list.txt`` in your Dockerfile.  You could use the default `Dask Dockerfile`_ as a template and simply add your pinned additional packages.

.. _`Dask Dockerfile`: https://github.com/dask/dask-docker/blob/master/base/Dockerfile

.. autoclass:: FargateCluster
   :members:

IAM Permissions
^^^^^^^^^^^^^^^

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