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

Here is the minimal IAM policy that you need to create the cluster:

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

.. toctree::
   :maxdepth: 3
   :hidden:

   api
