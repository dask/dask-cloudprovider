Welcome to dask-cloud's documentation!
======================================

..

   ⚠ This library is in early alpha. Development is currently iterating on
   master. Will switch to regular GitHub flow once things stabalize.
   Use at own risk.


Native Cloud integration for Dask. This library intends to allow people to
create dask clusters on a given cloud provider with no set up other than having
credentials.

Providers
=========

Below are the different modules for creating clusters on various cloud
providers.

AWS
---

In order to create clusters on AWS you need to set your access key, secret key
and region. The simplest way is to use the aws command line tool.

.. code-block:: console

   $ pip install awscli
   $ aws configure

ECS
^^^

The ``ECSCluster`` will create a new Fargate ECS cluster by default along with
all the IAM roles, security groups, etc that it needs to function.

.. code-block:: python

   from dask_cloud import ECSCluster
   cluster = ECSCluster()

..

   ⚠ All AWS resources created by ``ECSCluster`` should be removed on garbage
   collection. If the process is killed harshly this will not happen.

.. toctree::
   :maxdepth: 3
   :hidden:

   api
