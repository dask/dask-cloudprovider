Amazon Web Services (AWS)
=========================

.. currentmodule:: dask_cloudprovider.aws

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


Credentials
^^^^^^^^^^^

In order for your Dask workers to be able to connect to other AWS resources such as S3 they will need credentials.

This can be done by attaching IAM roles to individual resources or by passing credentials as environment variables. See
each cluster manager docstring for more information.

Elastic Compute Cloud (EC2)
---------------------------

.. autoclass:: EC2Cluster
   :members:

Elastic Container Service (ECS)
-------------------------------

.. autoclass:: ECSCluster
   :members:

Fargate
-------

.. autoclass:: FargateCluster
   :members:
