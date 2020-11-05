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