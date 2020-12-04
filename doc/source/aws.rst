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

If you are using EC2Cluster you would need to pass your aws credentials to the workers nodes. Here's a way to do that if you can read your aws credentials from the local machine:

.. code-block:: console
   
   def get_aws_credentials():
      """Read in your AWS credentials file and convert to environment variables."""
      parser = configparser.RawConfigParser()
    
      parser.read(os.path.expanduser('~/.aws/config'))
      config = parser.items('default')
    
      parser.read(os.path.expanduser('~/.aws/credentials'))
      credentials = parser.items('default')
    
      all_credentials = {key.upper(): value for key, value in [*config, *credentials]}
      with contextlib.suppress(KeyError):
      all_credentials["AWS_REGION"] = all_credentials.pop("REGION")
      return all_credentials

.. code-block:: console

   env_vars = get_aws_credentials()

Then at the launch of EC2Cluster specify the env_vars
 
.. code-block:: console

   from dask_cloudprovider.aws import EC2Cluster
   from dask.distributed import wait

   cluster = EC2Cluster(ami="ami-06d62f645899df7de",  # Example Deep Learning AMI (Ubuntu 18.04) # AWS AMI id can be region specific
                             docker_image="rapidsai/rapidsai:cuda11.0-runtime-ubuntu18.04",
                             instance_type="g4dn.xlarge",
                             worker_class="dask_cuda.CUDAWorker", 
                             n_workers=2,
                             bootstrap=False,
                             filesystem_size=120, env_vars=env_vars )


Elastic Container Service (ECS)
-------------------------------

.. autoclass:: ECSCluster
   :members:

Fargate
-------

.. autoclass:: FargateCluster
   :members:
