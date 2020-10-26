Configuration
=============

Each cluster manager in Dask Cloudprovider will require some configuration specific to the cloud
services you wish to use. Many config options will have sensible defaults and often you can create
a cluster with just your authentication credentials configured.

Authentication
--------------

All cluster managers assume you have already configured your credentials for the cloud you are using.

For AWS this would mean storing your access key and secret key in ``~/.aws/credentials``. The AWS CLI
can create this for you by running the command ``aws configure``.

See each cluster manager for specific details.

.. warning::
    Most cluster managers also allow passing credentials as keyword arguments, although this would result in
    credentials being stored in code and is not advised.

Cluster config
--------------

Configuration can be passed to a cluster manager via keyword arguments, YAML config or environment variables.

For example the ``FargateCluster`` manager for AWS ECS takes a ``scheduler_mem`` configuration option to set how much memory
to give the scheduler in megabytes. This can be configured in the following ways.

.. code-block:: python

   from dask_cloudprovider import FargateCluster

   cluster = FargateCluster(
       scheduler_mem=8192
   )

.. code-block:: yaml

   # ~/.config/dask/cloudprovider.yaml

   cloudprovider:
     ecs:
       scheduler_mem: 8192

.. code-block:: console

   $ export DASK_CLOUDPROVIDER__ECS__SCHEDULER_MEM=8192

See each cluster manager and the `Dask configuration docs <https://docs.dask.org/en/latest/configuration.html>`_ for more information.