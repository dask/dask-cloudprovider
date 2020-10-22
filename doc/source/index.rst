Dask Cloud Provider
===================

*Native Cloud integration for Dask.*

This package provides classes for constructing and managing ephemeral Dask cluster on various
cloud platforms.

To use a cloud provider cluster manager you can import it and instantiate it. Instantiating the class
will result in cloud resources being created for you.

.. code-block:: python

   from dask_cloudprovider import FargateCluster
   cluster = FargateCluster(
      # Cluster manager specific config kwargs
   )

You can then construct a Dask client with that cluster object to use the cluster.

.. code-block:: python

   from dask.distributed import Client
   client = Client(cluster)

Once you are connected to the cluster you can go ahead and use Dask and all computation will take
place on your cloud resource.

Once you are finished be sure to close out your cluster to shut down any cloud resources you have and end any charges.

.. code-block:: python

   cluster.close()

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Overview

   installation.rst
   config.rst

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Providers

   aws.rst
   azure.rst
   digitalocean.rst

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Advanced

   packer.rst
