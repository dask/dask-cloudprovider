Dask Cloud Provider
===================

*Native Cloud integration for Dask.*

This package provides classes for constructing and managing ephemeral Dask clusters on various
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

.. warning::

   Cluster managers will attempt to automatically remove hanging cloud resources on garbage collection if the cluster
   object is destroyed without calling ``cluster.close()``, however this is not guaranteed.

To implicitly close your cluster when you are done with it you can optionally contruct the cluster manager via a
context manager. However this will result in the creation and destruction of the whole cluster whenever you run
this code.

.. code-block:: python

    from dask_cloudprovider import FargateCluster
    from dask.distributed import Client

    with FargateCluster(...) as cluster:
        with Client(cluster) as client:
            # Do some Dask things

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
    digitalocean.rst
    azure.rst

To connect to the Jupyter Lab session running on the cluster from your own computer, simply click the link provided in the widget printed above, or if you need the link directly it is stored in ``amlcluster.jupyter_link``.

Once connected, you'll be in an AzureML `Run` session. To connect Dask from within the session, just run to following code to connect dask to the cluster:

.. code-block:: python

    from azureml.core import Run
    from dask.distributed import Client

    run = Run.get_context()
    c = Client(run.get_metrics()["scheduler"])


.. toctree::
    :maxdepth: 2
    :hidden:
    :caption: Advanced

    troubleshooting.rst
    packer.rst
