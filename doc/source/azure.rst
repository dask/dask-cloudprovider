Microsoft Azure
===============

.. currentmodule:: dask_cloudprovider

.. autosummary::
   AzureMLCluster

Overview
--------

In order to start using ``dask_cloudprovider.AzureMLCluster`` you need, at a minimum,
an `Azure subscription <https://azure.microsoft.com/free/services/machine-learning/>`_ and
an `AzureML Workspace <https://docs.microsoft.com/python/api/azureml-core/azureml.core.workspace.workspace?view=azure-ml-py>`_.

AzureML
-------

.. autoclass:: AzureMLCluster
   :members:Note that ``AzureMLCluster`` uses IPython Widgets to present this information, so if you are working in Jupyter Lab and see text that starts with ``VBox(children=``..., make sure you have enabled the IPython Widget `extension <https://jupyterlab.readthedocs.io/en/stable/user/extensions.html>`_.


Using Your Cluster
^^^^^^^^^^^^^^^^^^

Once the cluster is running, one can use the cluster in one of two ways:

1. Simply create a Dask Client with the Cluster in your current Python session,
2. Open the JupyterLab session running on the Cluster (if the cluster was created with the ``jupyter=True`` option) and connect to Dask Client locally.

To create a Dask Client in your current session, simply pass your cluster object to the ``Client`` function:

.. code-block:: python

   from dask.distributed import Client

    c = Client(amlcluster)
    # dask operations will now execute on the cluster via this client

When you're finished, you can then stop the cluster with ``amlcluster.close()``. The cluster will automatically spin down if unused for 20 minutes by default.
Alternatively, you can delete the Azure ML Compute Target or cancel the Run from the Python SDK or UI to stop the cluster.