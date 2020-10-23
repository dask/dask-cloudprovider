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

Imports
^^^^^^^

First, import all necessary modules.

.. code-block:: python

   from azureml.core import Workspace
   from dask_cloudprovider import AzureMLCluster

Setup
^^^^^

Next, create the ``Workspace`` object given your AzureML ``Workspace`` parameters. Check
more in the AzureML documentation for `Workspace <https://docs.microsoft.com/python/api/azureml-core/azureml.core.workspace.workspace?view=azure-ml-py>`_.

You can use ``ws = Workspace.from_config()`` after downloading the config file from the `Azure Portal <https://portal.azure.com>`_ or `ML Studio <https://ml.azure.com>`_.

.. code-block:: python

   subscription_id = "<your-subscription-id-here>"
   resource_group = "<your-resource-group>"
   workspace_name = "<your-workspace-name>"

   ws = Workspace(
      workspace_name=workspace_name,
      subscription_id=subscription_id,
      resource_group=resource_group
   )

Create cluster
^^^^^^^^^^^^^^

To create cluster:

.. code-block:: python

   amlcluster = AzureMLCluster(
   # required
   ws,

   # optional
   vm_size="STANDARD_DS13_V2",                                 # Azure VM size for the Compute Target
   datastores=ws.datastores.values(),                          # Azure ML Datastores to mount on the headnode
   environment_definition=ws.environments['AzureML-Dask-CPU'], # Azure ML Environment to run on the cluster
   jupyter=true,                                               # Flag to start JupyterLab session on the headnode
   initial_node_count=2,                                       # number of nodes to start
   scheduler_idle_timeout=7200                                 # scheduler idle timeout in seconds
   )

Once the cluster has started, the Dask Cluster widget will print out two links:

1. Jupyter link to a Jupyter Lab instance running on the headnode.
2. Dask Dashboard link.

You can stop the cluster with `amlcluster.close()`. The cluster will automatically spin down if unused for 20 minutes by default.
Alternatively, you can delete the Azure ML Compute Target or cancel the Run from the Python SDK or UI to stop the cluster.

.. autoclass:: AzureMLCluster
   :members: