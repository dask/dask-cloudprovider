Microsoft Azure Machine Learning
================================

.. currentmodule:: dask_cloudprovider.azureml

.. warning::

    The Azure ML integration has been deprecated and will be removed in a future release.
    Please use the :class:`dask_cloudprovider.azure.AzureVMCluster` cluster manager instead.

.. autosummary::
   AzureMLCluster

Overview
--------

To start using ``dask_cloudprovider.AzureMLCluster`` you need, at a minimum,
an `Azure subscription <https://azure.microsoft.com/free/services/machine-learning/>`_ and
an `AzureML Workspace <https://docs.microsoft.com/python/api/azureml-core/azureml.core.workspace.workspace?view=azure-ml-py>`_.

AzureML
-------

.. autoclass:: AzureMLCluster
   :members:
