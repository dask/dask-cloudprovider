IBM Cloud
============

.. currentmodule:: dask_cloudprovider.ibm

.. autosummary::
   IBMCodeEngineCluster

Overview
--------

Authentication
^^^^^^^^^^^^^^

To authenticate with IBM Cloud you must first generate an 
`API key <https://cloud.ibm.com/docs/account?topic=account-userapikey&interface=ui&locale=en#create_user_key>`_.

Then you must put this in your Dask configuration at ``cloudprovider.ibm.api_key``. This can be done by
adding the API key to your YAML configuration or exporting an environment variable.

.. code-block:: yaml

   # ~/.config/dask/cloudprovider.yaml

   cloudprovider:
      ibm:
         api_key: "your_api_key"

.. code-block:: console

   $ export DASK_CLOUDPROVIDER__IBM__API_KEY="your_api_key"

Project ID
^^^^^^^^^^

To use Dask Cloudprovider with IBM Cloud you must also configure your `Project ID <https://dataplatform.cloud.ibm.com/docs/content/wsj/analyze-data/fm-project-id.html?context=wx>`_.
This can be found at the top of the IBM Cloud dashboard.

Your Project ID must be added to your Dask config file.

.. code-block:: yaml

    # ~/.config/dask/cloudprovider.yaml
    cloudprovider:
      ibm:
         project_id: "your_project_id"

Or via an environment variable.

.. code-block:: console

    $ export DASK_CLOUDPROVIDER__IBM__PROJECT_ID="your_project_id"

Code Engine
-------

.. autoclass:: IBMCodeEngineCluster
   :members: