Google Cloud Platform
=====================

.. currentmodule:: dask_cloudprovider.gcp

.. autosummary::
   GCPCluster

Overview
--------

Authentication
^^^^^^^^^^^^^^

In order to create clusters on GCP you need to set your authentication credentials.
You can do this via the ``gcloud`` `command line tool <https://cloud.google.com/sdk/gcloud>`_.

.. code-block:: console

   $ gcloud auth login

Alternatively you can use a `service account <https://cloud.google.com/iam/docs/service-accounts>`_ which provides credentials in a JSON file.
You must set the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable to the path to the JSON file.

.. code-block:: console

   $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

Project ID
^^^^^^^^^^

To use Dask Cloudprovider with GCP you must also configure your `Project ID <https://cloud.google.com/resource-manager/docs/creating-managing-projects>`_.
Generally when creating a GCP account you will create a default project. This can be found at the top of the GCP dashboard.

Your Project ID must be added to your Dask config file.

.. code-block:: yaml

    # ~/.config/dask/cloudprovider.yaml
    cloudprovider:
      gcp:
        projectid: "YOUR PROJECT ID"

Or via an environment variable.

.. code-block:: console

    $ export DASK_CLOUDPROVIDER__GCP__PROJECTID="YOUR PROJECT ID"

Google Cloud VMs
----------------

.. autoclass:: GCPCluster
   :members: