Nebius
============

.. currentmodule:: dask_cloudprovider.nebius

.. autosummary::
   NebiusCluster

Overview
--------

Authentication
^^^^^^^^^^^^^^


Before creating clusters on Nebius, you must configure your authentication credentials. You can do this using the `nebius` `command line tool <https://docs.nebius.com/cli/quickstart>`_.

After obtaining your credentials, add them to your Dask configuration under:

* cloudprovider.nebius.token
* cloudprovider.nebius.project_id

You can specify these values by either:

#. Including the environment variables NB_IAM_TOKEN and NB_PROJECT_ID in your YAML configuration.

    .. code-block:: yaml

      # ~/.config/dask/cloudprovider.yaml

      cloudprovider:
        nebius:
            token: "your_iam_token"
            project_id: "your_project_id"

#. Exporting them as environment variables in your shell.

    .. code-block:: console

      $ export DASK_CLOUDPROVIDER__NEBIUS__TOKEN=($nebius iam get-access-token)
      $ export DASK_CLOUDPROVIDER__NEBIUS__PROJECT_ID="your_project_id"

Dask Configuration
^^^^^^^^^^^^^^^^^^

You can change configuration of ``server_platform``, ``server_preset`` and ``image_family``. List of all available platforms and presets you can find in `Nebius docs <https://docs.nebius.com/compute/virtual-machines/types>`_.

.. autoclass:: NebiusCluster
   :members: