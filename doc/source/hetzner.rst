Hetzner
============

.. currentmodule:: dask_cloudprovider.hetzner

.. autosummary::
   HetznerCluster

Overview
--------

Authentication
^^^^^^^^^^^^^^

To authenticate with Hetzner you must first generate a
`personal access token <https://www.digitalocean.com/docs/apis-clis/api/create-personal-access-token/>`_.

Then you must put this in your Dask configuration at ``cloudprovider.hetzner.token``. This can be done by
adding the token to your YAML configuration or exporting an environment variable.

.. code-block:: yaml

   # ~/.config/dask/cloudprovider.yaml

   cloudprovider:
     hetzner:
       token: "yourtoken"

.. code-block:: console

   $ export DASK_CLOUDPROVIDER__HETZNER__TOKEN="yourtoken"


.. autoclass:: HetznerCluster
   :members:
