DigitalOcean
============

.. currentmodule:: dask_cloudprovider.digitalocean

.. autosummary::
   DropletCluster

Overview
--------

Authentication
^^^^^^^^^^^^^^

To authenticate with DigitalOcean you must first generate a
`personal access token <https://www.digitalocean.com/docs/apis-clis/api/create-personal-access-token/>`_.

Then you must put this in your Dask configuration at ``cloudprovider.digitalocean.token``. This can be done by
adding the token to your YAML configuration or exporting an environment variable.

.. code-block:: yaml

   # ~/.config/dask/cloudprovider.yaml

   cloudprovider:
     digitalocean:
       token: "yourtoken"

.. code-block:: console

   $ export DASK_CLOUDPROVIDER__DIGITALOCEAN__TOKEN="yourtoken"

Droplet
-------

.. autoclass:: DropletCluster
   :members: