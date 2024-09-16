Openstack
============

.. currentmodule:: dask_cloudprovider.openstack

.. autosummary::
   OpenStackCluster

Overview
--------

Authentication
^^^^^^^^^^^^^^

To authenticate with the OpenStack Identity service (Keystone) 

1) Get your Authentication URL (auth_url) for OpenStack Identity service (Keystone) and put it in your Dask configuration at ``cloudprovider.openstack.auth_url``.

2) Get your `region <https://docs.openstack.org/python-openstackclient/latest/cli/command-objects/region.html>`_ and put it in your Dask configuration at ``cloudprovider.openstack.region``.
    .. code-block:: console

        $ openstack region list
        +-----------+---------------+-------------+
        | Region    | Parent Region | Description |
        +-----------+---------------+-------------+
        | RegionOne | None          |             |
        +-----------+---------------+-------------+

3) Generate an  `application credential <https://docs.openstack.org/keystone/latest/user/application_credentials.html>`_.

    .. code-block:: console

        $ openstack application credential create dask --unrestricted
        +--------------+----------------------------------------------------------------------------------------+
        | Field        | Value                                                                                  |
        +--------------+----------------------------------------------------------------------------------------+
        | description  | None                                                                                   |
        | expires_at   | None                                                                                   |
        | id           | 0a0372dbedfb4e82ab66449c3316ef1e                                                       |
        | name         | dask                                                                             |
        | project_id   | e99b6f4b9bf84a9da27e20c9cbfe887a                                                       |
        | roles        | Member anotherrole                                                                     |
        | secret       | ArOy6DYcLeLTRlTmfvF1TH1QmRzYbmD91cbVPOHL3ckyRaLXlaq5pTGJqvCvqg6leEvTI1SQeX3QK-3iwmdPxg |
        | unrestricted | True                                                                                   |
        +--------------+----------------------------------------------------------------------------------------+

    and put ``application_credential_id`` and ``application_credential_secret`` in your Dask configuration at ``cloudprovider.openstack.application_credential_id``
    and ``cloudprovider.openstack.application_credential_secret``. 

All of this variables can be gathered from either `OpenStack RC file <https://docs.openstack.org/newton/user-guide/common/cli-set-environment-variables-using-openstack-rc.html>`_ 
or `clouds.yaml file <https://docs.openstack.org/python-openstackclient/latest/configuration/index.html>`_.

Example Config File
^^^^^^^^^^^^^^
.. code-block:: yaml

   # ~/.config/dask/cloudprovider.yaml

    cloudprovider:
      openstack:
        region: "RegionOne"
        auth_url: "https://cloud.home.karatosun.xyz:5000"
        application_credential_id: "0a0372dbedfb4e82ab66449c3316ef1e"
        application_credential_secret: "ArOy6DYcLeLTRlTmfvF1TH1QmRzYbmD91cbVPOHL3ckyRaLXlaq5pTGJqvCvqg6leEvTI1SQeX3QK-3iwmdPxg"
        auth_type: "v3applicationcredential"

You can also export them as environment variables.

.. code-block:: console

   $ export DASK_CLOUDPROVIDER__APPLICATION_CREDENTIAL_ID="0a0372dbedfb4e82ab66449c3316ef1e"


.. autoclass:: OpenStackCluster
   :members:
