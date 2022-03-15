Microsoft Azure
===============

.. currentmodule:: dask_cloudprovider.azure

.. autosummary::
   AzureVMCluster

Overview
--------

Authentication
^^^^^^^^^^^^^^

In order to create clusters on Azure you need to set your authentication credentials.
You can do this via the ``az`` `command line tool <https://docs.microsoft.com/en-us/cli/azure/install-azure-cli>`_.

.. code-block:: console

   $ az login

.. note::

   Setting the default output to ``table`` with ``az configure`` will make the ``az`` tool much easier to use.

Resource Groups
^^^^^^^^^^^^^^^

To create resources on Azure they must be placed in a resource group. Dask Cloudprovider will need a group to create
Dask components in.

You can list existing groups via the cli.

.. code-block:: console

   $ az group list

You can also create a new resource group if you do not have an existing one.

.. code-block:: console

   $ az group create --location <location> --name <resource group name> --subscription <subscription>

You can get a full list of locations with ``az account list-locations`` and subscriptions with ``az account list``.

Take note of your resource group name for later.

Virtual Networks
^^^^^^^^^^^^^^^^

Compute resources on Azure must be placed in virtual networks (vnet). Dask Cloudprovider will require an existing vnet to connect
compute resources to.

You can list existing vnets via the cli.

.. code-block:: console

   $ az network vnet list

You can also create a new vnet via the cli.

.. code-block:: console

   $ az network vnet create -g <resource group name> -n <vnet name> --address-prefix 10.0.0.0/16 \
         --subnet-name <subnet name> --subnet-prefix 10.0.0.0/24

This command will create a new vnet in your resource group with one subnet with the ``10.0.0.0/24`` prefix. For more than 255 compute resources you will need additional subnets.

Take note of your vnet name for later.

Security Groups
^^^^^^^^^^^^^^^

To allow network traffic to reach your Dask cluster you will need to create a security group which allows traffic on ports 8786-8787 from wherever you are.

You can list existing security groups via the cli.

.. code-block:: console

   $ az network nsg list

Or you can create a new security group.

.. code-block:: console

   $ az network nsg create -g <resource group name> --name <security group name>
   $ az network nsg rule create -g <resource group name> --nsg-name <security group name> -n MyNsgRuleWithAsg \
         --priority 500 --source-address-prefixes Internet --destination-port-ranges 8786 8787 \
         --destination-address-prefixes '*' --access Allow --protocol Tcp --description "Allow Internet to Dask on ports 8786,8787."

This example allows all traffic to 8786-8787 from the internet. It is recommended you make your rules more restrictive than this by limiting it to your corporate network
or specific IP.

Again take note of this security group name for later.

AzureVM
-------

.. autoclass:: AzureVMCluster
   :members:

Azure Spot Instance Plugin
--------------------------

.. autoclass:: AzurePreemptibleWorkerPlugin
   :members:
