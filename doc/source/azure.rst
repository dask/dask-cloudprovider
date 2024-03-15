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

Extra options
^^^^^^^^^^^^^

To further customize the VMs created, you can provide ``extra_vm_options`` to :class:`AzureVMCluster`. For example, to set the identity
of the virtual machines to a (previously created) user assigned identity, create an ``azure.mgmt.compute.models.VirtualMachineIdentity``

.. code-block:: python

   >>> import os
   >>> import azure.identity
   >>> import dask_cloudprovider.azure
   >>> import azure.mgmt.compute.models

   >>> subscription_id = os.environ["DASK_CLOUDPROVIDER__AZURE__SUBSCRIPTION_ID"]
   >>> rg_name = os.environ["DASK_CLOUDPROVIDER__AZURE__RESOURCE_GROUP"]
   >>> identity_name = "dask-cloudprovider-identity"
   >>> v = azure.mgmt.compute.models.UserAssignedIdentitiesValue()
   >>> user_assigned_identities = {
   ...     f"/subscriptions/{subscription_id}/resourcegroups/{rg_name}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{identity_name}": v
   ... }
   >>> identity = azure.mgmt.compute.models.VirtualMachineIdentity(
   ...     type="UserAssigned",
   ...     user_assigned_identities=user_assigned_identities   
   ... )


And then provide that to :class:`AzureVMCluster`

.. code-block:: python

   >>> cluster = dask_cloudprovider.azure.AzureVMCluster(extra_vm_options={"identity": identity.as_dict()})
   >>> cluster.scale(1)

Dask Configuration
^^^^^^^^^^^^^^^^^^

You'll provide the names or IDs of the Azure resources when you create a :class:`AzureVMCluster`. You can specify
these values manually, or use Dask's `configuration system <https://docs.dask.org/en/stable/configuration.html>`_
system. For example, the ``resource_group`` value can be specified using an environment variable:

.. code-block:: console

   $ export DASK_CLOUDPROVIDER__AZURE__RESOURCE_GROUP="<resource group name>"
   $ python

Or you can set it in a YAML configuration file.

.. code-block:: yaml

   cloudprovider:
     azure:
       resource_group: "<resource group name>"
       azurevm:
        vnet: "<vnet name>"

Note that the options controlling the VMs are under the `cloudprovider.azure.azurevm` key.

See :doc:`config` for more.

AzureVM
-------

.. autoclass:: AzureVMCluster
   :members:

Azure Spot Instance Plugin
--------------------------

.. autoclass:: AzurePreemptibleWorkerPlugin
   :members:
