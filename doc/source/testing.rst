Testing
=======

Tests in ``dask-cloudprovider`` and written and run using ``pytest``.

To set up your testing environment run:

.. code-block:: bash

    pip install -r requirements_test.txt

To run tests run ``pytest`` from the root directory

.. code-block:: bash

    pytest

You may notice that many tests will be skipped. This is because those tests create external resources on cloud providers. You can set those tests to run with the
``--create-external-resources`` flag.

.. warning::

   Running tests that create external resources are slow and will cost a small amount of credit on each cloud provider.

.. code-block:: bash

    pytest -rs --create-external-resources

It is also helpful to set the ``-rs`` flag here because tests may also skip if you do not have appropriate credentials to create those external resources.
If this is the case the skip reason will contain instructions on how to set up those credentials. For example

.. code-block::

    SKIPPED [1] dask_cloudprovider/azure/tests/test_azurevm.py:49:
        You must configure your Azure resource group and vnet to run this test.

            $ export DASK_CLOUDPROVIDER__AZURE__LOCATION="<LOCATION>"
            $ export DASK_CLOUDPROVIDER__AZURE__AZUREVM__RESOURCE_GROUP="<RESOURCE GROUP>"
            $ export DASK_CLOUDPROVIDER__AZURE__AZUREVM__VNET="<VNET>"
            $ export DASK_CLOUDPROVIDER__AZURE__AZUREVM__SECURITY_GROUP="<SECUROTY GROUP>"

