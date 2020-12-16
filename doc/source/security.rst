Security
========

Dask Cloudprovider aims to balance ease of use with security best practices. The two are not always compatible so this document aims to outline the compromises and decisions made in this library.

Public Schedulers
-----------------

For each cluster manager to work correctly it must be able to make a connection to the Dask scheduler on port ``8786``.
In many cluster managers the default option is to expose the Dask scheduler and dashboard to the internet via a public IP address.
This makes things quick and easy for new users to get up and running, but may pose a security risk long term.

Many organisations have policies which do not allow users to assign public IP addresses or open ports. Our best practices
advice is to use Dask Cloudprovider from within a cloud platform, either from a VM or a managed environment. Then disable public
networking.

See each cluster manager for configuration options.

Authentication and encryption
-----------------------------

Cluster managers such as :class:`dask_cloudprovider.aws.EC2Cluster`, :class:`dask_cloudprovider.azure.AzureVMCluster`,
:class:`dask_cloudprovider.gcp.GCPCluster` and :class:`dask_cloudprovider.digitalocean.DropletCluster` enable certificate based authentication
and encryption by default.

When a cluster is launched with any of these cluster managers a set of temporary keys will be generated and distributed to the cluster nodes
via their startup script. All communication between the client, scheduler and workers will then be encrypted and only clients and workers with
valid certificates will be able to connect to the scheduler.

You can also specify your own certificates using the :class:`distributed.Security` object.

.. code-block:: python

    >>> from dask_cloudprovider.gcp import GCPCluster
    >>> from dask.distributed import Client
    >>> from distributed.security import Security
    >>> sec = Security(tls_ca_file='cluster_ca.pem',
    ...                tls_client_cert='cli_cert.pem',
    ...                tls_client_key='cli_key.pem',
    ...                require_encryption=True)
    >>> cluster = GCPCluster(n_workers=1, security=sec)
    >>> client = Client(cluster)
    >>> client
    <Client: 'tls://10.142.0.29:8786' processes=0 threads=0, memory=0 B>

You can disable secure connections by setting the ``security`` keyword argument to ``False``. This may be desirable when troubleshooting or
when running on a trusted network (entirely inside a VPC for example).