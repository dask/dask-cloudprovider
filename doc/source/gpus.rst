GPU clusters
============

.. currentmodule:: dask_cloudprovider

Many cloud providers have GPU offerings and so it is possible to launch GPU enabled Dask clusters
with Dask Cloudprovider.

Each cluster manager handles this differently but generally you will need to configure the following settings:

- Configure the hardware to include GPUs. This may be by changing the hardware type or adding accelerators.
- Ensure the OS/Docker image has the NVIDIA drivers. For Docker images it is recommended to use the [RAPIDS images](https://hub.docker.com/r/rapidsai/rapidsai/).
- Set the ``worker_module`` config option to ``dask_cuda.cli.dask_cuda_worker`` or ``worker_command`` option to ``dask-cuda-worker``.

In the following AWS :class:`dask_cloudprovider.aws.EC2Cluster` example we set the ``ami`` to be a Deep Learning AMI with NVIDIA drivers, the ``docker_image`` to RAPIDS, the ``instance_type``
to ``p3.2xlarge`` which has one NVIDIA Tesla V100 and the ``worker_module`` to ``dask_cuda.cli.dask_cuda_worker``.

.. code-block:: python

    >>> cluster = EC2Cluster(ami="ami-0c7c7d78f752f8f17",  # Example Deep Learning AMI (Ubuntu 18.04)
                             docker_image="rapidsai/rapidsai:cuda10.1-runtime-ubuntu18.04",
                             instance_type="p3.2xlarge",
                             worker_module="dask_cuda.cli.dask_cuda_worker",
                             bootstrap=False,
                             filesystem_size=120)

See each cluster manager's example sections for info on starting a GPU cluster.