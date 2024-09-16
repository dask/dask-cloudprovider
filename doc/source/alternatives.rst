Alternatives
============

Many tools and services exist today for deploying Dask clusters, many of which are commonly used on the cloud.
This project aims to provide cloud native plugins and tools for Dask which can often compliment other approaches.

Community tools
---------------

Dask has a `vibrant ecosystem of community tooling for deploying Dask <https://docs.dask.org/en/latest/ecosystem.html#deploying-dask>`_ on various platforms. Many of which can be used on public cloud.

Kubernetes
^^^^^^^^^^

`Kubernetes <https://kubernetes.io/>`_ is an extremely popular project for managing cloud workloads and is part of the broader `Cloud Native Computing Foundation (CNCF) <https://www.cncf.io/>`_ ecosystem.

Dask has many options for `deploying clusters on Kubernetes <https://docs.dask.org/en/stable/deploying-kubernetes.html>`_.

HPC on Cloud
^^^^^^^^^^^^

Many popular HPC scheduling tools are used on the cloud and support features such as elastic scaling.
If you are already leveraging HPC tools like `SLURM on the cloud <https://slurm.schedmd.com/elastic_computing.html>`_ then `Dask has great integration with HPC schedulers <https://jobqueue.dask.org/en/latest/>`_.

Hadoop/Spark/Yarn
^^^^^^^^^^^^^^^^^

Many cloud platforms have popular managed services for running Apache Spark workloads.

If you're already using a managed map-reduce service like `Amazon EMR <https://aws.amazon.com/emr/>`_ then check out `dask-yarn <https://yarn.dask.org/en/latest/>`_.

Nebari
^^^^^^

`Nebari <https://www.nebari.dev/>`_ is an open source data science platform which can be run locally or on a cloud platform of your choice.
It includes a managed Dask service built on `Dask Gateway <http://gateway.dask.org/>`_ for managing Dask clusters.

Managed Services
----------------

Cloud vendors and third-party companies also offer managed Dask clusters as a service

Coiled
^^^^^^

`Coiled <https://www.coiled.io/>`_ is a mature managed Dask service that spawns clusters in your cloud account and allows you to manage them via a central control plane.

Saturn Cloud
^^^^^^^^^^^^

`Saturn Cloud <https://saturncloud.io/>`_ is a managed data science platform with hosted Dask clusters or the option to deploy them in your own AWS account.
