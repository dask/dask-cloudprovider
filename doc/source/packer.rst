Creating custom OS images with Packer
=====================================

Many cloud providers in Dask Cloudprovider involve creating VMs and installing dependencies on those VMs at boot time.

This can slow down the creation and scaling of clusters, so this page discusses building custom images using `Packer <https://www.packer.io/>`_ to speed up cluster creation.

Packer is a utility which boots up a VM on your desired cloud, runs any installation steps and then takes a snapshot of the VM for use as a template for creating
new VMs later. This allows us to run through the installation steps once, and then reuse them when starting Dask components.

Installing Packer
-----------------

See the `official install docs <https://www.packer.io/docs/install>`_.

Packer Overview
---------------

To create an image with packer we need to create a JSON config file.

A Packer config file is broken into a couple of sections, ``builders`` and ``provisioners``.

A builder configures what type of image you are building (AWS AMI, GCP VMI, etc). It describes the base
image you are building on top of and connection information for Packer to connect to the build instance.

When you run ``packer build /path/to/config.json`` a VM (or multiple VMs if you configure more than one) will be
created automatically based on your ``builders`` config section.

Once your build VM is up and running the ``provisioners`` will be run. These are steps to configure and provision your
machine. In the examples below we are mostly using the ``shell`` provisioner which will run commands on the VM to set things
up.

Once your provisioning scripts have completed the VM will automatically stop, a snapshot will be taken and you will be provided
with an ID which you can then use as a template in future runs of ``dask-cloudprovider``.

Image Requirements
------------------

Each cluster manager that uses VMs will have specific requirements for the VM image.

The AWS ``ECSCluster`` for example requires `ECS optimised AMIs <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-optimized_AMI.html>`_.

The VM cluster managers such as ``EC2cluster`` and ``DropletCluster`` just require `Docker <https://docs.docker.com/engine/install/>`_ to be installed (or `NVIDIA Docker <https://github.com/NVIDIA/nvidia-docker>`_ for GPU VM types).

Examples
--------

``EC2Cluster`` with cloud-init
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When any of the ``VMCluster`` based cluster managers, such as ``EC2Cluster``, lauch a new default VM it uses the Ubuntu base image and installs all dependencies
with `cloud-init <https://cloudinit.readthedocs.io/en/latest/>`_.

Instead of doing this every time we could use Packer to do this once, and then reuse that image every time.

Each ``VMCluster`` cluster manager has a class method called ``get_cloud_init`` which takes the same keyword arguments as creating the object itself, but instead
returns the cloud-init file that would be generated.

.. code-block:: python

    from dask_cloudprovider.aws import EC2Cluster

    cloud_init_config = EC2Cluster.get_cloud_init(
        # Pass any kwargs here you would normally pass to ``EC2Cluster``
    )
    print(cloud_init_config)

We should see some output like this.

.. code-block:: YAML

    #cloud-config

    packages:
    - apt-transport-https
    - ca-certificates
    - curl
    - gnupg-agent
    - software-properties-common

    # Enable ipv4 forwarding, required on CIS hardened machines
    write_files:
    - path: /etc/sysctl.d/enabled_ipv4_forwarding.conf
        content: |
        net.ipv4.conf.all.forwarding=1

    # create the docker group
    groups:
    - docker

    # Add default auto created user to docker group
    system_info:
    default_user:
        groups: [docker]

    runcmd:

    # Install Docker
    - curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
    - add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
    - apt-get update -y
    - apt-get install -y docker-ce docker-ce-cli containerd.io
    - systemctl start docker
    - systemctl enable docker

    # Run container
    - docker run --net=host  daskdev/dask:latest dask-scheduler --version

We should save this output somewhere for reference later. Let's refer to it as ``/path/to/cloud-init-config.yaml``.

Next we need a Packer config file to build our image, let's refer to it as ``/path/to/config.json``.
We will use the official Ubuntu 20.04 image and specify our cloud-init config file in the ``user_data_file`` option.

Packer will not necesserily wait for our cloud-init config to finish executing before taking a snapshot, so we need to add a provisioner
that will block until the cloud-init completes.

.. code-block:: JSON

    {
        "builders": [
            {
                "type": "amazon-ebs",
                "region": "eu-west-2",
                "source_ami_filter": {
                    "filters": {
                        "virtualization-type": "hvm",
                        "name": "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*",
                        "root-device-type": "ebs"
                    },
                    "owners": [
                        "099720109477"
                    ],
                    "most_recent": true
                },
                "instance_type": "t2.micro",
                "ssh_username": "ubuntu",
                "ami_name": "dask-cloudprovider {{timestamp}}",
                "user_data_file": "/path/to/cloud-init-config.yaml"
            }
        ],
        "provisioners": [
            {
                "type": "shell",
                "inline": [
                    "echo 'Waiting for cloud-init'; while [ ! -f /var/lib/cloud/instance/boot-finished ]; do sleep 1; done; echo 'Done'"
                ]
            }
        ]
    }

Then we can build our image with ``packer build /path/to/config.json``.

.. code-block::

    $ packer build /path/to/config.json
    amazon-ebs: output will be in this color.

    ==> amazon-ebs: Prevalidating any provided VPC information
    ==> amazon-ebs: Prevalidating AMI Name: dask-cloudprovider 1600875672
        amazon-ebs: Found Image ID: ami-062c2b6de9e9c54d3
    ==> amazon-ebs: Creating temporary keypair: packer_5f6b6c99-46b5-6002-3126-8dcb1696f969
    ==> amazon-ebs: Creating temporary security group for this instance: packer_5f6b6c9a-bd7d-8bb3-58a8-d983f0e95a96
    ==> amazon-ebs: Authorizing access to port 22 from [0.0.0.0/0] in the temporary security groups...
    ==> amazon-ebs: Launching a source AWS instance...
    ==> amazon-ebs: Adding tags to source instance
        amazon-ebs: Adding tag: "Name": "Packer Builder"
        amazon-ebs: Instance ID: i-0531483be973d60d8
    ==> amazon-ebs: Waiting for instance (i-0531483be973d60d8) to become ready...
    ==> amazon-ebs: Using ssh communicator to connect: 18.133.244.42
    ==> amazon-ebs: Waiting for SSH to become available...
    ==> amazon-ebs: Connected to SSH!
    ==> amazon-ebs: Provisioning with shell script: /var/folders/0l/fmwbqvqn1tq96xf20rlz6xmm0000gp/T/packer-shell512450076
        amazon-ebs: Waiting for cloud-init
        amazon-ebs: Done
    ==> amazon-ebs: Stopping the source instance...
        amazon-ebs: Stopping instance
    ==> amazon-ebs: Waiting for the instance to stop...
    ==> amazon-ebs: Creating AMI dask-cloudprovider 1600875672 from instance i-0531483be973d60d8
        amazon-ebs: AMI: ami-064f8db7634d19647
    ==> amazon-ebs: Waiting for AMI to become ready...
    ==> amazon-ebs: Terminating the source AWS instance...
    ==> amazon-ebs: Cleaning up any extra volumes...
    ==> amazon-ebs: No volumes to clean up, skipping
    ==> amazon-ebs: Deleting temporary security group...
    ==> amazon-ebs: Deleting temporary keypair...
    Build 'amazon-ebs' finished after 4 minutes 5 seconds.

    ==> Wait completed after 4 minutes 5 seconds

    ==> Builds finished. The artifacts of successful builds are:
    --> amazon-ebs: AMIs were created:
    eu-west-2: ami-064f8db7634d19647

Then to use our new image we can create an ``EC2Cluster`` specifying the AMI and disabling the automatic bootstrapping.

.. code-block:: python

    from dask.distributed import Client
    from dask_cloudprovider.aws import EC2Cluster

    cluster = EC2Cluster(
        ami="ami-064f8db7634d19647",  # AMI ID provided by Packer
        bootstrap=False
    )
    cluster.scale(2)

    client = Client(cluster)
    # Your cluster is ready to use

``EC2Cluster`` with RAPIDS
^^^^^^^^^^^^^^^^^^^^^^^^^^

To launch `RAPIDS <https://rapids.ai/>`_ on AWS EC2 we can select a GPU instance type, choose the official Deep Learning AMIs that Amazon provides and run the official RAPIDS Docker image.

.. code-block:: python

    from dask_cloudprovider.aws import EC2Cluster

    cluster = EC2Cluster(
        ami="ami-0c7c7d78f752f8f17",  # Deep Learning AMI (this ID varies by region so find yours in the AWS Console)
        docker_image="rapidsai/rapidsai:cuda10.1-runtime-ubuntu18.04-py3.9",
        instance_type="p3.2xlarge",
        bootstrap=False,  # Docker is already installed on the Deep Learning AMI
        filesystem_size=120,
    )
    cluster.scale(2)

However every time a VM is created by ``EC2Cluster`` the RAPIDS Docker image will need to be pulled from Docker Hub.
The result is that the above snippet can take ~20 minutes to run, so let's create our own AMI which already has the RAPIDS image pulled.

In our builders section we will specify we want to build on top of the latest Deep Learning AMI by specifying
``"Deep Learning AMI (Ubuntu 18.04) Version *"`` to list all versions and ``"most_recent": true`` to use the most recent.

We also restrict the owners to ``898082745236`` which is the ID for the official image channel.

The official image already has the NVIDIA drivers and NVIDIA Docker runtime installed so the only step we need to do is to
pull the RAPIDS Docker image. That way when a scheduler or worker VM is created the image will already be available on the machine.

.. code-block:: JSON

    {
        "builders": [
            {
                "type": "amazon-ebs",
                "region": "eu-west-2",
                "source_ami_filter": {
                    "filters": {
                        "virtualization-type": "hvm",
                        "name": "Deep Learning AMI (Ubuntu 18.04) Version *",
                        "root-device-type": "ebs"
                    },
                    "owners": [
                        "898082745236"
                    ],
                    "most_recent": true
                },
                "instance_type": "p3.2xlarge",
                "ssh_username": "ubuntu",
                "ami_name": "dask-cloudprovider-rapids {{timestamp}}"
            }
        ],
        "provisioners": [
            {
                "type": "shell",
                "inline": [
                    "docker pull rapidsai/rapidsai:cuda10.1-runtime-ubuntu18.04-py3.9"
                ]
            }
        ]
    }

Then we can build our image with ``packer build /path/to/config.json``.

.. code-block::

    $ packer build /path/to/config.json
    ==> amazon-ebs: Prevalidating any provided VPC information
    ==> amazon-ebs: Prevalidating AMI Name: dask-cloudprovider-gpu 1600868638
        amazon-ebs: Found Image ID: ami-0c7c7d78f752f8f17
    ==> amazon-ebs: Creating temporary keypair: packer_5f6b511e-d3a3-c607-559f-d466560cd23b
    ==> amazon-ebs: Creating temporary security group for this instance: packer_5f6b511f-8f62-cf98-ca54-5771f1423d2d
    ==> amazon-ebs: Authorizing access to port 22 from [0.0.0.0/0] in the temporary security groups...
    ==> amazon-ebs: Launching a source AWS instance...
    ==> amazon-ebs: Adding tags to source instance
        amazon-ebs: Adding tag: "Name": "Packer Builder"
        amazon-ebs: Instance ID: i-077f54ed4ae6bcc66
    ==> amazon-ebs: Waiting for instance (i-077f54ed4ae6bcc66) to become ready...
    ==> amazon-ebs: Using ssh communicator to connect: 52.56.96.165
    ==> amazon-ebs: Waiting for SSH to become available...
    ==> amazon-ebs: Connected to SSH!
    ==> amazon-ebs: Provisioning with shell script: /var/folders/0l/fmwbqvqn1tq96xf20rlz6xmm0000gp/T/packer-shell376445833
        amazon-ebs: Waiting for cloud-init
        amazon-ebs: Bootstrap complete
    ==> amazon-ebs: Stopping the source instance...
        amazon-ebs: Stopping instance
    ==> amazon-ebs: Waiting for the instance to stop...
    ==> amazon-ebs: Creating AMI dask-cloudprovider-gpu 1600868638 from instance i-077f54ed4ae6bcc66
        amazon-ebs: AMI: ami-04e5539cb82859e69
    ==> amazon-ebs: Waiting for AMI to become ready...
    ==> amazon-ebs: Terminating the source AWS instance...
    ==> amazon-ebs: Cleaning up any extra volumes...
    ==> amazon-ebs: No volumes to clean up, skipping
    ==> amazon-ebs: Deleting temporary security group...
    ==> amazon-ebs: Deleting temporary keypair...
    Build 'amazon-ebs' finished after 20 minutes 35 seconds.

It took over 20 minutes to build this image, but now that we've done it once we can reuse the image in our RAPIDS powered Dask clusters.

We can then run our code snippet again but this time it will take less than 5 minutes to get a running cluster.

.. code-block:: python

    from dask.distributed import Client
    from dask_cloudprovider.aws import EC2Cluster

    cluster = EC2Cluster(
        ami="ami-04e5539cb82859e69",  # AMI ID provided by Packer
        docker_image="rapidsai/rapidsai:cuda10.1-runtime-ubuntu18.04-py3.9",
        instance_type="p3.2xlarge",
        bootstrap=False,
        filesystem_size=120,
    )
    cluster.scale(2)

    client = Client(cluster)
    # Your cluster is ready to use
