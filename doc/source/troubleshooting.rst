Troubleshooting
===============

This document contains frequently asked troubleshooting problems.

Unable to connect to scheduler
------------------------------

The most common issue is not being able to connect to the cluster once it has been constructed.

Each cluster manager will construct a Dask scheduler and by default expose it via a public IP address. You must be able
to connect to that address on ports ``8786`` and ``8787`` from wherver your Python session is.

If you are unable to connect to this address it is likely that there is something wrong with your network configuration,
for example you may have corporate policies implementing additional firewall rules on your account.

To reduce the chances of this happening it is often simplest to run Dask Cloudprovider from within the cloud you are trying
to use and configure private networking only. See your specific cluster manager docs for info.