Building from source
====================

Follow the steps below to build Crail from source.

Requirements
------------

* Java 8 or higher
* RDMA-based network, e.g., Infiniband, iWARP, RoCE. There are two options to run Crail without RDMA networking hardware: (a) use SoftiWARP, (b) us the TCP/DRAM storage tier
* Libdisni.so, available as part of `DiSNI <https://github.com/zrlio/disni>`_

Building
--------

To build Crail from source using `Apache Maven <http://maven.apache.org>`_ execute the following steps:

1. (a) Clone from Apache: :code:`git clone http://git-wip-us.apache.org/repos/asf/incubator-crail.git` or
   (b) Clone from Github: :code:`git clone https://github.com/apache/incubator-crail` or
   (c) Download and unpack the latest source release from `here <http://crail.apache.org/download>`_
2. Run: :code:`mvn -DskipTests install`
3. Copy tarball from :code:`assembly/target` to the cluster and unpack it using :code:`tar xvfz crail-X.Y-incubating-bin.tar.gz`

**Note:** *later, when deploying Crail, make sure libdisni.so is part of your LD_LIBRARY_PATH. The easiest way to make it work is to copy libdisni.so into crail-1.0/lib*
