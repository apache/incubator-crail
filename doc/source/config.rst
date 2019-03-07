.. Licensed under the Apache License, Version 2.0 (the "License"); you may not
.. use this file except in compliance with the License. You may obtain a copy of
.. the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
.. WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
.. License for the specific language governing permissions and limitations under
.. the License.

Configuration
=============

To configure Crail use the \*.template files as a basis and modify it to match your environment.
Set the :code:`$CRAIL_HOME` environment variable to your Crail deployment's path.

.. code-block:: bash

    cd $CRAIL_HOME/conf
    mv crail-site.conf.template crail-site.conf
    mv crail-env.sh.template crail-env.sh
    mv core-site.xml.template core-site.xml
    mv slaves.template slaves

**Note:** *Docker containers can be configured by using config files above. However it is only recommended for complex configurations. See* :ref:`Docker <Docker>` *for details.*

The purpuse of each of these files are:

* :ref:`crail-site.conf`: Configuration of the file system, data tiers and RPC
* :ref:`crail-env.sh`: Allows to pass additional JVM arguments
* :ref:`core-site.xml`: Configuration of the HDFS adapter
* :ref:`slaves`: Used by the start-crail.sh script to ease running Crail on multiple machines

crail-site.conf
---------------
There are a general file system properties and specific properties for the different storage tiers. Typical properties you might want to change are:

===============================    ======================   ======================================
Property                           Default Value            Description
===============================    ======================   ======================================
:code:`crail.namenode.address`     crail://localhost:9060   Namenode hostname and port
:code:`crail.cachelimit`           1073741824               Size (byte) of client buffer cache
:code:`crail.cachepath`            /dev/hugepages/cache     Hugepage path to client buffer cache
===============================    ======================   ======================================

Advanced properties (*Only modify if you know what you are doing*):

=====================================  =========================================  ===================================================
Property                               Default Value                              Description
=====================================  =========================================  ===================================================
:code:`crail.directorydepth`           16                                         Maximum depth of directory tree
:code:`crail.tokenexpiration`          10                                         Seconds write token is valid
:code:`crail.blocksize`                1048576                                    Size (byte) of block
:code:`crail.user`                     crail                                      Username used for HDFS adapter
:code:`crail.debug`                    false                                      Enable debug output
:code:`crail.statistics`               true                                       Collect statistics
:code:`crail.rpctimeout`               1000                                       RPC timeout in milliseconds
:code:`crail.datatimeout`              1000                                       Data operation timeout in milliseconds
:code:`crail.buffersize`               1048576                                    Size (byte) of buffer (buffered stream)
:code:`crail.slicesize`                524288                                     Size (byte) of slice (transfer unit)
:code:`crail.singleton`                true                                       Only create a single instance of the FS
:code:`crail.regionsize`               1073741824                                 Size (byte) of allocation unit (Cache)
:code:`crail.directoryrecord`          512                                        Size (byte) of directory entry
:code:`crail.directoryrandomize`       true                                       Randomize iteration of directories
:code:`crail.cacheimpl`                org.apache.crail.memory.MappedBufferCache  Client buffer cache implementation
:code:`crail.namenode.fileblocks`      16                                         File
:code:`crail.namenode.blockselection`  roundrobin                                 Block selection algorithm: roundrobin or random
=====================================  =========================================  ===================================================

RPC
~~~

Crail's modular architecture allows to plugin different kinds of RPC implementations. The :code:`crail.namenode.rpctype` property
is used to configure the RPC implementation. We currently offer two implementations:

* A TCP implementation based on `narpc <https://github.com/zrlio/narpc>`_ (default):
  **org.apache.crail.namenode.rpc.tcp.tcpnamenode**
* A RDMA implementation based on `darpc <https://github.com/zrlio/darpc>`_:
  **org.apache.crail.namenode.rpc.darpc.DaRPCNameNode**


Logging
'''''''

To allow shutting down the namenode without loosing data Crail offers namenode logging.
It can be enabled by setting a path to the log file with :code:`crail.namenode.log`.

**Note:** *this feature is experimental and should be used with caution*

Storage Tiers
~~~~~~~~~~~~~

Crail offers multiple types of datanode dependent on your network and storage requirements:

(a) TCP storage tier backed by DRAM (default)
(b) RDMA storage tier backed by DRAM
(c) NVMe over Fabrics storage tier, typically backed by NVMe drives

Crail allows to use multiple storage tier types together, e.g. to store hot data on
DRAM and cold data on NVMe, or extend your DRAM by NVMe storage. Storage types can be
configured as a comma separated list by setting the :code:`crail.storage.types` property:

(a) TCP: **org.apache.crail.storage.tcp.TcpStorageTier**
(b) RDMA: **org.apache.crail.storage.rdma.RdmaStorageTier**
(c) NVMf: **org.apache.crail.storage.nvmf.NvmfStorageTier**

Each of the storage types in the list defines a storage class, starting from storage class 0.
Types can appear multiple times to allow defining multiple storage classes for a type.
The maximum number of storage classes needs to be specified with the
:code:`crail.storage.classes` property (default = 1).
In the default configuration storage classes are used in incremental order, i.e.
storage class 0 is used until no more space is left then storage class 1 is used and so on.
However filesystem nodes (e.g. files) can also be created on a particular storage class and
can be configured to inherit the storage class of its container. The default storage
class of `/` is 0 however it can be configured via :code:`crail.storage.rootclass`.

Storage tiers send keep alive messages to the namenode to indicate that they are still
running and no error has occured. The interval in which keep alive message are
send can be configured in seconds with :code:`crail.storage.keepalive`.

Some of the configuration properties can be set via the command line when starting
a storage tier. Refer to :ref:`Run` for details.

TCP Tier
''''''''

The TCP storage tier (org.apache.crail.storage.tcp.TcpStorageTier) is backed by DRAM. The following
properties can be set to configure the storage tier:

========================================    ======================   ============================================================
Property                                    Default Value            Description
========================================    ======================   ============================================================
:code:`crail.storage.tcp.interface`         eth0                     Network interface to bind to
:code:`crail.storage.tcp.storagelimit`      1073741824               Size (Bytes) of DRAM to provide, multiple of allocation size
:code:`crail.storage.tcp.datapath`          /dev/hugepages/data      Hugepage path to data
========================================    ======================   ============================================================

Advanced properties:

========================================    ======================   ==============================================
Property                                    Default Value            Description
========================================    ======================   ==============================================
:code:`crail.storage.tcp.port`              50020                    Port to listen on
:code:`crail.storage.tcp.allocationsize`    crail.regionsize         Allocation unit
:code:`crail.storage.tcp.queuedepth`        16                       Data operation queue depth (single connection)
:code:`crail.storage.tcp.cores`             1                        Threads to process requests
========================================    ======================   ==============================================


RDMA Tier
'''''''''

The RDMA storage tier (org.apache.crail.storage.rdma.RdmaStorageTier) is backed by DRAM. The following
properties can be set to configure the storage tier:

========================================    ======================   ============================================================
Property                                    Default Value            Description
========================================    ======================   ============================================================
:code:`crail.storage.rdma.interface`        eth0                     Network interface to bind to
:code:`crail.storage.rdma.storagelimit`     1073741824               Size (Bytes) of DRAM to provide; multiple of allocation size
:code:`crail.storage.rdma.datapath`         /dev/hugepages/data      Hugepage path to data
========================================    ======================   ============================================================

Advanced properties:

=========================================    ======================   ========================================================
Property                                     Default Value            Description
=========================================    ======================   ========================================================
:code:`crail.storage.rdma.port`              50020                    Port to listen on
:code:`crail.storage.rdma.allocationsize`    crail.regionsize         Allocation unit
:code:`crail.storage.rdma.localmap`          true                     Use mmap if client is colocated with data tier
:code:`crail.storage.rdma.queuesize`         32                       Data operation queue depth (single connection)
:code:`crail.storage.rdma.type`              passive                  Operation type: passive or active (see DiSNI)
:code:`crail.storage.rdma.persistent`        false                    Allow restarting a data tier if namenode logging is used
:code:`crail.storage.rdma.backlog`           100                      Listen backlog
:code:`crail.storage.rdma.connecttimeout`    1000                     Connect timeout in milliseconds
=========================================    ======================   ========================================================

NVMf Tier
'''''''''

The NVMf storage tier (org.apache.crail.storage.nvmf.NvmfStorageTier) is typically backed by NVMe drives. However some target
implementations support using any block device. Unlike the RDMA and TCP storage tier the NVMf storage tier is not involved
in any data operation but only is used to provide metadata information. Crail uses the `jNVMf <https://github.com/zrlio/jNVMf>`_
library to connect to a standard NVMf target to gain metadata information about the storage and provide the information to the namenode.
Clients directly connect to the NVMf target. Crail has been tested to run with the Linux kernel, SPDK and Mellanox ConnectX-5
offloading target.

The following properties can be set to configure the storage tier:

========================================    ==========================  ============================================================
Property                                    Default Value               Description
========================================    ==========================  ============================================================
:code:`crail.storage.nvmf.ip`               localhost                   IP/hostname of NVMf target
:code:`crail.storage.nvmf.port`             50025                       Port of NVMf target
:code:`crail.storage.nvmf.nqn`              nqn.2017-06.io.crail:cnode  NVMe qualified name of NVMf controller
:code:`crail.storage.nvmf.namespace`        1                           Namespace of NVMe device
:code:`crail.storage.nvmf.hostnqn`          <random 128bit UUID>        NVMe qualified name of host
========================================    ==========================  ============================================================

Advanced properties:

===========================================    ======================   ==========================================================
Property                                       Default Value            Description
===========================================    ======================   ==========================================================
:code:`crail.storage.nvmf.allocationsize`      crail.regionsize         Allocation unit
:code:`crail.storage.nvmf.queueSize`           64                       NVMf submission queue size
:code:`crail.storage.nvmf.stagingcachesize`    262144                   Staging cache size (byte) for read-modify-write operations
===========================================    ======================   ==========================================================

crail-env.sh
------------

Modify crail-env.sh to pass additional JVM arguments to :code:`crail` respectively
:code:`start-crail.sh`.

It is recommended to increase heap (e.g. :code:`-Xmx24g`) and young generation heap size
(e.g. :code:`-Xmn16g`) for the namenodes and TCP datanodes to improve performance
for large deployments.

core-site.xml
-------------

To configure the HDFS adapter modify core-site.xml. For example the Crail shell
:code:`crail fs` uses the HDFS adapter thus requiring the core-site.xml file to
be setup. Modify :code:`fs.defaultFS` to match :code:`crail.namenode.address` in
:ref:`crail-site.conf`. The default is:

.. code-block:: xml

   <property>
     <name>fs.defaultFS</name>
     <value>crail://localhost:9060</value>
   </property>


slaves
------

The slaves file can be used to ease starting Crail on larger deployments. Refer
to :ref:`Run` for details. Each line should contain a hostname where a storage
tier is supposed to be started. Make sure the hostname allows passwordless ssh
connections. Note that the hostnames are not used by the storage tier itself but
only by the start/stop-crail.sh scripts to start and stop storage tiers.
IP/hostname of the storage tiers or any other configuration option are either
passed by command line arguments or via :ref:`crail-site.conf`.
Command line arguments can be configured in the slaves file following the hostname.

