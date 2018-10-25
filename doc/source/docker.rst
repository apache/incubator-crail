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

Docker
======

The easiest way to run Crail is to use Docker images. We provide two preconfigured
Docker images:

(1) TCP/DRAM: **apache/crail**
(2) RDMA/DRAM: **apache/crail_rdma**

If you want to run other or more complex configurations you can use either image
as a basis and provide your own configuration files.
Refer to :ref:`Own configurations` for details.

(1) and 2. share the following configuration parameters:

========================================    ======================   ==============================================
Property                                    Default Value            Description
========================================    ======================   ==============================================
:code:`NAMENODE_HOST`                       localhost                Namenode hostname/ip to bind to
:code:`NAMENODE_PORT`                       9060                     Namenode port to listen on
:code:`INTERFACE`                           eth0                     Datanode network interface to listen on
:code:`DATAPATH`                            /dev/hugepages/data      Datanode hugepage path to data
:code:`STORAGELIMIT`                        1073741824               Datanode Size (Bytes) of DRAM to provide
:code:`CACHEPATH`                           /dev/hugepages/cache     Client/Datanode hugepage path to buffer cache
:code:`CACHELIMIT`                            0                        Size (Bytes) of hugepage buffer cache
========================================    ======================   ==============================================

These properties can be specified as environment variables when
starting a Docker image with :code:`-e <property>=<value>`.

TCP image
---------

To run Crail you first need to start the namenode. For example

.. code-block:: bash

   docker run -it --network host -e NAMENODE_HOST=host02 -e INTERFACE=eth5 apache/crail namenode

starts a TCP namenode using Docker's host network configuration on host02 on interface eth5.
The TCP tier allows for other network configurations.
Refer to https://docs.docker.com/network/ for details.

Once the namenode has started start a Crail TCP storage tier (preferably on a different node).
For example


.. code-block:: bash

   docker run -it --network host -e NAMENODE_HOST=host02 -e INTERFACE=eth5 apache/crail datanode

starts a TCP datanode with 1GB of storage (default) listening on eth5. It is recommended
to put the data directory on a hugetlb mount. You can do this by passing an
mounted hugetlb directory on the host as a volume to Docker. For example


.. code-block:: bash

   docker run -it --network host -e NAMENODE_HOST=host02 -e INTERFACE=eth5 -v /dev/hugepages:/dev/hugepages apache/crail datanode

passes the hugetlb mount /dev/hugepages to the container.

RDMA image
----------

To run Crail/RDMA/DRAM you can start the namenode as follows:

.. code-block:: bash

    docker run -it --network host -e NAMENODE_HOST=host02 -e INTERFACE=eth5 --cap-add=IPC_LOCK --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/rdma_cm -v /dev/hugepages:/dev/hugepages apache/crail_rdma namenode

This starts a namenode on host02 using the host's network on eth5. Note that the
uverbs device needs to match the interface.

To run a RDMA storage tier:


.. code-block:: bash

    docker run -it --network host -e NAMENODE_HOST=host02 -e INTERFACE=eth5 --cap-add=IPC_LOCK --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/rdma_cm -v /dev/hugepages:/dev/hugepages apache/crail_rdma datanode

**Note:** *The RDMA docker image provides default RDMA provider libraries from Ubuntu 18.04.
They might not be compatible with your host's RDMA stack.
To install your own RDMA libraries use -v or create your own Docker image from crail_rdma.*

Own configurations
------------------

If you want to run more complex configurations that are not covered by the options above you have two options:

(1) Create your own Docker image by creating a Dockerfile and using the crail images as a source. You can then change the configuration by e.g. copying your own config into the image
(2) Pass your config as a volume with :code:`-v <local_path>:<docker_path>`

