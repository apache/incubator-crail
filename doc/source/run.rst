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

Run
===

For all deployments, make sure the :code:`$CRAIL_HOME` environment variable is
set on each machine to point to the top level Crail directory.

Starting Crail manually
-----------------------
The simplest way to run Crail is to start it manually on just a handful nodes.
You will need to start the Crail namenode, plus at least one datanode.
To start the namenode execute the following command on the host that is configured to be the namenode:

.. code-block:: bash

   $CRAIL_HOME/bin/crail namenode

To start a datanode run the following command on a host in the cluster
(ideally this is a different physical machine than the one running the namenode):

.. code-block:: bash

   $CRAIL_HOME/bin/crail datanode

Now you should have a small deployment up with just one datanode.
In this case the datanode is of type TCP/DRAM, which is the default datnode.
If you want to start a different storage tier you can do so by passing a specific
storage tier type.
You can find a list of supported storage tiers :ref:`here <Storage Tiers>`. For example:

.. code-block:: bash

   $CRAIL_HOME/bin/crail datanode -t org.apache.crail.storage.nvmf.NvmfStorageTier

starts the NVMf datanode. Note that configuration in :ref:`crail-site.conf` needs
to have the specific properties set of this type of datanode, in order for this to work.
Some storage tiers allow to set :ref:`configuration <Storage Tier Command Line>`
properties on the command line which can be appended after :code:`--` to the command line, e.g.:

.. code-block:: bash

   $CRAIL_HOME/bin/crail datanode -t org.apache.crail.storage.nvmf.NvmfStorageTier -- -a 192.168.0.2

Each storage tier instance can only belong to one storage class however the same
storage tier type can belong to multiple storage classes. Refer to :ref:`Storage Tiers`
for details. If there is only one storage class per type the storage class
is picked by the order in which they appear in :code:`crail.storage.types`
(:ref:`crail-site.conf`). Use :code:`-c <storage_class>` To start a storage tier
in a specific storage class, e.g.:


.. code-block:: bash

   $CRAIL_HOME/bin/crail datanode -t org.apache.crail.storage.nvmf.NvmfStorageTier -c 1

starts a NVMf storage tier in storage class 1 (storage classes start from 0).

Storage Tier Command Line
--------------------------

Command line arguments of the storage tiers override configuration
properties in :ref:`crail-site.conf`. Refer to :ref:`crail-site.conf` for
a detailed explanation of the properties and their default values.

TCP
~~~

==================  =================================
Argument            crail-site.conf
==================  =================================
:code:`-p <port>`   :code:`crail.storage.tcp.port`
:code:`-c <cores>`  :code:`crail.storage.tcp.cores`
==================  =================================

RDMA
~~~~

======================  =====================================
Argument                crail-site.conf
======================  =====================================
:code:`-i <interface>`  :code:`crail.storage.rdma.interface`
:code:`-p <port>`       :code:`crail.storage.rdma.port`
:code:`-s`              :code:`crail.storage.rdma.persistent`
======================  =====================================

NVMf
~~~~

=========================  =====================================
Argument                   crail-site.conf/Description
=========================  =====================================
:code:`-a <ip/hostname>`   :code:`crail.storage.nvmf.ip`
:code:`-p <port>`          :code:`crail.storage.nvmf.port`
:code:`-nqn <nqn>`         :code:`crail.storage.nvmf.nqn`
:code:`-n <namespace_id>`  Namespace id to use (default 1)
:code:`-hostnqn <nqn>`     :code:`crail.storage.nvmf.hostnqn`
=========================  =====================================

Larger deployments
------------------
To run larger deployments start Crail using

.. code-block:: bash

   $CRAIL_HOME/start-crail.sh

Similarly, Crail can be stopped by using

.. code-block:: bash

   $CRAIL_HOME/stop-crail.sh

For this to work include the list of machines to start datanodes in the :ref:`slaves` file.
You can start multiple datanode of different types on the same host as follows:

.. code-block:: bash

   host02
   host02 -t org.apache.crail.storage.nvmf.NvmfStorageTier -- -a 192.168.0.2
   host03

In this example, we are configuring a Crail cluster with 2 physical hosts but 3 datanodes and two different storage tiers.

Starting Crail in Docker
------------------------

Refer to :ref:`Docker` for how to run Crail in a Docker container.

