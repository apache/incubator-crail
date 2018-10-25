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

Spark
=====

Crail can be used to increase performance or enhance flexibility in
`Apache Spark <https://spark.apache.org/>`_. We provide multiple plugins to allow
Crail to be used as:

* :ref:`HDFS Adapter`: input and output
* :ref:`Spark-IO`: shuffle data and broadcast store

HDFS Adapter
------------

The Crail HDFS adapter is provided with every Crail :ref:`deployment <Deploy Crail>`.
The HDFS adpater allows to replace every HDFS path with a path on Crail.
However for it to be used for input and output in Spark the jar file paths
have to be added to the Spark configuration spark-defaults.conf:

.. code-block:: bash

   spark.driver.extraClassPath      $CRAIL_HOME/jars/*
   spark.executor.extraClassPath    $CRAIL_HOME/jars/*

Data in Crail can be accessed by prepending the value of :code:`crail.namenode.address`
from :ref:`crail-site.conf` to any HDFS path. For example :code:`crail://localhost:9060/test`
accesses :code:`/test` in Crail.
Note that Crail works independent of HDFS and does not interact with HDFS in
any way. However Crail does not completely replace HDFS since we do not offer
durability and fault tolerance cf. :ref:`Introduction`.
A good fit for Crail is for example inter-job data that can be recomputed
from the original data in HDFS.

Spark-IO
--------

Crail-Spark-IO contains various I/O accleration plugins for Spark tailored to
high-performance network and storage hardware (RDMA, NVMef, etc.).
Spark-IO is not provided with the default Crail deployment but can be
obtained `here <https://github.com/zrlio/crail-spark-io>`_.
Spark-IO currently contains two IO plugins: a shuffle engine and a broadcast module.
Both plugins inherit all the benefits of Crail such as very high performance
(throughput and latency) and multi-tiering (e.g., DRAM and flash).

Requirements
~~~~~~~~~~~~

* Spark >= 2.0
* Java 8
* Maven
* Crail >= 1.0

Building
~~~~~~~~

To build Crail execute the following steps:

1. Obtain a copy of Crail-Spark-IO from `Github <https://github.com/zrlio/crail-spark-io>`_
2. Make sure your local maven repository contains Crail, if not build Crail
   from :ref:`source <Building from source>`
3. Run: :code:`mvn -DskipTests install`


Configure Spark
~~~~~~~~~~~~~~~
To configure the crail shuffle plugin add the following lines to spark-defaults.conf

.. code-block:: bash

    spark.shuffle.manager           org.apache.spark.shuffle.crail.CrailShuffleManager

    spark.driver.extraClassPath     $CRAIL_HOME/jars/*:<path>/crail-spark-X.Y.jar:.
    spark.executor.extraClassPath   $CRAIL_HOME/jars/*:<path>/crail-spark-X.Y.jar:.


Since Spark version 2.0.0, broadcast is no longer an exchangeable plugin, unfortunately.
To use the Crail broadcast plugin in Spark it has to be manually added to Spark's BroadcastManager.scala.

Crail-TeraSort
--------------

SQL
---


