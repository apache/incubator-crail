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



Crail-TeraSort
--------------

SQL
---


