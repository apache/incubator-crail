Shell
=====

Crail provides an implementation of the HDFS API thus allows interaction using the HDFS shell.
For the HDFS adapter to work properly the :ref:`core-site.xml` needs to be configured
properly.

.. code-block:: bash

   $CRAIL_HOME/crail fs

Not all shell commands are support but the following operations have been tested to work:

.. code-block:: bash

   $CRAIL_HOME/crail fs -ls <crail_path>
   $CRAIL_HOME/crail fs -mkdir <crail_path>
   $CRAIL_HOME/crail fs -copyFromLocal <local_path> <crail_path>
   $CRAIL_HOME/crail fs -copyToLocal <crail_path> <local_path>
   $CRAIL_HOME/crail fs -cat <crail_path>

