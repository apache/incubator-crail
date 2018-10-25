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

Shell
=====

Crail provides an implementation of the HDFS API thus allows interaction using the HDFS shell.
For the HDFS adapter to work properly the :ref:`core-site.xml` needs to be configured
properly.

.. code-block:: bash

   $CRAIL_HOME/bin/crail fs

Not all shell commands are support but the following operations have been tested to work:

.. code-block:: bash

   $CRAIL_HOME/bin/crail fs -ls <crail_path>
   $CRAIL_HOME/bin/crail fs -mkdir <crail_path>
   $CRAIL_HOME/bin/crail fs -copyFromLocal <local_path> <crail_path>
   $CRAIL_HOME/bin/crail fs -copyToLocal <crail_path> <local_path>
   $CRAIL_HOME/bin/crail fs -cat <crail_path>

