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

iobench
=======

The iobench tool allows to perform microbenchmarks on Crail.

Examples
--------

Synchronously write 1MB 1024 times to get a 1GB file:

.. code-block:: bash

   $CRAIL_HOME/bin/crail iobench -t write -f /filename -s $((1024*1024)) -k 1024

Read 1024 1MB buffers asynchronously with a batch size of 4:


.. code-block:: bash

   $CRAIL_HOME/bin/crail iobench -t readSequentialAsync -f /filename -s $((1024*1024)) -k 1024 -b 4

Command Reference
-----------------

.. list-table::
   :header-rows: 1

   * - Argument
     - Default
     - Experiment type
     - Description
   * - :code:`-t <experiment>`
     - *-*
     - N/A
     -  * :code:`write` - sequential sync write
        * :code:`writeAsync` - sequential async write
        * :code:`readSequential` - sequential sync read
        * :code:`readRandom` - random sync read
        * :code:`readSequentialAsync` - sequential async read
        * :code:`readMultiStream` - multistream read
        * :code:`createFile` - create file RPC
        * :code:`createFileAsync` - create file async RPC
        * :code:`createMultiFile` - create multifile
        * :code:`getKey` - getKey RPC
        * :code:`getFile` - getFile sync RPC
        * :code:`getFileAsync` - getFile async RPC
        * :code:`enumerateDir` - enumerate directory
        * :code:`browseDir` - browse directory
        * :code:`writeInt` - write integer
        * :code:`readInt` - read integer
        * :code:`seekInt` - seek integer
        * :code:`readMultiStreamInt` - read integer multistream
        * :code:`printLocationclass` - print machine's location class
   * - :code:`-f <path>`
     - /tmp.dat
     -  * :code:`write`
        * :code:`writeAsync`
        * :code:`readSequential`
        * :code:`readRandom`
        * :code:`readSequentialAsync`
        * :code:`readMultiStream`
        * :code:`createFile`
        * :code:`createFileAsync`
        * :code:`createMultiFile`
        * :code:`getKey`
        * :code:`getFile`
        * :code:`getFileAsync`
        * :code:`enumerateDir`
        * :code:`browseDir`
        * :code:`writeInt`
        * :code:`readInt`
        * :code:`seekInt`
        * :code:`readMultiStreamInt`
     - Path to perform operation with
   * - :code:`-s <size>`
     - :ref:`crail.buffersize <crail-site.conf>`
     -  * :code:`write`
        * :code:`writeAsync`
        * :code:`readSequential`
        * :code:`readRandom`
        * :code:`readSequentialAsync`
        * :code:`readMultiStream`
        * :code:`getKey`
     - Buffer size in bytes. Only relevant for buffered experiments.
   * - :code:`-k <n>`
     - 1
     -  * :code:`write`
        * :code:`writeAsync`
        * :code:`readSequential`
        * :code:`readRandom`
        * :code:`readSequentialAsync`
        * :code:`readMultiStream`
        * :code:`createFile`
        * :code:`createFileAsync`
        * :code:`getKey`
        * :code:`getFile`
        * :code:`getFileAsync`
        * :code:`writeInt`
        * :code:`readInt`
        * :code:`seekInt`
        * :code:`readMultiStreamInt`
     - Number of operations to perform
   * - :code:`-b <size>`
     - 1
     -  * :code:`writeAsync`
        * :code:`readSequentialAsync`
        * :code:`readMultiStream`
        * :code:`createFileAsync`
        * :code:`createMultiFile`
        * :code:`getFileAsync`
        * :code:`enumerateDir`
        * :code:`readMultiStreamInt`
     - Batch size of asynchronous requests.
   * - :code:`-c <storage_class>`
     - 0
     -  * :code:`write`
        * :code:`writeAsync`
        * :code:`createFile`
     - Storage class of file.
   * - :code:`-p <location_class>`
     - 0
     -  * :code:`write`
        * :code:`writeAsync`
        * :code:`createFile`
     - Location class of file
   * - :code:`-w <n>`
     - 32
     -  * :code:`write`
        * :code:`writeAsync`
        * :code:`readSequential`
        * :code:`readRandom`
        * :code:`readSequentialAsync`
        * :code:`readMultiStream`
        * :code:`createFile`
        * :code:`createFileAsync`
        * :code:`getFile`
        * :code:`getFileAsync`
        * :code:`enumerateDir`
     - Number of warmup operations
   * - :code:`-e <experiments>`
     - 1
     -  * :code:`readSequential`
        * :code:`readRandom`
        * :code:`readSequentialAsync`
        * :code:`readMultiStream`
     - Number of experiments to run
   * - :code:`-o <true/false>`
     - false
     -  * :code:`readSequential`
        * :code:`readRandom`
        * :code:`readSequentialAsync`
        * :code:`readMultiStream`
     - Keep file system open between experiments
   * - :code:`-d <true/false>`
     - false
     -  * :code:`write`
        * :code:`writeAsync`
     - Skip writing directory record
   * - :code:`-m <true/false`
     - true
     -  * :code:`write`
        * :code:`readSequential`
        * :code:`readRandom`
     - Use buffered streams


