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

Programming against Crail
=========================

The best way to program against Crail is to use Maven. Make sure you have the Crail dependency specified in your application pom.xml file
with the latest Crail version (e.g. 1.1-incubating):

.. code-block:: xml

   <dependency>
     <groupId>org.apache.crail</groupId>
     <artifactId>crail-client</artifactId>
     <version>X.Y</version>
   </dependency>

Then, create a Crail client as follows:

.. code-block:: Java

   CrailConfiguration conf = new CrailConfiguration();
   CrailStore store = CrailStore.newInstance(conf);

Make sure the :code:`$CRAIL_HOME/conf` directory is part of the classpath.

Crail supports different file types. The simplest way to create a file in Crail is as follows:

.. code-block:: Java

   CrailFile file = store.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT).get().syncDir();

Aside from the actual filename, the :code:`create()` call takes as input the storage and location classes which are preferences for the storage tier and physical location that this file should be created in. Crail tries to satisfy these preferences later when the file is written. In the example we do not request any particular storage or location affinity.

This :code:`create()` command is non-blocking, calling :code:`get()` on the returning future object awaits the completion of the call. At that time, the file has been created, but its directory entry may not be visible. Therefore, the file may not yet show up in a file enumeration of the given parent directory. Calling :code:`syncDir()` waits to for the directory entry to be completed. Both the :code:`get()` and the :code:`syncDir()` operation can be deffered to a later time at which they may become non-blocking operations.

Once the file is created, a file stream can be obtained for writing:

.. code-block:: Java

   CrailBufferedOutputStream outstream = file.getBufferedOutputStream(1024);

Here, we create a buffered stream so that we can pass heap byte arrays as well. We could also create a non-buffered stream using

.. code-block:: Java

   CrailOutputStream outstream = file.getDirectOutputStream(1024);

In both cases, we pass a write hint (1024 in the example) that indicates to Crail how much data we are intending to write. This allows Crail to optimize metadatanode lookups. Crail never prefetches data, but it may fetch the metadata of the very next operation concurrently with the current data operation if the write hint allows to do so.

Once the stream has been obtained, there exist various ways to write a file. The code snippet below shows the use of the asynchronous interface:

.. code-block:: Java

    CrailBuffer dataBuf = fs.allocateBuffer();
    Future<DataResult> future = outputStream.write(dataBuf);
    ...
    future.get();

Reading files works very similar to writing. There exist various examples in org.apache.crail.tools.CrailBenchmark.

