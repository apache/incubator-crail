<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

# Crail

Crail is a fast multi-tiered distributed storage system designed from ground up for high-performance network and storage hardware. It marks the backbone of the Crail I/O architecture, which is described in more detail on [crail.incubator.apache.org](http://crail.incubator.apache.org). The unique features of Crail include:

* Zero-copy network access from userspace 
* Integration of multiple storage tiers such DRAM, flash and disaggregated shared storage
* Ultra-low latencies for both meta data and data operations. For instance: opening, reading and closing a small file residing in the distributed DRAM tier less than 10 microseconds, which is in the same ballpark as some of the fastest RDMA-based key/value stores
* High-performance sequential read/write operations: For instance: read operations on large files residing in the distributed DRAM tier are typically limited only by the performance of the network
* Very low CPU consumption: a single core sharing both application and file system client can drive sequential read/write operations at the speed of up to 100Gbps and more
* Asynchronous API leveraging the asynchronous nature of RDMA-based networking hardware
* Extensible plugin architecture: new storage tiers tailored to specific hardware can be added easily
 
Crail is implemented in Java offering a Java API which integrates directly with the Java off-heap memory. Crail is designed for performance critical temporary data within a scope of a rack or two. 

## Requirements

* Java 8 or higher
* RDMA-based network, e.g., Infiniband, iWARP, RoCE. There are two options to run Crail without RDMA networking hardware: (a) use SoftiWARP, (b) us the TCP/DRAM storage tier
* Libdisni.so, available as part of [DiSNI](https://github.com/zrlio/disni)

## Building 

To build Crail from source using [Apache Maven](http://maven.apache.org/) execute the following steps:

1. (a) Clone from Apache [git](http://git-wip-us.apache.org/repos/asf/incubator-crail.git) or<br/>
   (b) Clone from [Github](https://github.com/apache/incubator-crail) or<br/>
   (c) Download and unpack the latest source release from [here](https://dist.apache.org/repos/dist/release/incubator/crail/)
2. Run: mvn -DskipTests install
3. Copy tarball to the cluster and unpack it using tar xvfz crail-1.0-bin.tar.gz

Note: later, when deploying Crail, make sure libdisni.so is part of your LD_LIBRARY_PATH. The easiest way to make it work is to copy libdisni.so into crail-1.0/lib 

## Configuration

To configure Crail use crail-site.conf.template as a basis and modify it to match your environment. 

    cd crail-1.0/conf
    mv crail-site.conf.template crail-site.conf
  
There are a general file system properties and specific properties for the different storage tiers. A typical configuration for the general file system section may look as follows:

    crail.namenode.address                crail://namenode:9060
    crail.storage.types                   org.apache.crail.storage.rdma.RdmaStorageTier
    crail.cachepath                       /dev/hugepages/cache
    crail.cachelimit                      12884901888
    crail.blocksize                       1048576
    crail.buffersize                      1048576

In this configuration the namenode is configured to run using port 9060 on host 'namenode', which must be a valid host in the cluster. We further configure a single storage tier, in this case the RDMA-based DRAM tier. The cachepath property needs to point to a directory that is used by the file system to allocate memory for the client cache. Up to cachelimit size, all the memory that is used by Crail will be allocated via mmap from this location. Ideally, the directory specified in cachepath points to a hugetlbfs mountpoint. Aside from the general properties, each storage tier needs to be configured separately.

### RDMA/DRAM Storage

For the RDMA/DRAM tier we need to specify the interface that should be used by the storage nodes.

    crail.storage.rdma.interface         eth0
  
The datapath property specifies a path from which the storage nodes will allocate blocks of memory via mmap. Again, that path best points to a hugetlbfs mountpoint.

    crail.storage.rdma.datapath          /memory/data

You want to specify how much DRAM each datanode should donate into the file system pool using the `storagelimit` property. DRAM is allocated in chunks of `allocationsize`, which needs to be a multiple of `crail.blocksize`.

    crail.storage.rdma.allocationsize    1073741824
    crail.storage.rdma.storagelimit      75161927680

Crail supports optimized local operations via memcpy (instead of RDMA) in case a given file operation is backed by a local storage node. The indexpath specifies where Crail will store the necessary metadata that make these optimizations possible. Important: the indexpath must NOT point to a hugetlbfs mountpoint because index files will be updated which not possible in hugetlbfs.

    crail.storage.rdma.localmap          true
    crail.storage.rdma.indexpath         /index
    
### NVMf/Flash Storage    

Crail is a multi-tiered storage system. Additional tiers can be enabled by adding them to the configuration as follows.

    crail.storage.types                  org.apache.crail.storage.rdma.RdmaStorageTier,org.apache.crail.storage.nvmf.NvmfStorageTier

For the NVMf storage tier we need to configure the target's IP, port and subsystem NVMe qualified name.  

    crail.storage.nvmf.ip               10.40.0.XX
    crail.storage.nvmf.port             50025
    crail.storage.nvmf.nqn              nqn.2017-06.io.crail:cnode

## Deploying

For all deployments, make sure you define CRAIL_HOME on each machine to point to the top level Crail directory.

### Starting Crail manually

The simplest way to run Crail is to start it manually on just a handful nodes. You will need to start the Crail namenode, plus at least one datanode. To start the namenode execute the following command on the host that is configured to be the namenode:

    cd crail-1.0/
    ./bin/crail namenode

To start a datanode run the following command on a host in the cluster (ideally this is a different physical machine than the one running the namenode):

    ./bin/crail datanode

Now you should have a small deployment up with just one datanode. In this case the datanode is of type RDMA/DRAM, which is the default datnode. If you want to start a different storage tier you can do so by passing a specific datanode class as follows:

    ./bin/crail datanode -t org.apache.crail.storage.nvmf.NvmfStorageTier

This would start the shared storage datanode. Note that configuration in crail-site.conf needs to have the specific properties set of this type of datanode, in order for this to work. 

### Larger deployments

To run larger deployments start Crail using 

    ./bin/start-crail.sh

Similarly, Crail can be stopped by using 

    ./bin/stop-crail.sh

For this to work include the list of machines to start datanodes in conf/slaves. You can start multiple datanode of different types on the same host as follows:

    host02-ib
    host02-ib -t org.apache.crail.storage.nvmf.NvmfStorageTier
    host03-ib

In this example, we are configuring a Crail cluster with 2 physical hosts but 3 datanodes and two different storage tiers.

## Crail Shell

Crail provides an contains an HDFS adaptor, thus, you can interact with Crail using the HDFS shell:

    ./bin/crail fs

Crail, however, does not implement the full HDFS shell functionality. The basic commands to copy file to/from Crail, or to move and delete files, will work.

    ./bin/crail fs -mkdir /test
    ./bin/crail fs -ls /
    ./bin/crail fs -copyFromLocal <path-to-local-file> /test
    ./bin/crail fs -cat /test/<file-name>

For the Crail shell to work properly, the HDFS configuration in crail-1.0/conf/core-site.xml needs to be configured accordingly:

    <configuration>
      <property>
       <name>fs.crail.impl</name>
       <value>org.apache.crail.hdfs.CrailHadoopFileSystem</value>
      </property>
      <property>
        <name>fs.defaultFS</name>
        <value>crail://namenode:9060</value>
      </property>
      <property>
        <name>fs.AbstractFileSystem.crail.impl</name>
        <value>org.apache.crail.hdfs.CrailHDFS</value>
      </property>
     </configuration>

Note that the Crail HDFS interface currently cannot provide the full performance of Crail due to limitations of the HDFS API. In particular, the HDFS `FSDataOutputStream` API only support heap-based `byte[]` arrays which requires a data copy. Moreover, HDFS operations are synchronous preventing efficient pipelining of operations. Instead, applications that seek the best performance should use the Crail interface directly, as shown next.

## Programming against Crail

The best way to program against Crail is to use Maven. Make sure you have the Crail dependency specified in your application pom.xml file:

    <dependency>
      <groupId>org.apache.crail</groupId>
      <artifactId>crail-client</artifactId>
      <version>1.0</version>
    </dependency>

Then, create a Crail client as follows:

    CrailConfiguration conf = new CrailConfiguration();
    CrailStore store = CrailStore.newInstance(conf);

Make sure the crail-1.0/conf directory is part of the classpath. 

Crail supports different file types. The simplest way to create a file in Crail is as follows:

    CrailFile file = store.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT).get().syncDir();

Aside from the actual filename, the 'create()' call takes as input the storage and location classes which are preferences for the storage tier and physical location that this file should be created in. Crail tries to satisfy these preferences later when the file is written. In the example we do not request any particular storage or location affinity.

This 'create()' command is non-blocking, calling 'get()' on the returning future object awaits the completion of the call. At that time, the file has been created, but its directory entry may not be visible. Therefore, the file may not yet show up in a file enumeration of the given parent directory. Calling 'syncDir()' waits to for the directory entry to be completed. Both the 'get()' and the 'syncDir()' operation can be deffered to a later time at which they may become non-blocking operations. 

Once the file is created, a file stream can be obtained for writing:

    CrailBufferedOutputStream outstream = file.getBufferedOutputStream(1024);	

Here, we create a buffered stream so that we can pass heap byte arrays as well. We could also create a non-buffered stream using

    CrailOutputStream outstream = file.getDirectOutputStream(1024);

In both cases, we pass a write hint (1024 in the example) that indicates to Crail how much data we are intending to write. This allows Crail to optimize metadatanode lookups. Crail never prefetches data, but it may fetch the metadata of the very next operation concurrently with the current data operation if the write hint allows to do so. 

Once the stream has been obtained, there exist various ways to write a file. The code snippet below shows the use of the asynchronous interface:

    CrailBuffer dataBuf = fs.allocateBuffer();
    Future<DataResult> future = outputStream.write(dataBuf);
    ...
    future.get();

Reading files works very similar to writing. There exist various examples in org.apache.crail.tools.CrailBenchmark.

## TCP Storage Tiers and RPC binding

Crail is designed for user-level networking and storage. It does, however, also provide plain TCP-based storage backends for storage and RPC and, thus, can be run easily on any machine without requiring spspecial hardware support. The TCP storage backend can be enabled as follows:

    crail.storage.types		org.apache.crail.storage.tcp.TcpStorageTier

The TCP RPC binding can be enabled as follows:

    crail.namenode.rpctype	org.apache.crail.namenode.rpc.tcp.TcpNameNode

## Benchmarks

Crail provides a set of benchmark tools to measure the performance. Type

    ./bin/crail iobench

to get an overview of the available benchmarks. For instance, to benchmark the sequential write performance, type

    ./bin/crail iobench -t write -s 1048576 -k 102400 -f /tmp.dat

This will create a file of size 100G, written sequentially in a sequence of 1MB operations. 

To read a file sequentially, type

    ./bin/crail iobench -t read -s 1048576 -k 102400 -f /tmp.dat

This command issues 102400 read operations of 1MB each.

The tool also contains benchmarks to read files randomly, or to measure the performance of opening files, etc.

## Applications

Crail is used by [Crail-Spark-IO](https://github.com/zrlio/crail-spark-io), a high-performance shuffle engine for Spark. [Crail-Terasort](https://github.com/zrlio/crail-terasort) is a fast sorting benchmark for Spark based on Crail. 

## Contributions

For any potential changes/proposals we recommend that you open a JIRA ticket to have a disucssion. After making necessary code changes, please open a pull request at Github, and update the JIRA. See here for more resources: http://crail.incubator.apache.org/community/

## Contact 

Please join the Crail developer mailing list for discussions and notifications. The list is at: 

dev@crail.incubator.apache.org.
