# Crail

Crail is a fast multi-tiered distributed file system for temporary data, designed from ground up for high-performance network and storage hardware. The unique features of Crail include:

* Zero-copy network access from userspace using RDMA 
* Integration of multiple storage tiers such DRAM, flash and disaggregated shared storage
* Ultra-low latencies for both meta data and data operations. For instance: opening, reading and closing a small file residing in the distributed DRAM tier takes 10-15 microseconds, which is in the same ballpark as some of the fastest RDMA-based key/value stores
* High-performance sequential read/write operations: For instance: read operations on large files residing in the distributed DRAM tier are typically limited only by the performance of the network
* Very low CPU consumption: a single core sharing both application and file system client can drive sequential read/write operations at the speed of up to 100Gbps and more
* Asynchronous file system API leveraging the asynchronous nature of RDMA-based networking hardware
* Etensible plugin architecture: new storage tiers tailored to specific hardware can be added easily
 
Crail is implemented in Java offering a Java API which integrates directly with the Java off-heap memory. Crail is designed for performance critical temporary data within a scope of a rack or two. It currently does not provide fault tolerance 

## Requirements

* Java 8 or higher
* RDMA-based network, e.g., Infiniband, iWARP, RoCE. There are two options to run Crail without RDMA networking hardware: (a) use SoftiWARP, (b) us the TCP/DRAM storage tier

## Building 

Building the source requires [Apache Maven](http://maven.apache.org/) and Java version 8 or higher.
To build Crail execute the following steps:

1. Obtain a copy of [Crail](https://github.com/zrlio/crail) from Github
2. Make sure your local maven repo contains [DiSNI](https://github.com/zrlio/disni), if not build DiSNI from Github
   (Later, when deploying Crail, make sure libdisni.so is part of your LD_LIBRARY_PATH. The easiest way to make it work is to copy libdisni.so into the crail-1.0/lib directory)
3. Make sure your local maven repo contains [DaRPC](https://github.com/zrlio/darpc), if not build DaRPC from Github
4. Run: mvn -DskipTests install
5. Copy tarball to the cluster and unpack it using tar xvfz crail-1.0-bin.tar.gz

## Configuration

To configure Crail use crail-site.conf.template as a basis and modify it to match your environment. 

    cd crail-1.0/conf
    mv crail-site.conf.template crail-site.conf
  
There are a general file system properties and specific properties for the different storage tiers. A typical configuration for the general file system section may look as follows:

    crail.namenode.address                crail://namenode:9060
    crail.datanode.types                  com.ibm.crail.datanode.rdma.RdmaDataNode
    crail.cachepath                       /memory/cache
    crail.cachelimit                      12884901888

In this configuration the namenode is configured to run using port 9060 on host 'namenode', which must be a valid host in the cluster. We further configure a single storage tier, in this case the RDMA-based DRAM tier. Cachepath points to a directory that is used by the file system to allocate memory for the client cache. Up to cachelimit size, all the memory that is used by Crail will be allocated via mmap from this location. Ideally, the directory specified in cachepath points to a hugetlbfs mountpoint. 

Each storage tier will have its own separate set of parameters. For the RDMA/DRAM tier we need to specify the interface that should be used by the storage nodes.

    crail.datanode.rdma.interface         eth0
  
The datapath property specifies a path from which the storage nodes will allocate blocks of memory via mmap. Again, that path best points to a hugetlbfs mountpoint.

    crail.datanode.rdma.datapath          /memory/data
  
Crail supports optimized local operations via memcpy (instead of RDMA) in case a given file operation is backed by a local storage node. The indexpath specifies where Crail will store the necessary metadata that make these optimizations possible. Important: the indexpath must NOT point to a hugetlbfs mountpoint because index files will be updated which not possible in hugetlbfs.

    crail.datanode.rdma.localmap          true
    crail.datanode.rdma.indexpath         /index

## Deploying


### Starting Crail manually

The simplest way to run Crail is to start it manually on just a handful nodes. You will need to start the Crail namenode, plus at least one datanode. To start the namenode execute the following command on the host that is configured to be the namenode:

    cd crail-1.0/
    ./bin/crail namenode

To start a datanode run the following command on a host in the cluster (ideally this is a different physical machine than the one running the namenode):

    ./bin/crail datanode

Now you should have a small deployment up with just one datanode. In this case the datanode is of type RDMA/DRAM, which is the default datnode. If you want to start a different storage tier you can do so by passing a specific datanode class as follows:

    ./bin/crail datanode -t com.ibm.crail.datanode.blkdev.BlkDevDataNode

This would start the shared storage datanode. Note that configuration in crail-site.conf needs to have the specific properties set of this type of datanode, in order for this to work. Also, in order for the storage tier to become visible to clients, it has to be enlisted in the list of datanode types as follows:

    crail.datanode.types                  com.ibm.crail.datanode.rdma.RdmaDataNode,com.ibm.crail.datanode.blkdev.BlkDevDataNode

### Larger deployments

For larger deployments you want to start Crail using 

    ./bin/start-crail.sh

Similarly, Crail can be stopped by using 

    ./bin/stop-crail.sh

For this to work include the list of machines to start datanodes in conf/slaves. You can start multiple datanode of different types on the same host as follows:

    host02-ib
    host02-ib -t com.ibm.crail.datanode.blkdev.BlkDevDataNode
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
       <value>com.ibm.crail.hdfs.CrailHadoopFileSystem</value>
      </property>
      <property>
        <name>fs.defaultFS</name>
        <value>crail://namenode:9060</value>
      </property>
      <property>
        <name>fs.AbstractFileSystem.crail.impl</name>
        <value>com.ibm.crail.hdfs.CrailHDFS</value>
      </property>
     </configuration>

## Programming against Crail

The best way to program against Crail is to use Maven. Make sure you have the Crail dependency specified in your application pom.xml file:

    <dependency>
      <groupId>com.ibm.crail</groupId>
      <artifactId>crail-client</artifactId>
      <version>1.0</version>
    </dependency>

Then, create a Crail file system instance as follows:

    CrailConfiguration conf = new CrailConfiguration();
    CrailFS fs = CrailFS.newInstance(conf);

Make sure the crail-1.0/conf directory is part of the classpath. 

The simplest way to create a file in Crail is as follows:

    CrailFile file = fs.createFile(filename, 0, 0).get().syncDir();

Aside from the actual filename, the 'createFile()' takes as input to the storage and location affinity which are preferences about the storage tier and physical location that this file should created in. Crail tries to satisfy these preferences later when the file is written. In the example we do not request any particular storage or location affinity.

This 'createFile()' command is non-blocking, calling 'get()' on the returning future object awaits the completion of the call. At that time, the file has been created, but its directory entry may not be visible. Therefore, the file may not yet show up in a file enumeration of the given parent directory. Calling 'syncDir()' waits to for the directory entry to be completed. Both the 'get()' and the 'syncDir()' can be deffered to a later time at which they may become non-blocking operations as well. 

Once the file is created, a file stream can be obtained for writing:

    CrailBufferedOutputStream outstream = file.getBufferedOutputStream(1024);	

Here, we create a buffered stream so that we can pass heap byte arrays as well. We could also create a non-buffered stream using

    CrailOutputStream outstream = getDirectOutputStream(1024);

In both cases, we pass a write hint (1024 in the example) that indicates to Crail how much data we are intending to write. This allows Crail to optimize metadatanode lookups. Crail never prefetches data, but it may fetch the metadata of the very next operation concurrently with the current data operation if the write hint allows to do so. 

Once the stream has been obtained, there exist various ways to write a file. The code snippet below shows the use of the asynchronous interface:

    ByteBuffer dataBuf = fs.allocateBuffer();
    Future<DataResult> future = outputStream.writeAsync(dataBuf);
    ...
    future.get();

Reading files works very similar to writing. There exist various examples in com.ibm.crail.tools.CrailBenchmark.

## Storage Tiers

Crail ships with the RDMA/DRAM storage tier. Currently there are two additional storage tiers available in separate repos:

* [Crail-Blkdev](https://github.com/zrlio/crail-blkdev)  is a storage tier integrating shared volume block devices such as disaggregated flash. 
* [Crail-Netty](https://github.com/zrlio/crail-netty) is a DRAM storage tier for Crail that uses TCP, you can use it to run Crail on non-RDMA hardware. Follow the instructions in these repos to build, deploy and use these storage tiers in your Crail environmnet. 

## Applications

Crail is used by [Spark-IO](https://github.com/zrlio/spark-io), a high-performance shuffle engine for Spark. [Crail-Terasort](https://github.com/zrlio/crail-terasort) is a fast sorting benchmark for Spark based on Crail. 

## Contact

If you have questions feel free to post at:

https://groups.google.com/forum/#!forum/zrlio-users
