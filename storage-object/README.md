# Crail on shared volume block device

Crail-objectstore is an extension of the Crail project to enable
storing data in an object storage tier.

## Building

Clone and build [`crail-objectstore`]() using:

```bash
mvn -DskipTests install
```

Then copy the jar files crail-storage-objectstore-1.0.jar and its dependencies 
from the `target` folder into `$CRAIL_HOME/jars/`. Make sure that there are
no older jar files with differnt versions due to hadoop, crail, or any other
precompiled projects or dependencies.

Alternatively you can also put these files in your custom classpath.

## Configuration parameters
The parameters accepted by the code can be found in ObjectStoreDataNode.java 
(together with their defaut values). You can put these values in 
`$CRAIL_HOME/conf/crail-site.conf`. 

## Starting a crail-objectstore datanode 
To start a crail-objectstore datanode, start a datanode as 
```bash 
$CRAIL_HOME/bin/crail datanode -t org.apache.crail.datanode.objectstore.ObjectStoreDataNode
```
In order for a client to automatically connect to a new ObjectStore datanode
type, you have to add the following class to your list of datanode types in the
`$CRAIL_HOME/conf/crail-site.conf` file. An example of such entry is :

```bash
crail.datanode.types  org.apache.crail.datanode.objectstore.ObjectStoreDataNode
```

Please note that, this is a comma separated list of datanode **types** which 
defines the priority order in which the blocks the datanodes will 
be consumed. 

## Setting up automatic deployment

To enable deployment via `$CRAIL_HOME/bin/start-crail.sh` use the following extension 
in the crail slave file (`$CRAIL_HOME/conf/slave`.): 

```bash
hostname1 -t org.apache.crail.datanode.objectstore.ObjectStoreDataNode
...
```
Note: A crail-objectstore datanode does not serve data requests from clients, but
only registers the block device storage information to the namenode and translates
block IDs to object IDs. Clients access directly the object store through one of 
the configured endpoints and no data traffic traverses the datanode.

## Persistency and concurency considerations

TODO:

## Contributions

PRs are always welcome. Please fork, and make necessary modifications you propose, and let us know.

## Contact

If you have questions or suggestions, feel free to post at:

https://groups.google.com/forum/#!forum/zrlio-users

or email: zrlio-users@googlegroups.com  
