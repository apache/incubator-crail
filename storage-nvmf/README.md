# Crail Storage Tier for NVMe over Fabrics



## Building

Clone and build [`disni`](https://www.github.com/zrlio/disni) dependency with `--with-spdk`.
Clone and build the project using:

```bash
mvn -DskipTests install
```
Then copy the jar files crail-nvmf-1.0.jar and its dependencies from the
`target`folder into`$CRAIL_HOME/jars/`.

Alternatively you can also put these files in your custom classpath.

## Configuration parameters
The current code accepts following parameters (shown with their defaut values):
```
crail.datanode.nvmf.bindip       X.X.X.X
crail.datanode.nvmf.port          50025
crail.datanode.nvmf.pcieaddr      XXXX:XX:XX.X
crail.datanode.nvmf.namespace     1
crail.datanode.nvmf.allocationsize 1073741824
```

You can put these values in `$CRAIL_HOME/conf/crail-site.conf`.

## Starting a crail-nvmf datanode 
To start a crail-nvmf datanode, start a datanodes as 
```bash 
$CRAIL_HOME/bin/crail datanode -t com.ibm.crail.datanode.nvmf.NvmfDataNode
```
in order for a client to automatically pick up connection to a new datanode 
type, you have to add following class to your list of datanode types in the
`$CRAIL_HOME/conf/crail-site.conf` file. An example of such entry is :

```bash
crail.datanode.types  com.ibm.crail.datanode.rdma.RdmaDataNode,com.ibm.crail.datanode.nvmf.NvmfDataNode
```

Please note that, this is a comma separated list of datanode **types** which 
defines the priorty order as well in which the blocks from a datanode will 
be consumed by the namenode. 

## Setting up automatic deployment

To enable deployment via `$CRAIL_HOME/bin/start-crail.sh` use the following extension 
in the crail slave file (`$CRAIL_HOME/conf/slave`.): 

```bash
hostname1 -t com.ibm.crail.datanode.nvmf.NvmfDataNode
...
```

## Contributions

PRs are always welcome. Please fork, and make necessary modifications you propose, and let us know.

## Contact

If you have questions or suggestions, feel free to post at:

https://groups.google.com/forum/#!forum/zrlio-users

or email: zrlio-users@googlegroups.com  
