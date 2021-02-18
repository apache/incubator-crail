package org.apache.crail.tools;

import org.apache.commons.cli.*;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.rpc.*;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

public class RemoveDataNode {

    public static void main(String[] args) throws Exception {

        InetAddress ipaddr = null;
        int port = -1;

        Option ipOption = Option.builder("i").desc("Ip address").hasArg().required().build();
        Option portOption = Option.builder("p").desc("port").hasArg().required().build();

        Options options = new Options();
        options.addOption(ipOption);
        options.addOption(portOption);

        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args);

        try {
            ipaddr = InetAddress.getByName(line.getOptionValue(ipOption.getOpt()));
            port = Integer.parseInt(line.getOptionValue(portOption.getOpt()));
        } catch(Exception e) {
            formatter.printHelp("RemoveDataNode", options);
            System.exit(-1);
        }

        Logger LOG = CrailUtils.getLogger();
        CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
        CrailConstants.updateConstants(conf);
        CrailConstants.printConf();
        CrailConstants.verify();
        RpcClient rpcClient = RpcClient.createInstance(CrailConstants.NAMENODE_RPC_TYPE);

        rpcClient.init(conf, null);
        rpcClient.printConf(LOG);

        ConcurrentLinkedQueue<InetSocketAddress> namenodeList = CrailUtils.getNameNodeList();
        ConcurrentLinkedQueue<RpcConnection> connectionList = new ConcurrentLinkedQueue<RpcConnection>();
        while(!namenodeList.isEmpty()){
            InetSocketAddress address = namenodeList.poll();
            RpcConnection connection = rpcClient.connect(address);
            connectionList.add(connection);
        }
        RpcConnection rpcConnection = connectionList.peek();
        if (connectionList.size() > 1){
            rpcConnection = new RpcDispatcher(connectionList);
        }
        LOG.info("connected to namenode(s) " + rpcConnection.toString());

        LOG.info("Trying to remove datanode at " + ipaddr.getHostName() + ":" + port);

        Future<RpcRemoveDataNode> res = rpcConnection.removeDataNode(ipaddr, port);
        short response = res.get().getRpcStatus();

        rpcConnection.close();

        if(response == RpcErrors.ERR_DATANODE_NOT_REGISTERED) {
            LOG.info("Datanode running at " + ipaddr.getHostAddress() + ":" + port + " is not registered at NameNode");
        } else if(response == RpcErrors.ERR_OK) {
            LOG.info("Datanode running at " + ipaddr.getHostAddress() + ":" + port + " was scheduled for removal");
        } else {
            throw new Exception("Unexpected error code in RPC response");
        }


    }
}
