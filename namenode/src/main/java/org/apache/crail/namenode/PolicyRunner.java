package org.apache.crail.namenode;

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.rpc.RpcNameNodeService;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public abstract class PolicyRunner implements Runnable {

    static final Logger LOG = CrailUtils.getLogger();
    NameNodeService service;
    int instances = 0;

    PolicyRunner(RpcNameNodeService service){
        this.service = (NameNodeService) service;
        Thread runner = new Thread(this);
        runner.start();
    }

    public abstract void checkPolicy();

    public void run() {

        while (true) {
            checkPolicy();

            try {
                Thread.sleep(CrailConstants.ELASTICSTORE_POLICYRUNNER_INTERVAL);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

    }

    public void launchDatanode() {

        try {
            String port = Integer.toString(50020+this.instances);
            Process p = new ProcessBuilder(System.getenv("CRAIL_HOME") + "/bin/crail", "datanode", "--",  "-p" + port).start();

            LOG.info("Launched new datanode instance");
            this.instances++;

        } catch(Exception e) {
            LOG.error("Unable to launch datanode");
            e.printStackTrace();
        }

    }
}
