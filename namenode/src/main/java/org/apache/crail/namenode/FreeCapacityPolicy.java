package org.apache.crail.namenode;

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.rpc.RpcNameNodeService;

public class FreeCapacityPolicy extends PolicyRunner {

    double scaleUp;
    double scaleDown;
    int minDataNodes;
    int maxDataNodes;
    int datanodes; // maintains the desired number of datanodes (i.e. a datanode might be still starting / terminating)
    boolean updated; // shows whether a launch/terminate datanode operation returned
    long lastCapacity; // maintains the capacity that was available when the launch/terminate datanode operation was issued

    FreeCapacityPolicy(RpcNameNodeService service, double scaleUp, double scaleDown, int minDataNodes, int maxDataNodes) {
        super(service);
        this.scaleUp = scaleUp;
        this.scaleDown = scaleDown;
        this.minDataNodes = minDataNodes;
        this.maxDataNodes = maxDataNodes;
        this.datanodes = 0;
        this.updated = true;
        this.lastCapacity = 0;
    }

    @Override
    public void checkPolicy() {

        try {

            // log current usage information
            double usage = this.service.getStorageUsedPercentage();

            if (CrailConstants.ELASTICSTORE_LOGGING) {
                LOG.info("Current block usage: " + this.service.getNumberOfBlocksUsed() + "/" + this.service.getNumberOfBlocks());
                LOG.info("Current storage usage: " + 100*usage + "%");
                LOG.info("Current number of datanodes: " + this.datanodes);
            }

            // check whether datanode launch/terminate operation finished
            if (!this.updated && this.lastCapacity != this.service.getNumberOfBlocks()) {
                this.updated = true;
            }

            // check whether scaling up or down is possible
            if (this.updated) {
                if (usage < scaleDown && this.datanodes > minDataNodes) {
                    LOG.info("Scale down detected");
    
                    DataNodeBlocks removeCandidate = this.service.identifyRemoveCandidate();
    
                    if (removeCandidate != null) {
                        this.lastCapacity = this.service.getNumberOfBlocks();
                        this.updated = false;
                        this.service.prepareDataNodeForRemoval(removeCandidate);
                        this.datanodes--;
                    }
                }
    
                if (usage > this.scaleUp && this.datanodes < maxDataNodes) {
                    LOG.info("Scale up detected");
                    this.lastCapacity = this.service.getNumberOfBlocks();
                    this.updated = false;
                    launchDatanode();
                    this.datanodes++;
                }
            }


        } catch (Exception e) {
            LOG.error("Unable to retrieve storage usage information");
        }
    }
}