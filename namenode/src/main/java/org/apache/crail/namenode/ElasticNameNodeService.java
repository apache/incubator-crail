package org.apache.crail.namenode;

import java.io.IOException;

import org.apache.crail.conf.CrailConstants;

public class ElasticNameNodeService extends NameNodeService {

    PolicyRunner policyRunner;

    public ElasticNameNodeService() throws IOException {

        this.policyRunner = new FreeCapacityPolicy(this,
                CrailConstants.ELASTICSTORE_SCALEUP,
                CrailConstants.ELASTICSTORE_SCALEDOWN,
                CrailConstants.ELASTICSTORE_MINNODES,
                CrailConstants.ELASTICSTORE_MAXNODES);
    }
}
