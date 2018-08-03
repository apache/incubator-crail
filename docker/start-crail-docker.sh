#!/bin/bash

# substitude env variables in core-site.xml
envsubst < $CRAIL_HOME/conf/core-site.xml > $CRAIL_HOME/conf/core-site.xml

crail $@
