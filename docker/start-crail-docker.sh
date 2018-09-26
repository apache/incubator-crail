#!/bin/bash

# substitude env variables in core-site.xml
envsubst < $CRAIL_HOME/conf/core-site.xml.env > $CRAIL_HOME/conf/core-site.xml

crail $@
