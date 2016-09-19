#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

bin=`dirname "${BASH_SOURCE-$0}"`
CRAIL_HOME=`cd "$bin/.."; pwd`
bin=`cd "$bin"; pwd`

LIBEXEC_DIR="$bin"/../libexec

#---------------------------------------------------------
# namenodes

NAMENODES=$($CRAIL_HOME/bin/crail getconf namenode)

echo "Stopping namenodes on [$NAMENODES]"

"$LIBEXEC_DIR/crail-daemons.sh" \
  --hostnames "$NAMENODES" \
  --script "$bin/crail" stop namenode

#---------------------------------------------------------
# datanodes (using default slaves file)

if [ -n "$HADOOP_SECURE_DN_USER" ]; then
  echo \
    "Attempting to stop secure cluster, skipping datanodes. " \
    "Run stop-secure-dns.sh as root to complete shutdown."
else
  "$LIBEXEC_DIR/crail-daemons.sh" \
    --script "$bin/crail" stop datanode
fi

exit

# eof
