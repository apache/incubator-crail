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

function trim() {
	s="$@"
	echo ${s//[[:blank:]]/}
}

usage="Usage: crail-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] [--script script] (start|stop) <hadoop-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

LIBEXEC_DIR="$bin"/../libexec

CONF_PATH="$bin"/../conf
if [ -f "${CONF_PATH}/crail-env.sh" ]; then
  # Promote all variable declarations to environment (exported) variables
  set -a
  . "${CONF_PATH}/crail-env.sh"
  set +a
fi

# get arguments

#default value
hadoopScript="$HADOOP_PREFIX"/bin/hadoop
if [ "--script" = "$1" ]
  then
    shift
    hadoopScript=$1
    shift
fi
startStop=$1
shift
command=$1
shift

hadoop_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
  . "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi

# Determine if we're starting a secure datanode, and if so, redefine appropriate variables
if [ "$command" == "datanode" ] && [ "$EUID" -eq 0 ] && [ -n "$HADOOP_SECURE_DN_USER" ]; then
  export HADOOP_PID_DIR=$HADOOP_SECURE_DN_PID_DIR
  export HADOOP_LOG_DIR=$HADOOP_SECURE_DN_LOG_DIR
  export HADOOP_IDENT_STRING=$HADOOP_SECURE_DN_USER
  starting_secure_dn="true"
fi

if [ "$HADOOP_IDENT_STRING" = "" ]; then
  export HADOOP_IDENT_STRING="$USER"
fi


# get log directory
if [ "$HADOOP_LOG_DIR" = "" ]; then
  export HADOOP_LOG_DIR="$CRAIL_HOME/logs"
fi

if [ ! -w "$HADOOP_LOG_DIR" ] ; then
  mkdir -p "$HADOOP_LOG_DIR"
  chown $HADOOP_IDENT_STRING $HADOOP_LOG_DIR
fi

if [ "$HADOOP_PID_DIR" = "" ]; then
  HADOOP_PID_DIR=/tmp
fi

ARGS=$(trim "$@")
# some variables
export HADOOP_LOGFILE=crail-$HADOOP_IDENT_STRING-$command-$ARGS-$HOSTNAME.log
export HADOOP_ROOT_LOGGER=${HADOOP_ROOT_LOGGER:-"INFO,RFA"}
export HADOOP_SECURITY_LOGGER=${HADOOP_SECURITY_LOGGER:-"INFO,RFAS"}
export HDFS_AUDIT_LOGGER=${HDFS_AUDIT_LOGGER:-"INFO,NullAppender"}
log=$HADOOP_LOG_DIR/crail-$HADOOP_IDENT_STRING-$command-$ARGS-$HOSTNAME.out
pid=$HADOOP_PID_DIR/crail-$HADOOP_IDENT_STRING-$command-$ARGS.pid
HADOOP_STOP_TIMEOUT=${HADOOP_STOP_TIMEOUT:-5}

# Set default scheduling priority
if [ "$HADOOP_NICENESS" = "" ]; then
    export HADOOP_NICENESS=0
fi

case $startStop in

  (start)
    
    [ -w "$HADOOP_PID_DIR" ] ||  mkdir -p "$HADOOP_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    if [ "$HADOOP_MASTER" != "" ]; then
      echo rsync from $HADOOP_MASTER
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' $HADOOP_MASTER/ "$HADOOP_PREFIX"
    fi

    hadoop_rotate_log $log
    echo starting $command, logging to $log
    cd "$bin/../"
    current=`pwd`
    case $command in
      namenode|secondarynamenode|datanode|journalnode|dfs|dfsadmin|fsck|balancer|zkfc)
        if [ -z "$CRAIL_HOME" ]; then
          hdfsScript="$current"/bin/crail
        else
          hdfsScript="$current"/bin/crail
        fi
        nohup nice -n $HADOOP_NICENESS $hdfsScript $command "$@" > "$log" 2>&1 < /dev/null &
      ;;
      (*)
        nohup nice -n $HADOOP_NICENESS $hadoopScript $command "$@" > "$log" 2>&1 < /dev/null &
      ;;
    esac
    echo $! > $pid
    sleep 1
    head "$log"
    # capture the ulimit output
    if [ "true" = "$starting_secure_dn" ]; then
      echo "ulimit -a for secure datanode user $HADOOP_SECURE_DN_USER" >> $log
      # capture the ulimit info for the appropriate user
      su --shell=/bin/bash $HADOOP_SECURE_DN_USER -c 'ulimit -a' >> $log 2>&1
    else
      echo "ulimit -a for user $USER" >> $log
      ulimit -a >> $log 2>&1
    fi
    sleep 3;
    if ! ps -p $! > /dev/null ; then
      exit 1
    fi
    ;;
          
  (stop)

    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping $command
        kill $TARGET_PID
        sleep $HADOOP_STOP_TIMEOUT
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "$command did not stop gracefully after $HADOOP_STOP_TIMEOUT seconds: killing with kill -9"
          kill -9 $TARGET_PID
        fi
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


