<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
## [1.2](https://github.com/apache/incubator-crail/tree/v1.2) - 26.11.2019

#### New features / improvements

* [[CRAIL-106](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-106)] Allow namenode as parameter instead of conf file.
* [[CRAIL-100](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-100)] Fix DiSNI version in Dockerfile
* [[CRAIL-96](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-96)]   Make host NQN configurable
* [[CRAIL-90](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-90)]   Release documentation clarifications
* [[CRAIL-83](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-83)]   Include README in binary tarball



#### Bug fixes

* [[CRAIL-107](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-107)] Fix RPC type for TCP and start/stop script paths in documentation
* [[CRAIL-104](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-104)] StorageServer client RDMA completion queue persists unused, if client disconnects
* [[CRAIL-103](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-103)] StorageServer client enpoint does not get destroyed if client disconnects.
* [[CRAIL-99](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-99)]   Docker file uses wrong naming and fails building image
* [[CRAIL-98](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-98)]   Make keepalive thread a daemon thread
* [[CRAIL-93](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-93)]   Do not append port to subsystem NQN



## [1.1](https://github.com/apache/incubator-crail/tree/v1.1) - 26.11.2018

#### New features / improvements

* [[CRAIL-5](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-5)] How to I contribute to Crail?
* [[CRAIL-15](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-15)] Better default configuration
* [[CRAIL-19](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-19)] Add checkstyle plugin
* [[CRAIL-20](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-20)] Statistics for storage and rpc client
* [[CRAIL-24](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-24)] New documentation using readthedocs
* [[CRAIL-27](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-27)] HDFS adapter implementation improvement
* [[CRAIL-34](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-34)] Use Apache parent pom
* [[CRAIL-36](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-36)] Add incubation disclaimer to README
* [[CRAIL-39](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-39)] Add download page to website
* [[CRAIL-41](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-41)] Use DiSNI/DaRPC v1.6
* [[CRAIL-49](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-49)] Add Docker container
* [[CRAIL-50](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-50)] Use DiSNI/DaRPC v1.7
* [[CRAIL-53](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-53)] Add missing license headers and exclude configuration files from rat check
* [[CRAIL-54](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-54)] Tar source files / scm tag is now HEAD
* [[CRAIL-55](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-55)] Add programming documentation
* [[CRAIL-61](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-61)] Add how to release documentation
* [[CRAIL-74](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-74)] Top-level directory for binary tarball
* [[CRAIL-77](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-77)] Include LICENSE, NOTICE and DISCLAIMER in meta-inf
* [[CRAIL-79](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-79)] Update DiSNI and DaRPC
* [[CRAIL-80](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-80)] Move all dependencies to parent pom for version management
* [[CRAIL-81](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-81)] Add licenses of all dependencies



#### Bug fixes

* [[CRAIL-26](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-26)] HDFS statistics
* [[CRAIL-29](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-29)] Fix starting multiple datanodes on same host
* [[CRAIL-40](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-40)] Fix startup script memory requirement
* [[CRAIL-45](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-45)] Fix Nullpointer exception when datapath is not accessible
* [[CRAIL-46](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-46)] Clarify incubating status on website
* [[CRAIL-51](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-51)] Fix subsitution of core-site.xml in Docker container
* [[CRAIL-52](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-52)] Fix CRAIL_HOME in Docker container
* [[CRAIL-58](https://jira.apache.org/jira/projects/crail/issues/crail-58)] fix rdma docker build
* [[CRAIL-87](https://jira.apache.org/jira/projects/crail/issues/crail-87)] do not include category X licensed artifacts in binary release
* [[CRAIL-89](https://jira.apache.org/jira/projects/CRAIL/issues/CRAIL-89)] Fixed minor compilation problem

## [1.0](https://github.com/apache/incubator-crail/tree/v1.0) - 23.05.2018

This is our first Apache incubator release. Below are new features and bug fixes since the import to Apache.

#### New features / improvements

* [[CRAIL-22](https://issues.apache.org/jira/projects/CRAIL/issues/CRAIL-22)] New NVMf storage tier that does not depend on SPDK
* [[CRAIL-17](https://issues.apache.org/jira/projects/CRAIL/issues/CRAIL-17)] CrailBufferedStream: align access on underlying CoreStream
* [[CRAIL-11](https://issues.apache.org/jira/projects/CRAIL/issues/CRAIL-11)] Enable environment variable expansion in crail-site.conf
* [[CRAIL-10](https://issues.apache.org/jira/projects/CRAIL/issues/CRAIL-10)] Allow nodes in Crail to be non-enumerable.
* [[CRAIL-9](https://issues.apache.org/jira/projects/CRAIL/issues/CRAIL-9)] Allow Crail to use multiple cores if configured with NaRPC (RPC or Storage)

#### Bug fixes

* [[CRAIL-3](https://issues.apache.org/jira/projects/CRAIL/issues/CRAIL-3)] Directory index lost when writing
* [[CRAIL-4](https://issues.apache.org/jira/projects/CRAIL/issues/CRAIL-4)] getLong and getShort functions on the CrailBufferInputStream return double
* [[CRAIL-8](https://issues.apache.org/jira/projects/CRAIL/issues/CRAIL-8)] Make sure DataNodeInfo.key is invalidated on object updates
