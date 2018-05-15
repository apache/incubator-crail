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
