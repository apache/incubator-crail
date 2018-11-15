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

![license](https://img.shields.io/github/license/apache/incubator-crail.svg)
[![Documentation Status](https://readthedocs.org/projects/incubator-crail/badge/?version=latest)](https://incubator-crail.readthedocs.io/en/latest/?badge=latest)

# Apache Crail (incubating)

Apache Crail is a fast multi-tiered distributed storage system designed from ground up for high-performance network and storage hardware.
It marks the backbone of the Crail I/O architecture, which is described in more detail on [crail.incubator.apache.org](http://crail.incubator.apache.org). 
The unique features of Crail include:

* Zero-copy network access from userspace
* Integration of multiple storage tiers such DRAM, flash and disaggregated shared storage
* Ultra-low latencies for both meta data and data operations. For instance: opening, reading and closing a small file residing in the distributed DRAM tier less than 10 microseconds, which is in the same ballpark as some of the fastest RDMA-based key/value stores
* High-performance sequential read/write operations: For instance: read operations on large files residing in the distributed DRAM tier are typically limited only by the performance of the network
* Very low CPU consumption: a single core sharing both application and file system client can drive sequential read/write operations at the speed of up to 100Gbps and more
* Asynchronous API leveraging the asynchronous nature of RDMA-based networking hardware
* Extensible plugin architecture: new storage tiers tailored to specific hardware can be added easily

Crail is implemented in Java offering a Java API which integrates directly with the Java off-heap memory. Crail is designed for performance critical temporary data within a scope of a rack or two.

## Documentation

For information about how to deploy, run, test and program against Crail:
* Refer to https://incubator-crail.readthedocs.org
* Or build from source by running `make html` in `/doc` and opening `/doc/build/html/index.html`


## Community

Please join the Crail developer mailing list for discussions and notifications. The list is at:

dev@crail.incubator.apache.org.

## Disclaimer

Apache Crail is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC.
Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications,
and decision making process have stabilized in a manner consistent with other successful ASF projects.
While incubation status is not necessarily a reflection of the completeness or stability of the code,
it does indicate that the project has yet to be fully endorsed by the ASF.
