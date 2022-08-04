#!bin/bash
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



## July 2022 - Release 2.1
### Hadoop Discovery Tool - Containerized version

**Features & Improvements:**

* Integrate kafka metrics into the output assessment report.
* Support for additional OS - The tool now support CentOS, Ubuntu, RHEl and Debian



**Bug Fixes:**



**Known issues:**




## June 2022 - Release 2.0

### Hadoop Discovery Tool - Containerized version

This is a containerized version of the Hadoop Discovery Tool

1. Hardware & OS Footprint
2. Framework & Software Details
3. Data & Security
4. Network & Traffic
5. Operations & Monitoring
6. Application

**Features & Improvements:**

* Containerized deployment of the tool
* Eliminate the necessity to install external packages or upgrade python environment in order to run the tool
* The tool is a combination of bash script for automated initial data collection for hdfs, kafka and OS library commands and a Docker container based on Python 3.8 version for execution of the tool and collecting and reporting additional Hadoop cluster metrics.


**Bug Fixes:**
* Fix for Cluster Metrics API calls
* Fix for Yarn API calls


**Known issues:**
-   Kafka integration from V1.0 will soon be integrated into the containerized version of the tool
-   This version support only CentOS and Ubuntu OS.


## March 2021 - Release 1.0

### Hadoop Discovery Tool

This is the initial version of the Hadoop Discovery Tool responsible for collecting the below information in an automated manner

1. Hardware & OS Footprint
2. Framework & Software Details
3. Data & Security
4. Network & Traffic
5. Operations & Monitoring
6. Application

**Known issues/Limitations:**

The V1.0 requires a bunch of additional libraries to be installed in the customer's hadoop cluster in order for the tool to gather the required information.