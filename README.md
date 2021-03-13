# Hadoop Assessment Tool

This tool is developed to enable detailed Assessment and automated assessment of Hadoop clusters. It helps measure the migration efforts from the current Hadoop cluster. It generates a PDF report with information related to the complete cluster according to different categories.

Following are the specific categories: 

1. Hardware & OS Footprint
2. Framework & Software Details
3. Data & Security
4. Network & Traffic
5. Operations & Monitoring
6. Application

## Tool Functionality

The Hadoop Assessment tool is built to analyze the on-premise Hadoop environment based on various factors/metrics. 

![Alt text](architectural_diagram.png?raw=true)


1. This python-based tool will use Cloudera API, Generic - YARN API, and OS based CLI request to retrieve information from the Hadoop cluster
2. Information from the APIs will come in the form of JSON files
3. Information from CLI command request will be outputs stored in variables
4. With the help of Python parsing methods, required insights about the features will be retrieved
5. As an output of tool execution, a PDF report will be generated which will contain information about all the features
6. Script will also generate a log file, which will contain execution information & errors (if any)

## Prerequisites
1. The tool requires **Python3**
2. The tool needs the below permissions to the run the code and generate the PDF report - **Sudo permission** to master node, user should be part of **hdfs superuser group**, **Cloudera manager admin credentials**, **hive metastore login credentials**
3. The tool will support the following Linux distributions - **Red Hat/Centos 7.9.x and above**, **SLES 12-SP5 and above**, **Ubuntu 16.04 LTS and above**, **Debian 8.9, 8.4, 8.2**

## Installation

1. Go to the **code** folder

```bash
cd code
```
2. Give execute permission to the scripts 

```bash
chmod +x build.sh
chmod +x run.sh
```

3. Run the first script for building the environment, using the command 

```bash
sh build.sh
```

4. Run the second script to run the python script, using the command

```bash
sh run.sh
```

## Contributing


## License
