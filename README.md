#  Hadoop Assessment Tool - Containerized Version

This tool is developed to enable detailed Assessment and automated assessment of Hadoop clusters. It helps measure the migration efforts from the current Hadoop cluster. It generates a PDF report with information related to the complete cluster according to different categories.


The tool has been completely containerized thereby ensuring that no external libraries and dependencies are installed directly on the worker/edge nodes of your Hadoop Cluster.

The current version of the tool is a Bash Script + Docker Container approach to ensure that no changes are made to the customer's environments and the docker container (tool) can be deleted after the assessment is complete and this ensures that the sanity of your Hadoop Cluster Environment remains intact.

**How do I get access to the Automated Script or Docker Image of the Tool?** Please reach out to your Google account teams or PSO teams to get access to the Docker image of the tool along with the detailed user guide.

For others, please follow the process below.


**No data that is collected is sent outside your worker/edge nodes from where the tool is being run. The PDF generated can be shared with the Google teams after the customer's perusal of the collected data**

Following are the specific categories that the tool gathers information about: 

1. Hardware & OS Footprint
2. Framework & Software Details
3. Data & Security
4. Network & Traffic
5. Operations & Monitoring
6. Applications
7. Cloudera Hadoop Metrics
8. Kafka Metrics
9. Yarn application metrics

**The more integrated Edge/worker node on which the tool is being run with the rest of the cluster wrt HDFS, Kafla, API calls, the better would the be the data collection and result set**

**Ensure that in case of SSL communications, the worker/edge node has SSL access to Cloudera Manager URL ports**

**It is recommended that you run the bash script and the tool with a sudo equivalent user to avoid permission issues**

**It is recommended that you use an admin equivalent user to connect to Cloudera Admin portal to avoid authorization issues during API calls**


## 2 .Tool Functionality

The Hadoop Assessment tool is built to analyze the on-premise Hadoop environment based on various factors/metrics. 

![Alt text](https://github.com/GoogleCloudPlatform/hadoop-discovery-tool/blob/8384caf597e21041b863215a2295926bf1d9ab75/architectural_diagram.png)


1. This python-based tool will use **Cloudera API**, **YARN API**, **File system config files** and OS based native **CLI** request to retrieve information from the Hadoop cluster
2. Information from the APIs will come in the form of JSON responses
3. Information from CLI command request will be outputs stored in CSV files and JSON.
4. With the help of Python parsing methods inside docker container, required insights generated about the hadoop cluster will be retrieved
5. As an output of tool execution, a **PDF report** will be generated which will contain information about all the features
6. Script will also generate a **log file**, which will contain execution information & errors (if any)

## 3. Prerequisites
1. Important highlights of the Tool
   1. The worker or the edge node must either have docker cli pre installed (either enterprise or community version) or have network connectivity so that the tool can download and install docker cli (community version) automatically on confirmation from the end user.
   2. You must have **gsutil** or **git** or a way to download the Bash script file and the Docker Image. The preferred method would be to have **gsutil** as this would automate the whole deployment process.
      You can install gsutil from https://cloud.google.com/storage/docs/gsutil_install for your respective Operating System.
   3. The tool supports Cloudera version - **CDH 5.13.3 and above; CDH 6.X, CDH 7.X**
   4. The tool runs on the following OS versions **Centos, Ubuntu** **Debian** **RHEL**
2. Complete information to run the tool   
   1. It is recommended to run this tool on an edge/worker node that has access to Cloudera Manager URLs, HDFS filesystem to get best results.
   2. The tool requires **~ 1.3 gigabytes** of space
   3. **Preferred time** to run the tool: It is recommended to run the tool during hours when there is the least workload on the Hadoop cluster
   4. This tool supports the following **Cloudera versions:**
      1. CDH 5.13.3 and above
      2. CDH 6.x
      3. CDH 7.x
   5. Cloudera manager user should have one of the following **roles:**
      
      | Hadoop Version | Roles |
      |-----------------|:-------------|
      | CDH 5.13.3 | Dashboard User, User Administrator, Full Administrator, Operator, BDR Administrator, Cluster Administrator, Limited Operator, Configurator, Read-Only, Auditor, Key Administrator, Navigator Administrator |
      | CDH 6.x | Dashboard User, User Administrator, Full Administrator, Operator, BDR Administrator, Cluster Administrator, Limited Operator, Configurator, Read-Only, Auditor, Key Administrator, Navigator Administrator |
      | CDH 7.x | Auditor, Cluster Administrator, Configurator, Dashboard User, Full Administrator, Key Administrator, Limited Cluster Administrator, Limited Operator, Navigator Administrator, Operator, Read Only, Replication Administrator, User Administrator |
   6. Tool supports below hive metastore
          1. PostgreSQL
          2. MySQL
          3. Oracle
   7. Tool might need access to **dfsadmin** utility.   
   8. Detailed execution information will be logged in the two log files, which will be present at location :
         1. Python Log:- `./hadoop_assessment_tool_{YYYY-MM-DD_HH:MM:SS}.log`
         2. Shell Log:- `./hadoop_assessment_tool_terminal.log`
   9. After a successful tool execution, a PDF report will be generated at the location insider docker from where the tool is run:
   ` ./hadoop_assessment_report_{YYYY-MM-DD_HH-MM-SS}.pdf`

3. User Input Requirements
   1. The tool needs the below permissions to run the code and generate the PDF report:
      1. Sudo permission on the node.
      2. The path where Cloudera Hadoop, Hive and Spark is installed should be known as this will be requested as an input in the bash script.
      For example, if hadoop is installed on /etc/hadoop, please enter /etc as the input when prompted
      3. Cloudera Manager(User should have one of the roles mentioned in 3.1.9) and edge/worker nodes from where the tool is being run should have firewal ports open for Cloudera manager host and IP
         1. Host IP
         2. Port Number
         3. User Name
         4. Password
         5. Cluster Name
      4. Hive Metastore
         1. User Name
         2. Password
      5. SSL is enabled or not(conditional input - if automatic detection doesn't work it will be prompted). Please ensure that your edge/worker node from where the tool is being run has access to SSL ports of the Cloudera Manager portal. If certificate exchange is required, please do so.
      6. Yarn (conditional input - if automatic detection doesn't work it will be prompted)
         1. Resource managers hostname or IP address
         2. Port Number

## 4. Installation Steps

**Ensure you are logged into as Root user (or sudo equivalent) into the worker/edge node**

**Name of bash script file to be used**

**hadoop-discovery.sh**


1. **Step 1**: Please reach out to your Google account teams for detailed user guide. For other users, please execxute the below bash script to download the bash script.

   ```bash
   gsutil -m cp -r gs://hadoop-discovery/hadoop-discovery.sh .
   ```

2. **Step 2**: Provide execute permission on Bash script
   ```bash
   chmod +x hadoop-discovery.sh
   ```

3. **Step 3**: Execute the Bash script 
   ```bash
   ./hadoop-discovery.sh
   ```

**The tool will ask you for your permission to enter your Email Address and Organization - This is purely to track the end user utilization of the tool and no other data is being transmitted. It would be greatly help if these details can be filled at least once to solicit tool's feedback**

**Step 3.1**: Enter your choice of Operating System
               1. CentOS
               2. Ubuntu
               3. Debian
               4. RedHat
               
4. **Step 4**: At the checkpoint seeking confirmation, provide Y or y to continue. The tool will check for docker-cli (community version or enterprise version) if its installed in the node. If its not installed, the script will install docker-cli (community version). This is mandatory for the tool to run.

5. **Step 5**: **Ensure that __consumer_offsets topic exists in all brokers that you are providing inputs for, else the connectivity to Kafka broker would be considered as a failure after 3 attempts**

For more information, https://kafka.apache.org/11/documentation.html#impl_offsettracking

Enter your Kafka Broker Details [Ensure that you have Zookeeper installed on the worker/edge nodes with zookeeper.conf files]
            1. Number of Brokers
            2. Broker host [FQDN/ IP]
            3. Broker port number
            4. Broker Log path
            5. kafka-client.conf path

The tool collects Kafka related metrics - Number of Topics, Size of Topics etc. This will take a few minutes depending on the number of Kafka topics.



**The tool considers Kafka connecitivty a success only if you have a topic existing with the name __consumer_offsets**

6. **Step 6**: The tool will stop here for you to go through list of details collected at this step
      1. Hadoop Cluster Data - Hardware, Storage, Directory structure
      2. Operating System Details - Version, Distribution
      3. List of third party libraries installed
      4. JDBC/ODBC installed driver information
      5. List of Installed Services & their Status - httpd, apache, ntp, prometheus, grafana, ganglia, airflow, oozie etc.
      6. HDFS storage policy
      7. Spark version
      8. Hive metrics - Execution Engine, Txn manager and Concurrency
      9. Hardware Details
      10. Network Ingress/Egress statistics
      11. List of installed connectors
      12. Additional Software packages installed
      
Please press Y/y to continue or N/n to stop. The script file will take a few minutes to collect this data.

On pressing Y/y the script file continue to pull the docker container image of the tool automatically and load the image and deploy.

   **Step success message: Hadoop Assessment tool deployed successfully**

7. **Step 7**: Identify the container id of the deployed container with the below command. Copy it and keep it as this will be used in the next step
   ```bash
   sudo docker ps
   ```

8. **Step 8**: Log into the deployed docker container with the below command and container ID received from Step 6

   ```bash
   sudo docker exec -it -u 0 <container id> /bin/bash
   ```
   This will log you into the container with root access to perform the next step of tool execution.
   
9. **Step 9**: Once Step 8 is successfully complete, execute run.sh script
   ```bash
   run.sh
   ```

10. **Step 10**: Following details would be required for further execution of the script:
    1. **Step 10.1** Enter the installation directory for Hadoop, Hive and Spark

       Enter the Hadoop installation directory, for example if Hadoop is installed in /etc/hadoop enter **/etc**
       
       Enter the Hive installation directory, for example if Hive is installed in /etc/hive enter **/etc**
    
       Enter the Spark installation directory, for example if Spark is installed in /etc/spark enter **/etc**
      
    2. **Step 10.2(Conditional step) - SSL:**  If the tool is unable to automatically detect SSL enabled on the cluster, it would display the following message
       ```
       Do you have SSL enabled for your cluster? [y/n]
       ```
       1. **Step 10.2.1:** If you select **'y'**, continue to Step 9.3 -
          ```
          As SSL is enabled, enter the details accordingly
          ```
       2. **Step 10.2.2:** If you select **'n'**, continue to Step 9.3 -
          ```
          As SSL is disabled, enter the details accordingly
          ```
       **Note** - If you have SSL enabled and the input is y, please ensure there is connectivity from Edge/worker node from where the tool is being run to Clouder Manager Host and Port through HTTPS
    3. **Step 10.3 - Cloudera Manager credentials:** the prompt would ask you if you want to provide the Cloudera Manager credentials, you would have to select **'y'** or **'n'**
       1. **Step 10.3.1:** If you select **'y'**, continue to Step 8.2.1.1 -
       
           `
           A major number of metrics generation would require Cloudera manager credentials Therefore, would you be able to provide your Cloudera Manager credentials? [y/n]: 
           `

           1. **Step 10.3.1.1:** Enter Cloudera Manager Host IP
          
              `
              Enter Cloudera Manager Host IP:
              `
           2. **Step 10.3.1.2:** Cloudera Manager Port - the prompt would ask you if your  Cloudera Manager Port is 7180. If true select **'y'** else select **'n'**
          
              `
              Enter Cloudera Manager Host IP:
              `
                1. **Step 10.3.1.2.1:** If you select **'y'**, continue to Step 9.3.1.3
              
                    `
                    Is your Cloudera Manager Port number 7180? [y/n]: 
                    `
                2. **Step 10.3.1.2.2:** If you select **'n'**, continue to Step 9.3.1.2.2
              
                    `
                    Is your Cloudera Manager Port number 7180? [y/n]: 
                    `
                3. **Step 10.3.1.2.3:** Since the port number is not 7180, enter your Cloudera Manager Port number
              
                    `
                    Enter your Cloudera Manager Port number: 
                    `
           3. **Step 10.3.1.3:** Cloudera Manager username. Preferably provide your Cloudera Manager admin credentials.
              ```bash
              Enter Cloudera Manager username:
              ```
           4. **Step 10.3.1.4:** Cloudera Manager password 
              ```bash
              Enter Cloudera Manager password:
              ```
           5. **Step 10.3.1.5:** Select the Cluster
              ```bash
              Select the cluster from the list below:
              1] Cluster 1
              2] Cluster 2
               .
               .
              n] Cluster n
              Enter the serial number (1/2/../n) for the selected cluster name:
              ```
       2. **Step 10.3.2:** If you select **'n'**, continue to Step 9.4
          ```bash
           A major number of metrics generation would require Cloudera manager credentials Therefore, would you be able to provide your Cloudera Manager credentials? [y/n]: 
          ```
    4. **Step 10.4: Hive Metastore database credentials** - This would only be prompted if Cloudera Manager credentials were provided in the previous step. The prompt would ask you if you want to provide Hive Metastore database credentials, you would have to select **'y'** or **'n'**
       1. **Step 10.4.1:** If you select **'y'**, continue to Step 9.4.1.1
          ```bash
           To view hive-related metrics, would you be able to enter Hive credentials?[y/n]: 
          ```
          1. **Step 10.4.1.1:** Hive Metastore username - the prompt would ask you to enter your Hive Metastore username
             ```bash
              Enter Hive Metastore username: hive
             ```
          2. **Step 10.4.1.2:** Hive Metastore password - the prompt would ask you to enter your Hive Metastore password
             ```bash
               Enter Hive Metastore password:
             ```
       2. **Step 10.4.2:** If you select ‘n’, continue to the next step
          ```bash
           To view hive-related metrics, would you be able to enter Hive credentials?[y/n]: 
          ```
    5. **Step 10.5 (Conditional step) - YARN Configurations:** If the tool is unable to automatically detect YARN configurations, it would prompt you to enter Yarn credentials,  you would have to select **'y'** or **'n'**
       1. **Step 10.5.1:** If you select **'y'**, continue to Step 8.4.1.1 
          ```bash
           To view yarn-related metrics, would you be able to enter Yarn credentials?[y/n]:
          ```
          1. **Step 10.5.1.1:** Enter Yarn Resource Manager Host IP or Hostname:
             ```bash
              Enter Yarn Resource Manager Host IP or Hostname:
             ```
          2. **Step 10.5.1.2:** Enter Yarn Resource Manager Port:
             ```bash
              Enter Yarn Resource Manager Port:
             ```
       2. **Step 10.5.2:** If you select **'n'**, continue to Step 8.5
          ```bash
           To view yarn-related metrics, would you be able to enter Yarn credentials?[y/n]:
          ```
          
    6. **Step 10.6:** Date range for the Assessment report - Select one of the below options for a date range to generate the report for this time period
       ```bash
       Select the time range of the PDF Assessment report from the options below:
       [1] Week: generates the report from today to 7 days prior
       [2] Month: generates the report from today to 30 days prior
       [3] Custom: generates the report for a custom time period
       Enter the serial number [1/2/3] as required:
       ```
       1. If you select 1 and 2, the report automatically gets generated based on the selected range as per the description.
       2. If you select 3, here’s the prompt that appears, Important note: Please enter the timing details according to the timezone of the tool hosting node:
          ```bash
          Enter start date: [YYYY-MM-DD HH:MM]
          2021-03-15 00:00
          Enter end date: [YYYY-MM-DD HH:MM]
          2021-03-30 00:00
          ```
11. **Step 11:** PDF Report - A PDF report will be generated at the end of successful execution in the same location inside docker container from where the script is being run [/app/] , which can be downloaded with the help of the same SCP client or WinSCP tool with the help of which we uploaded the tar in Step1. If you want to move the PDF file to your host from docker container, exit from the docker container and run the command
   ```bash
   docker cp <containerid>:/app/<pdf file name> .
   ```
## FAQ

* What is this tool?\
The Hadoop assessment tool is a quick way to get an understanding of your Hadoop cluster topology, workloads, and utilization; it's a combination of scripts that interact with Cloudera Manager / YARN.

* Are the scripts invasive?\
The scripts are not invasive and make read-only API calls. The tool is containerized and functions in a way where you wont need to install external libraries on worker or edge nodes for the tool to assess your hadoop cluster.

* Why should I run this tool?\
The tool provides critical information that helps everyone understand the current state and where optimizations can be gained during the migration phase.

* How long will it take to run?\
A Hadoop Administrator can run the tool in less than half a day.

* What's the output?\
A PDF report will be generated at the end with a summary of insights and recommendations (Hadoop Assessment tool sample report: https://drive.google.com/file/d/1NmVj4uvxUPj5QwHATsb5aKTVgB9vfpZf/view?resourcekey=0-RMr51QdyjWWL81KvzgxGHw).

* What Linux Packages are required to be installed for the tool?\
You can choose to install gsutil as well to download the bash script file but is not mandatory. This can also be provided by your Google representative.

The tool only requires docker-cli to be installed. This is mandatory. All other package and libraries will be deployed within the docker container.

* Can I delete my docker container after the tool execution is complete and report has been generated?

Yes, you can delete the docker container completely from your worker/edge nodes using the below commands.

Copy the container ID from the below command
```bash
sudo docker ps
```
Stop the container
```bash
sudo docker stop <container id>
```
Delete the container
```bash
sudo docker container prune 
```
Please be mindful of the fact that there should not be any other stopped/running containers in your server while running prune command. 

* What if I don't want to install this tool on my cluster?\
You could add a temporary edge node where you can install this discovery tool and then remove the edge node once a pdf report is generated.

* Is dfsadmin access mandatory?\
No, dfsadmin access is optional.

## Contributing
We'd love to hear your feedback on the tool. For collaboration opportunities please reach out to @yadavaja@google.com, gdc-atc@google.com
