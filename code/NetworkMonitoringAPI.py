# ------------------------------------------------------------------------------
# This module contains all the features of the category Network, Traffic,
# Operation and Monitoring. This module contains the actual logic built with
# the help of Cloudera Manager API, Generic API and commands.
# -------------------------------------------------------------------------------

# Importing required libraries
from imports import *


class NetworkMonitoringAPI:
    """This Class has functions related to Network, Traffic, Operation and 
    Monitoring category.

    Has functions which fetch different network, monitoring, etc metrics from 
    Hadoop cluster like bandwidth, ingress, egress, disk speed, monitoring 
    tools, etc.

    Args:
        inputs (dict): Contains user input attributes
    """

    def __init__(self, inputs):
        """Initialize inputs"""

        self.inputs = inputs
        self.version = inputs["version"]
        self.cloudera_manager_host_ip = inputs["cloudera_manager_host_ip"]
        self.cloudera_manager_port = inputs["cloudera_manager_port"]
        self.cloudera_manager_username = inputs["cloudera_manager_username"]
        self.cloudera_manager_password = inputs["cloudera_manager_password"]
        self.cluster_name = inputs["cluster_name"]
        self.logger = inputs["logger"]
        self.ssl = inputs["ssl"]
        if self.ssl:
            self.http = "https"
        else:
            self.http = "http"
        self.start_date = inputs["start_date"]
        self.end_date = inputs["end_date"]

    def maxBandwidth(self):
        """Get maximum bandwidth of cluster.

        Returns:
            max_bandwidth (str): maximum bandwidth.
        """

        try:
            subprocess.Popen(
                "awk '/MaxBandwidth/  {print $2}' /etc/vnstat.conf > MaxBandwidth.csv",shell=True,stdout=subprocess.PIPE,encoding="utf-8")
            maxbandwidth_df = pd.read_csv("MaxBandwidth.csv", delimiter="\n")
            max_bandwidth = str(maxbandwidth_df["MaxBandwidth"][0])
            self.logger.info("maxBandwidth successful")
            return max_bandwidth
        except Exception as e:
            self.logger.error("maxBandwidth failed", exc_info=True)
            return None

    def ingress(self):
        """Get ingress network traffic cluster.

        Returns:
            max_value (str) : Maximun ingress value
            min_value (str) : Minimun ingress value
            avg_value (str) : Average ingress value
            curr_value (str) : Current ingress value
        """

        try:
            traffic = subprocess.Popen(
                ' cd /sys/class/net/eth0/statistics/; old="$(<rx_bytes)"; coun=1 ;  while [[ "$coun" -le 10 ]]; do  now=$(<rx_bytes); echo $((($now-$old)/1024)); old=$now; coun=`expr $coun + 1` ; $(sleep 1)  ;done',shell=True,stdout=subprocess.PIPE,encoding="utf-8")
            traffic,err = traffic.communicate()
            traffic_list = traffic.split("\n", 10)
            traffic_list.remove("0")
            traffic_list.remove("")
            traffic_list = [int(i) for i in traffic_list]
            max_value = max(traffic_list)
            min_value = min(traffic_list)
            avg_value = (
                0 if len(traffic_list) == 0 else sum(traffic_list) / len(traffic_list)
            )
            curr_value = traffic_list[-1]
            self.logger.info("ingress successful")
            return max_value, min_value, avg_value, curr_value
        except Exception as e:
            self.logger.error("ingress failed", exc_info=True)
            return None

    def egress(self):
        """Get egress network traffic cluster.

        Returns:
            max_value (str) : Maximun egress value
            min_value (str) : Minimun egress value
            avg_value (str) : Average egress value
            curr_value (str) : Current egress value
        """

        try:
            traffic = subprocess.Popen(
                ' cd /sys/class/net/eth0/statistics/; old="$(<tx_bytes)"; coun=1 ;  while [[ "$coun" -le 10 ]]; do  now=$(<tx_bytes); echo $((($now-$old)/1024)); old=$now; coun=`expr $coun + 1` ; $(sleep 1)  ;done'
            ,shell=True,stdout=subprocess.PIPE,encoding="utf-8")
            traffic,err = traffic.communicate()
            traffic_list = traffic.split("\n", 10)
            traffic_list.remove("0")
            traffic_list.remove("")
            traffic_list = [int(i) for i in traffic_list]
            max_value = max(traffic_list)
            min_value = min(traffic_list)
            avg_value = (
                0 if len(traffic_list) == 0 else sum(traffic_list) / len(traffic_list)
            )
            curr_value = traffic_list[-1]
            self.logger.info("egress successful")
            return max_value, min_value, avg_value, curr_value
        except Exception as e:
            self.logger.error("egress failed", exc_info=True)
            return None

    def diskReadWrite(self):
        """Get disk read and write speed of cluster.

        Returns:
            total_disk_read (str) : Disk read speed
            total_disk_write (str) : Disk write speed
        """

        try:
            subprocess.Popen(
                "iostat -d | awk 'BEGIN{OFS= \",\" ;}NR>2{print $3, $4;} ' > disk.csv"
            ,shell=True,stdout=subprocess.PIPE,encoding="utf-8")
            disk_df = pd.read_csv("disk.csv", delimiter=",")
            disk_df = disk_df.fillna(0)
            disk_df.columns = ["disk_read", "disk_write"]
            total_disk_read = 0
            for i in disk_df["disk_read"]:
                total_disk_read = total_disk_read + float(i)
            total_disk_write = 0
            for i in disk_df["disk_write"]:
                total_disk_write = total_disk_write + float(i)
            self.logger.info("diskReadWrite successful")
            return total_disk_read, total_disk_write
        except Exception as e:
            self.logger.error("diskReadWrite failed", exc_info=True)
            return None

    def thirdPartyMonitor(self):
        """Get list of third party monitoring tools in cluster.

        Returns:
            softwares_installed (str): List of software installed in cluster.
            prometheus_server (str): Presence of prometheus in cluster
            grafana_server (str): Presence of grafana in cluster
            ganglia_server (str): Presence of ganglia in cluster
            check_mk_server (str): Presence of check_mk_server in cluster
        """

        try:
            os_name = subprocess.Popen("grep PRETTY_NAME /etc/os-release",shell=True,stdout=subprocess.PIPE,encoding="utf-8")
            os_name,err = os_name.communicate()
            os_name = os_name.lower()
            softwares_installed = ""
            if "centos" in os_name:
                softwares_installed = subprocess.Popen("rpm -qa",shell=True,stdout=subprocess.PIPE,encoding="utf-8")
                softwares_installed,err = softwares_installed.communicate()
            elif "debian" in os_name:
                softwares_installed = subprocess.Popen("dpkg -l",shell=True,stdout=subprocess.PIPE,encoding="utf-8")
                softwares_installed,err = softwares_installed.communicate()
            elif "ubuntu" in os_name:
                softwares_installed = subprocess.Popen("apt list --installed",shell=True,stdout=subprocess.PIPE,encoding="utf-8")
                softwares_installed,err = softwares_installed.communicate()
            elif "red hat" in os_name:
                softwares_installed = subprocess.Popen("rpm -qa",shell=True,stdout=subprocess.PIPE,encoding="utf-8")
                softwares_installed,err = softwares_installed.communicate()
            elif "suse" in os_name:
                softwares_installed = subprocess.Popen("rpm -qa",shell=True,stdout=subprocess.PIPE,encoding="utf-8")
                softwares_installed,err = softwares_installed.communicate()
            prometheus_server = subprocess.Popen(
                "systemctl status prometheus | grep active",
                shell=True,
                stdout=subprocess.PIPE,
                encoding="utf-8",
            )
            out, err = prometheus_server.communicate()
            if not out:
                prometheus_server = "Prometheus server is not present"
            else:
                prometheus_server = "Prometheus server is present"
            grafana_server = subprocess.Popen(
                "grafana-server -v | grep Version",
                shell=True,
                stdout=subprocess.PIPE,
                encoding="utf-8",
            )
            out, err = grafana_server.communicate()
            if not out:
                grafana_server = "grafana server is not present"
            else:
                grafana_server = "grafana server is present"
            ganglia_server = subprocess.Popen(
                'find / -iname "ganglia.conf" 2>/dev/null',
                shell=True,
                stdout=subprocess.PIPE,
                encoding="utf-8",
            )
            out, err = ganglia_server.communicate()
            if not out:
                ganglia_server = "ganglia server is not present"
            else:
                ganglia_server = "ganglia server is present"
            check_mk_server = subprocess.Popen(
                "omd version | grep Version",
                shell=True,
                stdout=subprocess.PIPE,
                encoding="utf-8",
            )
            out, err = check_mk_server.communicate()
            if not out:
                check_mk_server = "check mk server is not present"
            else:
                check_mk_server = "check mk server is present"
            self.logger.info("thirdPartyMonitor successful")
            return (
                softwares_installed,
                prometheus_server,
                grafana_server,
                ganglia_server,
                check_mk_server,
            )
        except Exception as e:
            self.logger.error("thirdPartyMonitor failed", exc_info=True)
            return None

    def getLogs(self):
        """Get logs paths in cluster.

        Returns:
            logs (str): List of logs path.
        """

        try:
            subprocess.Popen("ls -l /var/log > /root/data.csv",shell=True,stdout=subprocess.PIPE,encoding="utf-8")
            col_names = [
                "permission",
                "links",
                "owner",
                "group_owner",
                "size",
                "creation_month",
                "creation_date",
                "creation_time",
                "name",
            ]
            df11 = pd.read_csv(
                "/root/data.csv", names=col_names, delimiter=r"\s+", skiprows=1
            )
            subprocess.Popen("rm data.csv",shell=True,stdout=subprocess.PIPE,encoding="utf-8")
            remove_list = ["root", "chrony", "ntp"]
            logs = df11[~df11["owner"].isin(remove_list)]
            logs.reset_index(inplace=True)
            self.logger.info("getLogs successful")
            return logs
        except Exception as e:
            self.logger.error("getLogs failed", exc_info=True)
            return None

    def orchestrationTools(self):
        """Get orchestration tool details present in cluster.

        Returns:
            oozie_flag (str): Presence of oozie in cluster
            crontab_flag (str): Presence of crontab in cluster
            airflow_flag (str): Presence of airflow in cluster
        """

        try:
            orchestrate = subprocess.Popen("oozie admin -status | grep mode",shell=True,stdout=subprocess.PIPE,encoding="utf-8")
            orchestrate,err = orchestrate.communicate()
            if "NORMAL" in orchestrate:
                oozie_flag = "oozie is enabled"
            else:
                oozie_flag = "oozie is not enabled"
            crontab = subprocess.Popen(
                "whereis -b crontab | cut -d' ' -f2 | xargs rpm -qf"
                ,shell=True,stdout=subprocess.PIPE,encoding="utf-8")
            crontab,err = crontab.communicate()
            if crontab.find("cronie") == -1:
                crontab_flag = "crontab not installed"
            else:
                crontab_flag = "crontab is installed"
            airflow = subprocess.Popen("airflow version",shell=True,stdout=subprocess.PIPE,encoding="utf-8")
            airflow,err = airflow.communicate()
            if not airflow:
                airflow_flag = "airflow is not enabled"
            else:
                airflow_flag = "airflow is enabled"
            self.logger.info("orchestrationTools successful")
            return oozie_flag, crontab_flag, airflow_flag
        except Exception as e:
            self.logger.error("orchestrationTools failed", exc_info=True)
            return None

    def loggingTool(self):
        """Get logging tool details present in cluster.

        Returns:
            ddog (str): Presence of Datadog in cluster
            splunk (str): Presence of Splunk in cluster
            new_relic (str): Presence of Newrelic in cluster
            elastic_search (str): Presence of Elasticsearch in cluster
        """

        try:
            ddog = subprocess.Popen(
                "systemctl status datadog-agent | grep active",
                shell=True,
                stdout=subprocess.PIPE,
                encoding="utf-8",
            )
            out, err = ddog.communicate()
            if not out:
                ddog = "Datadog is not deployed"
            else:
                ddog = "Datadog is deployed"
            logging = subprocess.Popen(
                'find / -type f \( -iname "splunk" -o -iname "newrelic-infra.yml" -o -iname "elasticsearch.yml"\) 2>/dev/null'
                ,shell=True,stdout=subprocess.PIPE,encoding="utf-8")
            logging, err = logging.communicate()
            if logging.find("splunk") == -1:
                splunk = "Splunk not installed"
            else:
                splunk = "Splunk is installed"
            if logging.find("newrelic") == -1:
                new_relic = "Newrelic not installed"
            else:
                new_relic = "Newrelic is installed"
            if logging.find("elasticsearch") == -1:
                elastic_search = "Elasticsearch not installed"
            else:
                elastic_search = "Elasticsearch is installed"
            self.logger.info("loggingTool successful")
            return ddog, splunk, new_relic, elastic_search
        except Exception as e:
            self.logger.error("loggingTool failed", exc_info=True)
            return None
