# Importing required libraries
from imports import *


class DataAPI:
    """This Class has functions related to Cluster Data category.

    Has functions which fetch different data metrics from Hadoop cluster 
    like HDFS metrics, hive metrics, etc.

    Args:
        inputs (dict): Contains user input attributes
    """

    def __init__(self, inputs):
        """Initialize inputs"""

        self.inputs = inputs
        self.version = inputs["version"]
        self.cloudera_manager_host_ip = inputs["cloudera_manager_host_ip"]
        self.cloudera_manager_username = inputs["cloudera_manager_username"]
        self.cloudera_manager_password = inputs["cloudera_manager_password"]
        self.cluster_name = inputs["cluster_name"]
        self.logger = inputs["logger"]

    def totalSizeConfigured(self):
        """Get total storage size and storage at each node for HDFS.

        Returns:
            individual_node_size (list): Total storage of all node.
            total_storage (float): Total storage of cluster.
        """

        try:
            os.popen("hdfs dfsadmin -report > ./data.csv").read()
            dt = "Live datanodes "
            list_of_results = []
            flag = 0
            with open("data.csv") as fp:
                with open("out2.csv", "w") as f1:
                    for line in fp:
                        if dt in line:
                            flag = 1
                        if dt not in line and flag == 1:
                            line = line.replace(": ", "  ")
                            f1.write(re.sub("[^\S\r\n]{2,}", ",", line))
            dataframe = pd.read_csv("out2.csv")
            dataframe.dropna(axis=0, how="all", inplace=True)
            dataframe.to_csv("sizeConfigured.csv", index=False)
            dataframe = pd.read_csv("sizeConfigured.csv", names=["key", "value"])
            list_Hostnames = []
            list_Configured_Capacity = []
            count_row = dataframe.shape[0]
            for i in range(count_row):
                if dataframe.loc[i, "key"] == "Hostname":
                    list_Hostnames.append(dataframe.loc[i, "value"])
                if dataframe.loc[i, "key"] == "Configured Capacity":
                    list_Configured_Capacity.append(dataframe.loc[i, "value"])
            dictionary = {
                "Hostname": list_Hostnames,
                "Configured_Capacity": list_Configured_Capacity,
            }
            mapped_df = pd.DataFrame(dictionary)
            os.popen("rm data.csv").read()
            os.popen("rm out2.csv").read()
            mapped_df[
                ["Configured_Capacity_bytes", "Configured_Capacity"]
            ] = mapped_df.Configured_Capacity.str.split("\(|\)", expand=True).iloc[
                :, [0, 1]
            ]
            mapped_df["Configured_Capacity_bytes"] = mapped_df[
                "Configured_Capacity_bytes"
            ].astype(int)
            total_storage = (mapped_df["Configured_Capacity_bytes"].sum()) / (
                1024 * 1024 * 1024
            )
            individual_node_size = mapped_df["Configured_Capacity"].tolist()
            self.logger.info("totalSizeConfigured successful")
            return individual_node_size, total_storage
        except Exception as e:
            self.logger.error("totalSizeConfigured failed", exc_info=True)
            return None

    def replicationFactor(self):
        """Get HDFS replication factor.

        Returns:
            replication_factor (str): Replication factor value
        """

        try:
            replication_factor = os.popen(
                "hdfs getconf -confKey dfs.replication"
            ).read()
            self.logger.info("replicationFactor successful")
            return replication_factor
        except Exception as e:
            self.logger.error("replicationFactor failed", exc_info=True)
            return None

    def getTrashStatus(self):
        """Get config value for HDFS trash interval.

        Returns:
            trash_flag (str): Trash interval value
        """

        try:
            xml_data = os.popen("cat /etc/hadoop/conf/core-site.xml").read()
            root = ET.fromstring(xml_data)
            for val in root.findall("property"):
                name = val.find("name").text
                if "trash" not in name:
                    root.remove(val)
            trash_value = int(root[0][1].text)
            trash_flag = ""
            if trash_value > 0:
                trash_flag = "Enabled"
            else:
                trash_flag = "Disabled"
            self.logger.info("getTrashStatus successful")
            return trash_flag
        except Exception as e:
            self.logger.error("getTrashStatus failed", exc_info=True)
            return None

    def getCliresult(self, clipath):
        """Get HDFS size breakdown based on HDFS directory system.

        Args:
            clipath (str): HDFS path for storage breakdown
        Returns:
            hdfs_root_dir (str): HDFS storage breakdown
        """

        try:
            path = clipath
            out = subprocess.Popen(
                ["hadoop", "fs", "-du", "-h", path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            stdout, stderr = out.communicate()
            hdfs_root_dir = stdout
            self.logger.info("getCliresult successful")
            return hdfs_root_dir
        except Exception as e:
            self.logger.error("getCliresult failed", exc_info=True)
            return None

    def getHdfsCapacity(self, cluster_name):
        """Get HDFS storage available data over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            hdfs_capacity_df (DataFrame): HDFS storage available over time
            hdfs_storage_config (float): Average HDFS storage available
        """

        try:
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}".format(
                        self.cloudera_manager_host_ip,
                        start_date,
                        cluster_name,
                        end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}".format(
                        self.cloudera_manager_host_ip,
                        start_date,
                        cluster_name,
                        end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}".format(
                        self.cloudera_manager_host_ip,
                        start_date,
                        cluster_name,
                        end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            if r.status_code == 200:
                hdfs_capacity = r.json()
                with open(
                    "Discovery_Report/{}/hdfs_capacity.json".format(cluster_name), "w"
                ) as fp:
                    json.dump(hdfs_capacity, fp, indent=4)
                hdfs_capacity_list = hdfs_capacity["items"][0]["timeSeries"][0]["data"]
                hdfs_capacity_df = pd.DataFrame(hdfs_capacity_list)
                hdfs_capacity_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            hdfs_capacity_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": hdfs_capacity_df["value"] / 1024 / 1024 / 1024,
                    }
                )
                hdfs_capacity_df["DateTime"] = pd.to_datetime(
                    hdfs_capacity_df["DateTime"]
                )
                hdfs_capacity_df = (
                    pd.DataFrame(
                        pd.date_range(
                            hdfs_capacity_df["DateTime"].min(),
                            hdfs_capacity_df["DateTime"].max(),
                            freq="H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(hdfs_capacity_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                hdfs_capacity_df["Time"] = hdfs_capacity_df.DateTime.dt.strftime(
                    "%d-%b %H:%M"
                )
                hdfs_capacity_df = hdfs_capacity_df.set_index("Time")
                hdfs_storage_config = hdfs_capacity_df.sort_values(
                    by="DateTime", ascending=False
                ).iloc[0]["Mean"]
                self.logger.info("getHdfsCapacity successful")
                return hdfs_capacity_df, hdfs_storage_config
            else:
                self.logger.error(
                    "getHdfsCapacity failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getHdfsCapacity failed", exc_info=True)
            return None

    def getHdfsCapacityUsed(self, cluster_name):
        """Get HDFS storage used data over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            hdfs_capacity_used_df (DataFrame): HDFS storage used over time
            hdfs_storage_used (float): Average HDFS storage used
        """

        try:
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity_used%2Bdfs_capacity_used_non_hdfs%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}".format(
                        self.cloudera_manager_host_ip,
                        start_date,
                        cluster_name,
                        end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity_used%2Bdfs_capacity_used_non_hdfs%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}".format(
                        self.cloudera_manager_host_ip,
                        start_date,
                        cluster_name,
                        end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity_used%2Bdfs_capacity_used_non_hdfs%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}".format(
                        self.cloudera_manager_host_ip,
                        start_date,
                        cluster_name,
                        end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            if r.status_code == 200:
                hdfs_capacity_used = r.json()
                with open(
                    "Discovery_Report/{}/hdfs_capacity_used.json".format(cluster_name),
                    "w",
                ) as fp:
                    json.dump(hdfs_capacity_used, fp, indent=4)
                hdfs_capacity_used_list = hdfs_capacity_used["items"][0]["timeSeries"][
                    0
                ]["data"]
                hdfs_capacity_used_df = pd.DataFrame(hdfs_capacity_used_list)
                hdfs_capacity_used_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            hdfs_capacity_used_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": hdfs_capacity_used_df["value"] / 1024 / 1024 / 1024,
                    }
                )
                hdfs_capacity_used_df["DateTime"] = pd.to_datetime(
                    hdfs_capacity_used_df["DateTime"]
                )
                hdfs_capacity_used_avg = (
                    hdfs_capacity_used_df["Mean"].sum()
                    / hdfs_capacity_used_df["DateTime"].count()
                )
                hdfs_capacity_used_df = (
                    pd.DataFrame(
                        pd.date_range(
                            hdfs_capacity_used_df["DateTime"].min(),
                            hdfs_capacity_used_df["DateTime"].max(),
                            freq="H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(hdfs_capacity_used_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                hdfs_capacity_used_df[
                    "Time"
                ] = hdfs_capacity_used_df.DateTime.dt.strftime("%d-%b %H:%M")
                hdfs_capacity_used_df = hdfs_capacity_used_df.set_index("Time")
                hdfs_storage_used = hdfs_capacity_used_df.sort_values(
                    by="DateTime", ascending=False
                ).iloc[0]["Mean"]
                self.logger.info("getHdfsCapacityUsed successful")
                return hdfs_capacity_used_df, hdfs_storage_used
            else:
                self.logger.error(
                    "getHdfsCapacityUsed failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getHdfsCapacityUsed failed", exc_info=True)
            return None
