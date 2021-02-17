# Importing Required Libraries
from imports import *


# This Class has functions related to Hardware and OS Footprint category
class HardwareOSAPI:
    # Initialize Inputs
    def __init__(self, inputs):
        self.inputs = inputs
        self.version = inputs["version"]
        self.cloudera_manager_host_ip = inputs["cloudera_manager_host_ip"]
        self.cloudera_manager_username = inputs["cloudera_manager_username"]
        self.cloudera_manager_password = inputs["cloudera_manager_password"]
        self.cluster_name = inputs["cluster_name"]
        self.logger = inputs["logger"]

    # Get OS version using system CLI command
    def osVersion(self):
        try:
            os_version = os.popen("cat /etc/*-release").read()
            os_version = os_version.replace("\n", ",")
            os_version = os_version.split(",")
            os_version_series = pd.Series(data=os_version).T
            os_version = os_version_series.iloc[18]
            self.logger.info("osVersion successful")
            return os_version
        except Exception as e:
            self.logger.error("osVersion failed", exc_info=True)
            return None

    # Get list of all clusters present in Cloudera Manager
    def clusterItems(self):
        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/clusters".format(
                        self.cloudera_manager_host_ip
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "http://{}:7180/api/v33/clusters".format(
                        self.cloudera_manager_host_ip
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "http://{}:7180/api/v19/clusters".format(
                        self.cloudera_manager_host_ip
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            if r.status_code == 200:
                cluster = r.json()
                with open("Discovery_Report/clusters.json", "w") as fp:
                    json.dump(cluster, fp, indent=4)
                cluster_items = cluster["items"]
                self.logger.info("clusterItems successful")
                return cluster_items
            else:
                self.logger.error(
                    "clusterItems failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
        except Exception as e:
            self.logger.error("clusterItems failed", exc_info=True)
            return None

    # Get List of all hosts present in a cluster
    def clusterHostItems(self, clusterName):
        try:
            cluster_name = clusterName
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/clusters/{}/hosts".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "http://{}:7180/api/v33/clusters/{}/hosts".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "http://{}:7180/api/v19/clusters/{}/hosts".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            if r.status_code == 200:
                cluster_host = r.json()
                clusterHostLen = len(cluster_host["items"])
                with open(
                    "Discovery_Report/{}/clusters_host.json".format(cluster_name), "w"
                ) as fp:
                    json.dump(cluster_host, fp, indent=4)
                cluster_host_items = cluster_host["items"]
                self.logger.info("clusterHostItems successful")
                return cluster_host_items, clusterHostLen
            else:
                self.logger.error(
                    "clusterHostItems failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
        except Exception as e:
            self.logger.error("clusterHostItems failed", exc_info=True)
            return None

    # Get list of services present in a cluster with its details
    def clusterServiceItem(self, clusterName):
        try:
            cluster_name = clusterName
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/clusters/{}/services".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "http://{}:7180/api/v33/clusters/{}/services".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "http://{}:7180/api/v19/clusters/{}/services".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            if r.status_code == 200:
                cluster_services = r.json()
                with open(
                    "Discovery_Report/{}/cluster_services.json".format(cluster_name),
                    "w",
                ) as fp:
                    json.dump(cluster_services, fp, indent=4)
                cluster_service_item = cluster_services["items"]
                self.logger.info("clusterServiceItem successful")
                return cluster_service_item
            else:
                self.logger.error(
                    "clusterServiceItem failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
        except Exception as e:
            self.logger.error("clusterServiceItem failed", exc_info=True)
            return None

    # Get detailed specs of a host
    def hostData(self, hostId):
        try:
            hostid = hostId
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/hosts/{}".format(
                        self.cloudera_manager_host_ip, hostid
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "http://{}:7180/api/v33/hosts/{}".format(
                        self.cloudera_manager_host_ip, hostid
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "http://{}:7180/api/v19/hosts/{}".format(
                        self.cloudera_manager_host_ip, hostid
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            if r.status_code == 200:
                host_data = r.json()
                self.logger.info("hostData successful")
                return host_data
            else:
                self.logger.error(
                    "hostData failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
        except Exception as e:
            self.logger.error("hostData failed", exc_info=True)
            return None

    # Get cores availability data over a date range
    def clusterTotalCores(self, clusterName):
        try:
            cluster_name = clusterName
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_cores_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_cores_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_cores_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                cluster_total_cores = r.json()
                with open(
                    "Discovery_Report/{}/cluster_total_cores.json".format(cluster_name),
                    "w",
                ) as fp:
                    json.dump(cluster_total_cores, fp, indent=4)
                cluster_total_cores_list = cluster_total_cores["items"][0][
                    "timeSeries"
                ][0]["data"]
                cluster_total_cores_df = pd.DataFrame(cluster_total_cores_list)
                cluster_total_cores_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            cluster_total_cores_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": cluster_total_cores_df["value"],
                    }
                )
                cluster_total_cores_df["DateTime"] = pd.to_datetime(
                    cluster_total_cores_df["DateTime"]
                )
                cluster_total_cores_df = (
                    pd.DataFrame(
                        pd.date_range(
                            cluster_total_cores_df["DateTime"].min(),
                            cluster_total_cores_df["DateTime"].max(),
                            freq="1H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(cluster_total_cores_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                cluster_total_cores_df[
                    "Time"
                ] = cluster_total_cores_df.DateTime.dt.strftime("%d-%b %H:%M")
                cluster_total_cores_df = cluster_total_cores_df.set_index("Time")
                self.logger.info("clusterTotalCores successful")
                return cluster_total_cores_df
            else:
                self.logger.error(
                    "clusterTotalCores failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
        except Exception as e:
            self.logger.error("clusterTotalCores failed", exc_info=True)
            return None

    # Get cores usage data over a date range
    def clusterCpuUsage(self, clusterName):
        try:
            cluster_name = clusterName
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20cpu_percent_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20cpu_percent_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20cpu_percent_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                cluster_cpu_usage = r.json()
                with open(
                    "Discovery_Report/{}/cluster_cpu_usage.json".format(cluster_name),
                    "w",
                ) as fp:
                    json.dump(cluster_cpu_usage, fp, indent=4)
                cluster_cpu_usage_list = cluster_cpu_usage["items"][0]["timeSeries"][0][
                    "data"
                ]
                cluster_cpu_usage_df = pd.DataFrame(cluster_cpu_usage_list)
                cluster_cpu_usage_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            cluster_cpu_usage_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": cluster_cpu_usage_df["value"],
                        "Min": cluster_cpu_usage_df["aggregateStatistics"].apply(
                            pd.Series
                        )["min"],
                        "Max": cluster_cpu_usage_df["aggregateStatistics"].apply(
                            pd.Series
                        )["max"],
                    }
                )
                cluster_cpu_usage_df["DateTime"] = pd.to_datetime(
                    cluster_cpu_usage_df["DateTime"]
                )
                cluster_cpu_usage_avg = (
                    cluster_cpu_usage_df["Mean"].sum()
                    / cluster_cpu_usage_df["DateTime"].count()
                )
                cluster_cpu_usage_df = (
                    pd.DataFrame(
                        pd.date_range(
                            cluster_cpu_usage_df["DateTime"].min(),
                            cluster_cpu_usage_df["DateTime"].max(),
                            freq="H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(cluster_cpu_usage_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                cluster_cpu_usage_df[
                    "Time"
                ] = cluster_cpu_usage_df.DateTime.dt.strftime("%d-%b %H:%M")
                cluster_cpu_usage_df = cluster_cpu_usage_df.set_index("Time")
                self.logger.info("clusterCpuUsage successful")
                return cluster_cpu_usage_df, cluster_cpu_usage_avg
            else:
                self.logger.error(
                    "clusterCpuUsage failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
        except Exception as e:
            self.logger.error("clusterCpuUsage failed", exc_info=True)
            return None

    # Get memory availability data over a date range
    def clusterTotalMemory(self, clusterName):
        try:
            cluster_name = clusterName
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                cluster_total_memory = r.json()
                with open(
                    "Discovery_Report/{}/cluster_total_memory.json".format(
                        cluster_name
                    ),
                    "w",
                ) as fp:
                    json.dump(cluster_total_memory, fp, indent=4)
                cluster_total_memory_list = cluster_total_memory["items"][0][
                    "timeSeries"
                ][0]["data"]
                cluster_total_memory_df = pd.DataFrame(cluster_total_memory_list)
                cluster_total_memory_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            cluster_total_memory_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": cluster_total_memory_df["value"] / 1024 / 1024 / 1024,
                    }
                )
                cluster_total_memory_df["DateTime"] = pd.to_datetime(
                    cluster_total_memory_df["DateTime"]
                )
                cluster_total_memory_df = (
                    pd.DataFrame(
                        pd.date_range(
                            cluster_total_memory_df["DateTime"].min(),
                            cluster_total_memory_df["DateTime"].max(),
                            freq="1H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(cluster_total_memory_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                cluster_total_memory_df[
                    "Time"
                ] = cluster_total_memory_df.DateTime.dt.strftime("%d-%b %H:%M")
                cluster_total_memory_df = cluster_total_memory_df.set_index("Time")
                self.logger.info("clusterTotalMemor successful")
                return cluster_total_memory_df
            else:
                self.logger.error(
                    "clusterTotalMemor failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
        except Exception as e:
            self.logger.error("clusterTotalMemor failed", exc_info=True)
            return None

    # Get memory usage data over a date range
    def clusterMemoryUsage(self, clusterName):
        try:
            cluster_name = clusterName
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20100*total_physical_memory_used_across_hosts/total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20100*total_physical_memory_used_across_hosts/total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20100*total_physical_memory_used_across_hosts/total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                cluster_memory_usage = r.json()
                with open(
                    "Discovery_Report/{}/cluster_memory_usage.json".format(
                        cluster_name
                    ),
                    "w",
                ) as fp:
                    json.dump(cluster_memory_usage, fp, indent=4)
                cluster_memory_usage_list = cluster_memory_usage["items"][0][
                    "timeSeries"
                ][0]["data"]
                cluster_memory_usage_df = pd.DataFrame(cluster_memory_usage_list)
                cluster_memory_usage_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            cluster_memory_usage_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": cluster_memory_usage_df["value"],
                    }
                )
                cluster_memory_usage_df["DateTime"] = pd.to_datetime(
                    cluster_memory_usage_df["DateTime"]
                )
                cluster_memory_usage_avg = (
                    cluster_memory_usage_df["Mean"].sum()
                    / cluster_memory_usage_df["DateTime"].count()
                )
                cluster_memory_usage_df = (
                    pd.DataFrame(
                        pd.date_range(
                            cluster_memory_usage_df["DateTime"].min(),
                            cluster_memory_usage_df["DateTime"].max(),
                            freq="H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(cluster_memory_usage_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                cluster_memory_usage_df[
                    "Time"
                ] = cluster_memory_usage_df.DateTime.dt.strftime("%d-%b %H:%M")
                cluster_memory_usage_df = cluster_memory_usage_df.set_index("Time")
                self.logger.info("clusterMemoryUsage successful")
                return cluster_memory_usage_df, cluster_memory_usage_avg
            else:
                self.logger.error(
                    "clusterMemoryUsage failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
        except Exception as e:
            self.logger.error("clusterMemoryUsage failed", exc_info=True)
            return None
