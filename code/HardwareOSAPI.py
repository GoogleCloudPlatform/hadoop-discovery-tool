# ------------------------------------------------------------------------------
# This module contains all the features of the category Hardware and Operating
# System footprint.This module contains the actual logic built with the help of
# Cloudera Manager API, Generic API and commands.
# -------------------------------------------------------------------------------

# Importing required libraries
from imports import *


class HardwareOSAPI:
    """This Class has functions related to Hardware and OS Footprint category.

    Has functions which fetch different hardware and os metrics from Hadoop 
    cluster like list of host and their details, list of services, core and 
    memory usage pattern over time, etc.

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

    def osVersion(self):
        """Get OS version using system CLI command.
        
        Returns:
            os_version (str): OS version and distribution of host
        """

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

    def clusterItems(self):
        """Get list of all clusters present in Cloudera Manager.

        Returns:
            cluster_items (dict): Metrics of all clusters
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/clusters".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "{}://{}:{}/api/v19/clusters".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "{}://{}:{}/api/v19/clusters".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
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

    def clusterHostItems(self, cluster_name):
        """Get List of all hosts present in a cluster.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            cluster_host_items (dict): Summary of all hosts in cluster
            cluster_host_len (int): Number of hosts in cluster
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/clusters/{}/hosts".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        cluster_name,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "{}://{}:{}/api/v19/clusters/{}/hosts".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        cluster_name,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "{}://{}:{}/api/v19/clusters/{}/hosts".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        cluster_name,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            if r.status_code == 200:
                cluster_host = r.json()
                cluster_host_len = len(cluster_host["items"])
                with open(
                    "Discovery_Report/{}/clusters_host.json".format(cluster_name), "w"
                ) as fp:
                    json.dump(cluster_host, fp, indent=4)
                cluster_host_items = cluster_host["items"]
                self.logger.info("clusterHostItems successful")
                return cluster_host_items, cluster_host_len
            else:
                self.logger.error(
                    "clusterHostItems failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
        except Exception as e:
            self.logger.error("clusterHostItems failed", exc_info=True)
            return None

    def clusterServiceItem(self, cluster_name):
        """Get a list of services present in a cluster with its details.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            cluster_service_item (dict): All services installed in cluster
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/clusters/{}/services".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        cluster_name,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "{}://{}:{}/api/v19/clusters/{}/services".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        cluster_name,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "{}://{}:{}/api/v19/clusters/{}/services".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        cluster_name,
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

    def hostData(self, hostId):
        """Get detailed specs of a host.

        Args:
            hostId (str): Host ID present in cloudera manager.
        Returns:
            host_data (dict): Detailed specs of host
        """

        try:
            hostid = hostId
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/hosts/{}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        hostid,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "{}://{}:{}/api/v19/hosts/{}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        hostid,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "{}://{}:{}/api/v19/hosts/{}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        hostid,
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

    def clusterTotalCores(self, cluster_name):
        """Get cores availability data over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            cluster_total_cores_df (DataFrame): Total cores available over time.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_cores_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        self.start_date,
                        cluster_name,
                        self.end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_cores_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        self.start_date,
                        cluster_name,
                        self.end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_cores_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        self.start_date,
                        cluster_name,
                        self.end_date,
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

    def clusterCpuUsage(self, cluster_name):
        """Get cores usage data over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            cluster_cpu_usage_df (DataFrame): CPU usage over time
            cluster_cpu_usage_avg (float): Average CPU usage in cluster
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20cpu_percent_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        self.start_date,
                        cluster_name,
                        self.end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20cpu_percent_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        self.start_date,
                        cluster_name,
                        self.end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20cpu_percent_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        self.start_date,
                        cluster_name,
                        self.end_date,
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

    def clusterTotalMemory(self, cluster_name):
        """Get memory availability data over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            cluster_total_memory_df (DataFrame): Total memory available over time
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        self.start_date,
                        cluster_name,
                        self.end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        self.start_date,
                        cluster_name,
                        self.end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        self.start_date,
                        cluster_name,
                        self.end_date,
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

    def clusterMemoryUsage(self, cluster_name):
        """Get memory usage data over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            cluster_memory_usage_df (DataFrame): Memory usage over time
            cluster_memory_usage_avg (float): Average memory usage in cluster
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20100*total_physical_memory_used_across_hosts/total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        self.start_date,
                        cluster_name,
                        self.end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20100*total_physical_memory_used_across_hosts/total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        self.start_date,
                        cluster_name,
                        self.end_date,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20100*total_physical_memory_used_across_hosts/total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                        self.start_date,
                        cluster_name,
                        self.end_date,
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

    def dataBaseServer(self):
        """Get database server like mysql for metadata.

        Returns:
            database_server (str): Database server present in cluster.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/cm/scmDbInfo".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "{}://{}:{}/api/v19/cm/scmDbInfo".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "{}://{}:{}/api/v19/cm/scmDbInfo".format(
                        self.http,
                        self.cloudera_manager_host_ip,
                        self.cloudera_manager_port,
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            if r.status_code == 200:
                database_server = r.json()
                database_server = database_server["scmDbType"]
                self.logger.info("dataBaseServer successful")
                return database_server
            else:
                self.logger.error(
                    "dataBaseServer failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
        except Exception as e:
            self.logger.error("dataBaseServer failed", exc_info=True)
            return None

    def dnsServer(self):
        """Get DNS server details.

        Returns:
            dns_server (str): DNS server present in cluster.
        """

        try:
            dns_server = os.popen("systemctl status named | grep Active").read()
            if not dns_server:
                dns_server = "DNS server does not enabled within machine"
            else:
                dns_server = "DNS server not enabled within machine"
            self.logger.info("dnsServer successful")
            return dns_server
        except Exception as e:
            self.logger.error("dnsServer failed", exc_info=True)
            return None

    def webServer(self):
        """Get web server details.

        Returns:
            web_server (str): Web server present in cluster.
        """

        try:
            web_server = os.popen("systemctl status httpd | grep Active").read()
            web_server = web_server.split(":")
            if "inactive" in web_server[1]:
                web_server = "Web server is not enabled"
            else:
                web_server = "Web server is enabled"
            self.logger.info("webServer successful")
            return web_server
        except Exception as e:
            self.logger.error("webServer failed", exc_info=True)
            return None

    def ntpServer(self):
        """Get NTP server details.

        Returns:
            ntp_server (str): NTP server present in cluster.
        """

        try:
            ntp_server = os.popen("timedatectl status | grep NTP | grep enabled").read()
            ntp_server = ntp_server.split(":")
            ntp_server = ntp_server[1]
            self.logger.info("ntpServer successful")
            return ntp_server
        except Exception as e:
            self.logger.error("ntpServer failed", exc_info=True)
            return None

    def manufacturerName(self):
        """Get manufacturer name of processor.

        Returns:
            manufacturer_name (str): Manufacturer name of processor present in cluster.
        """

        try:
            manufacturer_name = os.popen(
                "dmidecode --type processor | grep Manufacturer | awk 'FNR <= 1'"
            ).read()
            manufacturer_name = manufacturer_name.split(":")
            manufacturer_name = manufacturer_name[1]
            self.logger.info("manufacturerName successful")
            return manufacturer_name
        except Exception as e:
            self.logger.error("manufacturerName failed", exc_info=True)
            return None

    def serialNo(self):
        """Get serial number of processor.

        Returns:
            serial_no (str): Serial number of processor present in cluster.
        """

        try:
            serial_no = os.popen(
                "dmidecode --type processor | grep ID | awk 'FNR <= 1'"
            ).read()
            serial_no = serial_no.split(":")
            serial_no = serial_no[1]
            self.logger.info("serialNo successful")
            return serial_no
        except Exception as e:
            self.logger.error("serialNo failed", exc_info=True)
            return None

    def family(self):
        """Get family of processor.

        Returns:
            family (str): Family of processor present in cluster.
        """

        try:
            family = os.popen(
                "cat /proc/cpuinfo | grep cpu | grep family | awk 'FNR <= 1'"
            ).read()
            family = family.split(":")
            family = family[1]
            self.logger.info("family successful")
            return family
        except Exception as e:
            self.logger.error("family failed", exc_info=True)
            return None

    def modelName(self):
        """Get model name of processor.

        Returns:
            model_name (str): Model name of processor present in cluster.
        """

        try:
            model_name = os.popen(
                "cat /proc/cpuinfo | grep model | grep name | awk 'FNR <= 1'"
            ).read()
            model_name = model_name.split(":")
            model_name = model_name[1]
            self.logger.info("modelName successful")
            return model_name
        except Exception as e:
            self.logger.error("modelName failed", exc_info=True)
            return None

    def microcode(self):
        """Get microcode of processor.

        Returns:
            microcode (str): Microcode of processor present in cluster.
        """

        try:
            microcode = os.popen(
                "cat /proc/cpuinfo | grep microcode | awk 'FNR <= 1'"
            ).read()
            microcode = microcode.split(":")
            microcode = microcode[1]
            self.logger.info("microcode successful")
            return microcode
        except Exception as e:
            self.logger.error("microcode failed", exc_info=True)
            return None

    def cpuMHz(self):
        """Get CPU MHz of processor.

        Returns:
            cpu_mhz (str): CPU MHz of processor present in cluster.
        """

        try:
            cpu_mhz = os.popen(
                "cat /proc/cpuinfo | grep cpu |grep MHz| awk 'FNR <= 1'"
            ).read()
            cpu_mhz = cpu_mhz.split(":")
            cpu_mhz = cpu_mhz[1]
            self.logger.info("cpuMHz successful")
            return cpu_mhz
        except Exception as e:
            self.logger.error("cpuMHz failed", exc_info=True)
            return None

    def cpuFamily(self):
        """Get CPU family of processor.

        Returns:
            cpu_family (str): CPU family of processor present in cluster.
        """

        try:
            cpu_family = os.popen(
                "cat /proc/cpuinfo | grep cpu | grep family | awk 'FNR <= 1'"
            ).read()
            cpu_family = cpu_family.split(":")
            cpu_family = cpu_family[1]
            self.logger.info("cpuFamily successful")
            return cpu_family
        except Exception as e:
            self.logger.error("cpuFamily failed", exc_info=True)
            return None

    def networkInterfaceDetails(self):
        """Get NIC details for cluster hardware.

        Returns:
            nic_details (str): NIC details
        """

        try:
            subprocess.getoutput('ip -o -4 a show | cut -d " " -f 2,7 > nic_ip.txt')
            fin = open("nic_ip.txt", "rt")
            fout = open("nic_ip.csv", "wt")
            for iterator in fin:
                fout.write(re.sub("[^\S\r\n]{1,}", ",", iterator))
            fin.close()
            fout.close()
            column_names = ["nic", "ipv4"]
            nic_details = pd.read_csv("nic_ip.csv", names=column_names, header=None)
            os.popen("rm nic_ip.csv")
            os.popen("rm nic_ip.txt")
            delete_row = nic_details[nic_details["nic"] == "lo"].index
            nic_details = nic_details.drop(delete_row)
            self.logger.info("networkInterfaceDetails successful")
            return nic_details
        except Exception as e:
            self.logger.error("networkInterfaceDetails failed", exc_info=True)
            return None

    def appliedPatches(self):
        """Get List of security patches present in cluster.

        Returns:
            patch_dataframe (DataFrame): List of security patches.
            os_name (str): OS distribution
        """

        try:
            os_name = os.popen("grep PRETTY_NAME /etc/os-release").read()
            os_name = os_name.lower()
            if "centos" or "red hat" in os_name:
                subprocess.getoutput(
                    "sudo yum updateinfo list security installed | grep /Sec > security_level.csv"
                )
                fin = open("security_level.csv", "rt")
                fout = open("security_final.csv", "wt")
                for iterator in fin:
                    fout.write(re.sub("[^\S\r\n]{1,}", ",", iterator))
                fin.close()
                fout.close()
                column_names = ["Advisory_Name", "Severity", "Security_Package"]
                security_df = pd.read_csv(
                    "security_final.csv", names=column_names, header=None
                )
                os.popen("rm security_level.csv")
                os.popen("rm security_final.csv")
                subprocess.check_output(
                    "bash YUM_Get_Patch_Date.sh", shell=True, stderr=subprocess.STDOUT
                )
                fin = open("patch_date.csv", "rt")
                fout = open("security_patch_date.csv", "wt")
                for iterator in fin:
                    fout.write(re.sub(r"^([^\s]*)\s+", r"\1, ", iterator))
                fin.close()
                fout.close()
                column_names = ["Security_Package", "Patch_Deployed_Date"]
                patch_date_df = pd.read_csv(
                    "security_patch_date.csv", names=column_names, header=None
                )
                patch_date_df["Patch_Deployed_Date"] = pd.to_datetime(
                    patch_date_df.Patch_Deployed_Date
                )
                patch_date_df["Patch_Deployed_Date"] = patch_date_df[
                    "Patch_Deployed_Date"
                ].dt.strftime("%d-%b-%Y")
                os.popen("rm patch_date.csv")
                os.popen("rm security_patch_date.csv")
                patch_dataframe = pd.merge(
                    security_df, patch_date_df, on="Security_Package", how="inner"
                )
            elif "debian" or "ubuntu" in os_name:
                os.popen(
                    "sudo apt-show-versions | grep security | grep all | sort -u | head -10 > output.csv"
                ).read()
                fin = open("output.csv", "rt")
                fout = open("ubuntu_patches.csv", "wt")
                for iterator in fin:
                    fout.write(re.sub("[^\S\r\n]{1,}", ",", iterator))
                fin.close()
                fout.close()
                column_names = ["Security_Package", "Patch_Version", "Update_Status"]
                patch_data = pd.read_csv(
                    "ubuntu_patches.csv", names=column_names, header=None
                )
                patch_data = patch_data["Security_Package"].str.split(":").str[0]
                patch_dataframe = patch_data.to_frame()
                os.popen("rm output.csv")
                os.popen("rm ubuntu_patches.csv")
            else:
                patch_dataframe = pd.DataFrame(
                    ["Operating System is Not Supported"], columns=["Supported_Status"]
                )
            self.logger.info("appliedPatches successful")
            return patch_dataframe, os_name
        except Exception as e:
            self.logger.error("appliedPatches failed", exc_info=True)
            return None

    def listHadoopNonHadoopLibs(self):
        """Get List of hadoop and non-hadoop libraries present in cluster.

        Returns:
            hadoop_native_df (DataFrame): List of hadoop and non-hadoop libraries.
        """

        try:
            os.popen(
                "hadoop checknative -a | grep true | head -10 > hadoop_native.csv"
            ).read()
            fin = open("hadoop_native.csv", "rt")
            fout = open("hadoop_native_library.csv", "wt")
            for iterator in fin:
                fout.write(re.sub("[^\S\r\n]{1,}", ",", iterator))
            fin.close()
            fout.close()
            column_names = ["Hadoop_Libraries", "Status", "Library_Path"]
            hadoop_native_df = pd.read_csv(
                "hadoop_native_library.csv", names=column_names, header=None
            )
            os.popen("rm hadoop_native_library.csv")
            os.popen("rm hadoop_native.csv")
            hadoop_native_df["Hadoop_Libraries"] = hadoop_native_df[
                "Hadoop_Libraries"
            ].str.rstrip(":")
            hadoop_native_df.drop(["Status", "Library_Path"], axis=1, inplace=True)
            os.popen("ls /usr/local/lib/ | tr -d ' ' | head -10 > user_libs.csv").read()
            column_names = ["Non_Hadoop_Libraries"]
            custom_lib = pd.read_csv("user_libs.csv", names=column_names, header=None)
            os.popen("rm user_libs.csv")
            hadoop_native_df["Non_Hadoop_Libraries"] = custom_lib[
                "Non_Hadoop_Libraries"
            ]
            hadoop_native_df["Non_Hadoop_Libraries"] = hadoop_native_df[
                "Non_Hadoop_Libraries"
            ].fillna("")
            self.logger.info("listHadoopNonHadoopLibs successful")
            return hadoop_native_df
        except Exception as e:
            self.logger.error("listHadoopNonHadoopLibs failed", exc_info=True)
            return None

    def checkLibrariesInstalled(self):
        """Get check whether python, java and scala are installed in cluster.

        Returns:
            python_flag (int): Check for python installation.
            java_flag (int): Check for java installation.
            scala_flag (int): Check for scala installation.
        """

        try:
            python_flag, java_flag, scala_flag = 0, 0, 0
            jdk_line, scala_line = "", ""
            python_check = os.popen("python3 --version").read()
            os_name = os.popen("grep PRETTY_NAME /etc/os-release").read()
            os_name = os_name.lower()
            if "centos" in os_name:
                softwares_installed = os.popen(
                    "rpm -qa | grep java > java_check.csv"
                ).read()
            elif "debian" in os_name:
                softwares_installed = os.popen(
                    "dpkg -l | grep java > java_check.csv"
                ).read()
            elif "ubuntu" in os_name:
                softwares_installed = os.popen(
                    "apt list --installed | grep java > java_check.csv"
                ).read()
            elif "red hat" in os_name:
                softwares_installed = os.popen(
                    "rpm -qa | grep java > java_check.csv"
                ).read()
            elif "suse" in os_name:
                softwares_installed = os.popen(
                    "rpm -qa | grep java > java_check.csv"
                ).read()
            if "Python 3." in python_check:
                python_flag = 1
            with open("java_check.csv") as fp:
                for jdk_line in fp:
                    if "openjdk" in jdk_line:
                        java_flag = 1
                        break
            os.popen("rm java_check.csv").read()
            os.popen("timeout -k 21 20 spark-shell > scala.csv").read()
            with open("scala.csv") as fp:
                for scala_line in fp:
                    if "Using Scala version" in scala_line:
                        scala_flag = 1
                        break
            os.popen("rm scala.csv").read()
            self.logger.info("checkLibrariesInstalled successful")
            return python_flag, java_flag, scala_flag
        except Exception as e:
            self.logger.error("checkLibrariesInstalled failed", exc_info=True)
            return None

    def securitySoftware(self):
        """Get list of security software present in cluster.

        Returns:
            security_software (dict): List of security software.
        """

        try:
            cyberSecurity = os.popen(
                'find / -type f \( -iname "knox-server" -o -iname "ranger-admin" -o -iname "grr" -o -iname "splunk" -o -iname "MISP" -o -iname "TheHive-Project" -o -iname "nagios.cfg" \)'
            ).read()
            Cloudera_navigator = subprocess.Popen(
                "ls /*/*/*/webapp/static/release/js/cloudera/navigator",
                shell=True,
                stdout=subprocess.PIPE,
                encoding="utf-8",
            )
            out, err = Cloudera_navigator.communicate()
            security_software = {}
            if cyberSecurity.find("ranger-admin") == -1:
                security_software["ranger"] = "Ranger is not installed"
            else:
                security_software["ranger"] = "Ranger is installed"
            if cyberSecurity.find("knox-server") == -1:
                security_software["knox"] = "Knox-server is not installed"
            else:
                security_software["knox"] = "Knox-server is installed"
            if cyberSecurity.find("splunk") == -1:
                security_software["splunk"] = "Splunk is not installed"
            else:
                security_software["splunk"] = "Splunk is installed"
            if cyberSecurity.find("nagios") == -1:
                security_software["nagios"] = "Nagios is not installed"
            else:
                security_software["nagios"] = "Nagios is installed"
            if cyberSecurity.find("GRR") == -1:
                security_software["grr"] = "GRR Rapid responce is not installed"
            else:
                security_software["grr"] = "GRR Rapid responce is installed"
            if cyberSecurity.find("MISP") == -1:
                security_software["misp"] = "MISP is not installed"
            else:
                security_software["misp"] = "MISP is installed"
            if cyberSecurity.find("thehive") == -1:
                security_software["thehive"] = "TheHive is not installed"
            else:
                security_software["thehive"] = "TheHive is installed"
            osquery = os.popen("yum list installed osquery | grep osquery").read()
            if not osquery:
                security_software["osquery"] = "OSQuery is not installed"
            else:
                security_software["osquery"] = "OSQuery is installed"
            if not out:
                security_software[
                    "cloudera_navigator"
                ] = "Cloudera Navigator is not installed"
            else:
                security_software[
                    "cloudera_navigator"
                ] = "Cloudera Navigator is installed"
            self.logger.info("securitySoftware successful")
            return security_software
        except Exception as e:
            self.logger.error("securitySoftware failed", exc_info=True)
            return None

    def specialityHardware(self):
        """Get check whether GPU is present in cluster.

        Returns:
            gpu_status (str): Check for GPU.
        """

        try:
            gpu_status = subprocess.getoutput('lshw | egrep -i -c "non-vga"')
            self.logger.info("specialityHardware successful")
            return gpu_status
        except Exception as e:
            self.logger.error("specialityHardware failed", exc_info=True)
            return None
