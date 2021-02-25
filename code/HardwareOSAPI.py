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
                    "{}://{}:{}/api/v33/clusters".format(
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
                    "{}://{}:{}/api/v33/clusters/{}/hosts".format(
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
                    "{}://{}:{}/api/v33/clusters/{}/services".format(
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
                    "{}://{}:{}/api/v33/hosts/{}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_cores_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20cpu_percent_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20100*total_physical_memory_used_across_hosts/total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}".format(
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
