# ------------------------------------------------------------------------------
# This module contains the logic to retrieve information of the features of the
# category application. This module also contains the logic related to YARN
# Resource manager API and other Generic features.
# ------------------------------------------------------------------------------

# Importing required libraries
from imports import *


class ApplicationAPI:
    """This Class has functions related to Application category.

    Has functions which fetch different application metrics from Hadoop cluster 
    like yarn, Hbase, Kafka, etc.

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

    def getApplicationDetails(self, yarn_rm, yarn_port):
        """Get list of all yarn related application over a date range.

        Args:
            yarn_rm (str): Yarn resource manager IP.
        Returns:
            yarn_application_df (DataFrame): List of yarn related application in cluster.
        """

        try:
            r = requests.get(
                "{}://{}:{}/ws/v1/cluster/apps".format(self.http, yarn_rm, yarn_port)
            )
            if r.status_code == 200:
                yarn_application = r.json()
                yarn_application_list = yarn_application["apps"]["app"]
                yarn_application_df = pd.DataFrame(yarn_application_list)
                if self.version == 7:
                    yarn_application_df = pd.DataFrame(
                        {
                            "ApplicationId": yarn_application_df["id"],
                            "ApplicationType": yarn_application_df["applicationType"],
                            "LaunchTime": pd.to_datetime(
                                (yarn_application_df["launchTime"] + 500) / 1000,
                                unit="s",
                            ),
                            "StartedTime": pd.to_datetime(
                                (yarn_application_df["startedTime"] + 500) / 1000,
                                unit="s",
                            ),
                            "FinishedTime": pd.to_datetime(
                                (yarn_application_df["finishedTime"] + 500) / 1000,
                                unit="s",
                            ),
                            "ElapsedTime": (yarn_application_df["elapsedTime"] + 500)
                            / 1000,
                            "FinalStatus": yarn_application_df["finalStatus"],
                            "MemorySeconds": yarn_application_df["memorySeconds"],
                            "VcoreSeconds": yarn_application_df["vcoreSeconds"],
                            "User": yarn_application_df["user"],
                            "Diagnostics": yarn_application_df["diagnostics"],
                            "Queue": yarn_application_df["queue"],
                            "Name": yarn_application_df["name"],
                        }
                    )
                    yarn_application_df = yarn_application_df[
                        (yarn_application_df["StartedTime"] < (self.end_date))
                        & (yarn_application_df["StartedTime"] >= (self.start_date))
                        & (yarn_application_df["LaunchTime"] >= (self.start_date))
                        & (yarn_application_df["FinishedTime"] >= (self.start_date))
                    ]
                elif self.version == 6:
                    yarn_application_df = pd.DataFrame(
                        {
                            "ApplicationId": yarn_application_df["id"],
                            "ApplicationType": yarn_application_df["applicationType"],
                            "LaunchTime": pd.to_datetime(
                                (yarn_application_df["launchTime"] + 500) / 1000,
                                unit="s",
                            ),
                            "StartedTime": pd.to_datetime(
                                (yarn_application_df["startedTime"] + 500) / 1000,
                                unit="s",
                            ),
                            "FinishedTime": pd.to_datetime(
                                (yarn_application_df["finishedTime"] + 500) / 1000,
                                unit="s",
                            ),
                            "ElapsedTime": (yarn_application_df["elapsedTime"] + 500)
                            / 1000,
                            "FinalStatus": yarn_application_df["finalStatus"],
                            "MemorySeconds": yarn_application_df["memorySeconds"],
                            "VcoreSeconds": yarn_application_df["vcoreSeconds"],
                            "User": yarn_application_df["user"],
                            "Diagnostics": yarn_application_df["diagnostics"],
                            "Queue": yarn_application_df["queue"],
                            "Name": yarn_application_df["name"],
                        }
                    )
                    yarn_application_df = yarn_application_df[
                        (yarn_application_df["StartedTime"] < (self.end_date))
                        & (yarn_application_df["StartedTime"] >= (self.start_date))
                        & (yarn_application_df["LaunchTime"] >= (self.start_date))
                        & (yarn_application_df["FinishedTime"] >= (self.start_date))
                    ]
                elif self.version == 5:
                    yarn_application_df = pd.DataFrame(
                        {
                            "ApplicationId": yarn_application_df["id"],
                            "ApplicationType": yarn_application_df["applicationType"],
                            "LaunchTime": pd.to_datetime(
                                (yarn_application_df["startedTime"] + 500) / 1000,
                                unit="s",
                            ),
                            "StartedTime": pd.to_datetime(
                                (yarn_application_df["startedTime"] + 500) / 1000,
                                unit="s",
                            ),
                            "FinishedTime": pd.to_datetime(
                                (yarn_application_df["finishedTime"] + 500) / 1000,
                                unit="s",
                            ),
                            "ElapsedTime": (yarn_application_df["elapsedTime"] + 500)
                            / 1000,
                            "FinalStatus": yarn_application_df["finalStatus"],
                            "MemorySeconds": yarn_application_df["memorySeconds"],
                            "VcoreSeconds": yarn_application_df["vcoreSeconds"],
                            "User": yarn_application_df["user"],
                            "Diagnostics": yarn_application_df["diagnostics"],
                            "Queue": yarn_application_df["queue"],
                            "Name": yarn_application_df["name"],
                        }
                    )
                    yarn_application_df = yarn_application_df[
                        (yarn_application_df["StartedTime"] < (self.end_date))
                        & (yarn_application_df["StartedTime"] >= (self.start_date))
                        & (yarn_application_df["FinishedTime"] >= (self.start_date))
                    ]
                yarn_application_df = yarn_application_df.reset_index(drop=True)
                self.logger.info("getApplicationDetails successful")
                return yarn_application_df
            else:
                self.logger.error(
                    "getApplicationDetails failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getApplicationDetails failed", exc_info=True)
            return None

    def getApplicationTypeStatusCount(self, yarn_application_df):
        """Get yarn related application count based to its type and status.

        Args:
            yarn_application_df (DataFrame): List of yarn related application in cluster.
        Returns:
            app_count_df (DataFrame): Application count in yarn.
            app_type_count_df (DataFrame): Application count by type in yarn.
            app_status_count_df (DataFrame): Application count by status in yarn.
        """

        try:
            app_count_df = pd.DataFrame(
                {
                    "Application Type": yarn_application_df["ApplicationType"],
                    "Status": yarn_application_df["FinalStatus"],
                    "Count": 1,
                }
            )
            app_count_df = app_count_df.groupby(["Application Type", "Status"]).sum()
            app_count_df.index = app_count_df.index.set_names(
                ["Application Type", "Status"]
            )
            app_count_df.reset_index(inplace=True)
            app_type_count_df = app_count_df[["Application Type", "Count"]]
            app_type_count_df = app_type_count_df.groupby(["Application Type"]).sum()
            app_status_count_df = app_count_df[["Status", "Count"]]
            app_status_count_df = app_status_count_df.groupby(["Status"]).sum()
            self.logger.info("getApplicationTypeStatusCount successful")
            return app_count_df, app_type_count_df, app_status_count_df
        except Exception as e:
            self.logger.error("getApplicationTypeStatusCount failed", exc_info=True)
            return None

    def getApplicationVcoreMemoryUsage(self, yarn_application_df):
        """Get vcore and memory usage of yarn application.

        Args:
            yarn_application_df (DataFrame): List of yarn application in cluster.
        Returns:
            app_vcore_df (DataFrame): Vcores usage by applications
            app_memory_df (DataFrame): Memory usage by applications
        """

        try:
            app_vcore_df = pd.DataFrame(
                {
                    "Application Type": yarn_application_df["ApplicationType"],
                    "Vcore": yarn_application_df["VcoreSeconds"],
                }
            )
            app_vcore_df = app_vcore_df.groupby(["Application Type"]).sum()
            app_memory_df = pd.DataFrame(
                {
                    "Application Type": yarn_application_df["ApplicationType"],
                    "Memory": yarn_application_df["MemorySeconds"],
                }
            )
            app_memory_df = app_memory_df.groupby(["Application Type"]).sum()
            self.logger.info("getApplicationVcoreMemoryUsage successful")
            return app_vcore_df, app_memory_df
        except Exception as e:
            self.logger.error("getApplicationVcoreMemoryUsage failed", exc_info=True)
            return None

    def getBurstyApplicationDetails(self, yarn_application_df):
        """Get details about busrty yarn application.

        Args:
            yarn_application_df (DataFrame): List of yarn application in cluster.
        Returns:
            bursty_app_time_df (DataFrame): Time taken by bursty application.
            bursty_app_vcore_df (DataFrame): Vcores taken by bursty application.
            bursty_app_mem_df (DataFrame): Memory taken by bursty application.
        """

        try:
            bursty_app_time_df = pd.DataFrame(
                columns=["Application Name", "Min", "Mean", "Max"]
            )
            bursty_app_vcore_df = pd.DataFrame(
                columns=["Application Name", "Min", "Mean", "Max"]
            )
            bursty_app_mem_df = pd.DataFrame(
                columns=["Application Name", "Min", "Mean", "Max"]
            )
            count = 0
            for apps in yarn_application_df["Name"].unique():
                if apps == "Spark shell":
                    continue
                bursty_app_check = yarn_application_df[
                    yarn_application_df["Name"] == apps
                ]
                min_time = bursty_app_check["ElapsedTime"].min()
                max_time = bursty_app_check["ElapsedTime"].max()
                if (max_time / min_time) > 15.0:
                    count = count + 1
                    bursty_app_time_tmp_df = pd.DataFrame(
                        {
                            "Application Name": apps,
                            "Min": min_time,
                            "Mean": bursty_app_check["ElapsedTime"].mean(),
                            "Max": max_time,
                        },
                        index=[count],
                    )
                    bursty_app_vcore_tmp_df = pd.DataFrame(
                        {
                            "Application Name": apps,
                            "Min": bursty_app_check[
                                bursty_app_check["ElapsedTime"] == min_time
                            ]["VcoreSeconds"].iloc[0],
                            "Mean": bursty_app_check["VcoreSeconds"].mean(),
                            "Max": bursty_app_check[
                                bursty_app_check["ElapsedTime"] == max_time
                            ]["VcoreSeconds"].iloc[0],
                        },
                        index=[count],
                    )
                    bursty_app_mem_tmp_df = pd.DataFrame(
                        {
                            "Application Name": apps,
                            "Min": bursty_app_check[
                                bursty_app_check["ElapsedTime"] == min_time
                            ]["MemorySeconds"].iloc[0],
                            "Mean": bursty_app_check["MemorySeconds"].mean(),
                            "Max": bursty_app_check[
                                bursty_app_check["ElapsedTime"] == max_time
                            ]["MemorySeconds"].iloc[0],
                        },
                        index=[count],
                    )
                    bursty_app_time_df = bursty_app_time_df.append(
                        bursty_app_time_tmp_df
                    )
                    bursty_app_mem_df = bursty_app_mem_df.append(bursty_app_mem_tmp_df)
                    bursty_app_vcore_df = bursty_app_vcore_df.append(
                        bursty_app_vcore_tmp_df
                    )
            self.logger.info("getBurstyApplicationDetails successful")
            return bursty_app_time_df, bursty_app_vcore_df, bursty_app_mem_df
        except Exception as e:
            self.logger.error("getBurstyApplicationDetails failed", exc_info=True)
            return None

    def getFailedApplicationDetails(self, yarn_application_df):
        """Get details about failed or killed yarn application.

        Args:
            yarn_application_df (DataFrame): List of yarn application in cluster.
        Returns:
            yarn_failed_app (DataFrame): RCA of failed or killed application.
        """

        try:
            yarn_failed_app = yarn_application_df[
                (yarn_application_df["FinalStatus"] == "KILLED")
                | (yarn_application_df["FinalStatus"] == "FAILED")
            ].sort_values(by="ElapsedTime", ascending=False)
            self.logger.info("getFailedApplicationDetails successful")
            return yarn_failed_app
        except Exception as e:
            self.logger.error("getFailedApplicationDetails failed", exc_info=True)
            return None

    def getYarnTotalVcore(self, yarn_rm, yarn_port):
        """Get total vcores allocated to yarn.

        Args:
            yarn_rm (str): Yarn resource manager IP.
        Returns:
            yarn_total_vcores_count (float): Total vcores configured to yarn
        """

        try:
            r = requests.get(
                "{}://{}:{}/ws/v1/cluster/metrics".format(self.http, yarn_rm, yarn_port)
            )
            if r.status_code == 200:
                yarn_total_vcores = r.json()
                yarn_total_vcores_count = math.ceil(
                    yarn_total_vcores["clusterMetrics"]["totalVirtualCores"]
                )
                self.logger.info("getYarnTotalVcore successful")
                return yarn_total_vcores_count
            else:
                self.logger.error(
                    "getYarnTotalVcore failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
        except Exception as e:
            self.logger.error("getYarnTotalVcore failed", exc_info=True)
            return None

    def getYarnVcoreAvailable(self, cluster_name):
        """Get yarn vcore availability data over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            yarn_vcore_available_df (DataFrame): Vcores available over time.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_available_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_available_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_available_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                yarn_vcore_available = r.json()
                yarn_vcore_available_list = yarn_vcore_available["items"][0][
                    "timeSeries"
                ][0]["data"]
                yarn_vcore_available_df = pd.DataFrame(yarn_vcore_available_list)
                yarn_vcore_available_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            yarn_vcore_available_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": yarn_vcore_available_df["value"],
                        "Min": yarn_vcore_available_df["aggregateStatistics"].apply(
                            pd.Series
                        )["min"],
                        "Max": yarn_vcore_available_df["aggregateStatistics"].apply(
                            pd.Series
                        )["max"],
                    }
                )
                yarn_vcore_available_df["DateTime"] = pd.to_datetime(
                    yarn_vcore_available_df["DateTime"]
                )
                yarn_vcore_available_df = (
                    pd.DataFrame(
                        pd.date_range(
                            yarn_vcore_available_df["DateTime"].min(),
                            yarn_vcore_available_df["DateTime"].max(),
                            freq="H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(yarn_vcore_available_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                yarn_vcore_available_df[
                    "Time"
                ] = yarn_vcore_available_df.DateTime.dt.strftime("%d-%b %H:%M")
                yarn_vcore_available_df = yarn_vcore_available_df.set_index("Time")
                self.logger.info("getYarnVcoreAvailable successful")
                return yarn_vcore_available_df
            else:
                self.logger.error(
                    "getYarnVcoreAvailable failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getYarnVcoreAvailable failed", exc_info=True)
            return None

    def getYarnVcoreAllocated(self, cluster_name):
        """Get yarn vcore allocation data over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            yarn_vcore_allocated_avg (float): Average vcores allocated in cluster.
            yarn_vcore_allocated_df (DataFrame): Vcores allocation over time.
            yarn_vcore_allocated_pivot_df (DataFrame): Seasonality of vcores allocation over time.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_allocated_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_allocated_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_allocated_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                yarn_vcore_allocated = r.json()
                yarn_vcore_allocated_list = yarn_vcore_allocated["items"][0][
                    "timeSeries"
                ][0]["data"]
                yarn_vcore_allocated_df = pd.DataFrame(yarn_vcore_allocated_list)
                yarn_vcore_allocated_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            yarn_vcore_allocated_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": yarn_vcore_allocated_df["value"],
                        "Min": yarn_vcore_allocated_df["aggregateStatistics"].apply(
                            pd.Series
                        )["min"],
                        "Max": yarn_vcore_allocated_df["aggregateStatistics"].apply(
                            pd.Series
                        )["max"],
                    }
                )
                yarn_vcore_allocated_df["DateTime"] = pd.to_datetime(
                    yarn_vcore_allocated_df["DateTime"]
                )
                yarn_vcore_allocated_avg = (
                    yarn_vcore_allocated_df["Mean"].sum()
                    / yarn_vcore_allocated_df["DateTime"].count()
                )
                yarn_vcore_allocated_df = (
                    pd.DataFrame(
                        pd.date_range(
                            yarn_vcore_allocated_df["DateTime"].min(),
                            yarn_vcore_allocated_df["DateTime"].max(),
                            freq="H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(yarn_vcore_allocated_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                yarn_vcore_allocated_df[
                    "Time"
                ] = yarn_vcore_allocated_df.DateTime.dt.strftime("%d-%b %H:%M")
                yarn_vcore_allocated_df = yarn_vcore_allocated_df.set_index("Time")
                yarn_vcore_allocated_pivot_df = yarn_vcore_allocated_df[
                    ["DateTime", "Mean"]
                ]
                yarn_vcore_allocated_pivot_df = yarn_vcore_allocated_pivot_df.reset_index(
                    drop=True
                )
                yarn_vcore_allocated_pivot_df["Day"] = yarn_vcore_allocated_pivot_df[
                    "DateTime"
                ].dt.strftime("%A")
                yarn_vcore_allocated_pivot_df["Time"] = yarn_vcore_allocated_pivot_df[
                    "DateTime"
                ].dt.strftime("%H:%M")
                yarn_vcore_allocated_pivot_df = yarn_vcore_allocated_pivot_df.drop(
                    ["DateTime"], axis=1
                )
                yarn_vcore_allocated_pivot_df = pd.pivot_table(
                    yarn_vcore_allocated_pivot_df,
                    index="Day",
                    columns="Time",
                    values="Mean",
                )
                yarn_vcore_allocated_pivot_df = yarn_vcore_allocated_pivot_df.fillna(0)
                self.logger.info("getYarnVcoreAllocated successful")
                return (
                    yarn_vcore_allocated_avg,
                    yarn_vcore_allocated_df,
                    yarn_vcore_allocated_pivot_df,
                )
            else:
                self.logger.error(
                    "getYarnVcoreAllocated failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getYarnVcoreAllocated failed", exc_info=True)
            return None

    def getYarnTotalMemory(self, yarn_rm, yarn_port):
        """Get total memory allocated to yarn.

        Args:
            yarn_rm (str): Yarn resource manager IP.
        Returns:
            yarn_total_memory_count (float): Total memory configured to yarn.
        """

        try:
            r = requests.get(
                "{}://{}:{}/ws/v1/cluster/metrics".format(self.http, yarn_rm, yarn_port)
            )
            if r.status_code == 200:
                yarn_total_memory = r.json()
                yarn_total_memory_count = math.ceil(
                    yarn_total_memory["clusterMetrics"]["totalMB"] / 1024
                )
                self.logger.info("getYarnTotalMemory successful")
                return yarn_total_memory_count
            else:
                self.logger.error(
                    "getYarnTotalMemory failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getYarnTotalMemory failed", exc_info=True)
            return None

    def getYarnMemoryAvailable(self, cluster_name):
        """Get yarn memory availability data over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            yarn_memory_available_df (DataFrame): Memory available over time.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_available_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_available_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_available_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                yarn_memory_available = r.json()
                yarn_memory_available_list = yarn_memory_available["items"][0][
                    "timeSeries"
                ][0]["data"]
                yarn_memory_available_df = pd.DataFrame(yarn_memory_available_list)
                yarn_memory_available_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            yarn_memory_available_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": yarn_memory_available_df["value"],
                        "Min": yarn_memory_available_df["aggregateStatistics"].apply(
                            pd.Series
                        )["min"],
                        "Max": yarn_memory_available_df["aggregateStatistics"].apply(
                            pd.Series
                        )["max"],
                    }
                )
                yarn_memory_available_df["DateTime"] = pd.to_datetime(
                    yarn_memory_available_df["DateTime"]
                )
                yarn_memory_available_df = (
                    pd.DataFrame(
                        pd.date_range(
                            yarn_memory_available_df["DateTime"].min(),
                            yarn_memory_available_df["DateTime"].max(),
                            freq="H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(yarn_memory_available_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                yarn_memory_available_df[
                    "Time"
                ] = yarn_memory_available_df.DateTime.dt.strftime("%d-%b %H:%M")
                yarn_memory_available_df = yarn_memory_available_df.set_index("Time")
                self.logger.info("getYarnMemoryAvailable successful")
                return yarn_memory_available_df
            else:
                self.logger.error(
                    "getYarnMemoryAvailable failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getYarnMemoryAvailable failed", exc_info=True)
            return None

    def getYarnMemoryAllocated(self, cluster_name):
        """Get yarn memory allocation data over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            yarn_memory_allocated_avg (float): Average memory allocated in cluster.
            yarn_memory_allocated_df (DataFrame): Memory allocation over time.
            yarn_memory_allocated_pivot_df (DataFrame): Seasonality of memory allocation over time.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_allocated_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_allocated_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_allocated_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                yarn_memory_allocated = r.json()
                yarn_memory_allocated_list = yarn_memory_allocated["items"][0][
                    "timeSeries"
                ][0]["data"]
                yarn_memory_allocated_df = pd.DataFrame(yarn_memory_allocated_list)
                yarn_memory_allocated_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            yarn_memory_allocated_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": yarn_memory_allocated_df["value"],
                        "Min": yarn_memory_allocated_df["aggregateStatistics"].apply(
                            pd.Series
                        )["min"],
                        "Max": yarn_memory_allocated_df["aggregateStatistics"].apply(
                            pd.Series
                        )["max"],
                    }
                )
                yarn_memory_allocated_df["DateTime"] = pd.to_datetime(
                    yarn_memory_allocated_df["DateTime"]
                )
                yarn_memory_allocated_avg = (
                    yarn_memory_allocated_df["Mean"].sum()
                    / yarn_memory_allocated_df["DateTime"].count()
                )
                yarn_memory_allocated_df = (
                    pd.DataFrame(
                        pd.date_range(
                            yarn_memory_allocated_df["DateTime"].min(),
                            yarn_memory_allocated_df["DateTime"].max(),
                            freq="H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(yarn_memory_allocated_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                yarn_memory_allocated_df[
                    "Time"
                ] = yarn_memory_allocated_df.DateTime.dt.strftime("%d-%b %H:%M")
                yarn_memory_allocated_df = yarn_memory_allocated_df.set_index("Time")
                yarn_memory_allocated_pivot_df = yarn_memory_allocated_df[
                    ["DateTime", "Mean"]
                ]
                yarn_memory_allocated_pivot_df = yarn_memory_allocated_pivot_df.reset_index(
                    drop=True
                )
                yarn_memory_allocated_pivot_df["Day"] = yarn_memory_allocated_pivot_df[
                    "DateTime"
                ].dt.strftime("%A")
                yarn_memory_allocated_pivot_df["Time"] = yarn_memory_allocated_pivot_df[
                    "DateTime"
                ].dt.strftime("%H:%M")
                yarn_memory_allocated_pivot_df = yarn_memory_allocated_pivot_df.drop(
                    ["DateTime"], axis=1
                )
                yarn_memory_allocated_pivot_df = pd.pivot_table(
                    yarn_memory_allocated_pivot_df,
                    index="Day",
                    columns="Time",
                    values="Mean",
                )
                yarn_memory_allocated_pivot_df = yarn_memory_allocated_pivot_df.fillna(
                    0
                )
                self.logger.info("getYarnMemoryAllocated successful")
                return (
                    yarn_memory_allocated_avg,
                    yarn_memory_allocated_df,
                    yarn_memory_allocated_pivot_df,
                )
            else:
                self.logger.error(
                    "getYarnMemoryAllocated failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getYarnMemoryAllocated failed", exc_info=True)
            return None

    def getVcoreMemoryByApplication(self, yarn_application_df):
        """Get vcore and memory breakdown by yarn application.

        Args:
            yarn_application_df (DataFrame): List of yarn application in cluster.
        Returns:
            app_vcore_df (DataFrame): Vcore breakdown by application
            app_vcore_usage_df (DataFrame): Vcore usage over time
            app_memory_df (DataFrame): Memory breakdown by application
            app_memory_usage_df (DataFrame): Memory usage over time
        """

        try:
            app_vcore_df = pd.DataFrame(None)
            app_vcore_df = pd.DataFrame(
                {
                    "Application Id": yarn_application_df["ApplicationId"],
                    "Application Type": yarn_application_df["ApplicationType"],
                    "Launch Time": pd.to_datetime(
                        yarn_application_df["LaunchTime"]
                    ).dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Finished Time": pd.to_datetime(
                        yarn_application_df["FinishedTime"]
                    ).dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Vcore": yarn_application_df["VcoreSeconds"]
                    / yarn_application_df["ElapsedTime"],
                    "Vcore Seconds": yarn_application_df["VcoreSeconds"],
                }
            )
            app_vcore_df["Launch Time"] = pd.to_datetime(
                app_vcore_df["Launch Time"], format="%Y-%m-%d"
            )
            app_vcore_df["Finished Time"] = pd.to_datetime(
                app_vcore_df["Finished Time"], format="%Y-%m-%d"
            )
            app_vcore_usage_df = pd.DataFrame(
                pd.date_range(
                    app_vcore_df["Launch Time"].dt.strftime("%Y-%m-%d %H:%M").min(),
                    app_vcore_df["Finished Time"].dt.strftime("%Y-%m-%d %H:%M").max(),
                    freq="H",
                ),
                columns=["Date"],
            )
            app_memory_df = pd.DataFrame(None)
            app_memory_df = pd.DataFrame(
                {
                    "Application Id": yarn_application_df["ApplicationId"],
                    "Application Type": yarn_application_df["ApplicationType"],
                    "Launch Time": pd.to_datetime(
                        yarn_application_df["LaunchTime"]
                    ).dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Finished Time": pd.to_datetime(
                        yarn_application_df["FinishedTime"]
                    ).dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Memory": yarn_application_df["MemorySeconds"]
                    / yarn_application_df["ElapsedTime"],
                    "Memory Seconds": yarn_application_df["MemorySeconds"],
                }
            )
            app_memory_df["Launch Time"] = pd.to_datetime(
                app_memory_df["Launch Time"], format="%Y-%m-%d"
            )
            app_memory_df["Finished Time"] = pd.to_datetime(
                app_memory_df["Finished Time"], format="%Y-%m-%d"
            )
            app_memory_usage_df = pd.DataFrame(
                pd.date_range(
                    app_memory_df["Launch Time"].dt.strftime("%Y-%m-%d %H:%M").min(),
                    app_memory_df["Finished Time"].dt.strftime("%Y-%m-%d %H:%M").max(),
                    freq="H",
                ),
                columns=["Date"],
            )
            self.logger.info("getVcoreMemoryByApplication successful")
            return app_vcore_df, app_vcore_usage_df, app_memory_df, app_memory_usage_df
        except Exception as e:
            self.logger.error("getVcoreMemoryByApplication failed", exc_info=True)
            return None

    def getPendingApplication(self, cluster_name):
        """Get pending application over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            yarn_pending_apps_df (DataFrame): Pending application count over time.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_apps_pending_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_apps_pending_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_apps_pending_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                yarn_pending_apps = r.json()
                yarn_pending_apps_list = yarn_pending_apps["items"][0]["timeSeries"][0][
                    "data"
                ]
                yarn_pending_apps_df = pd.DataFrame(yarn_pending_apps_list)
                yarn_pending_apps_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            yarn_pending_apps_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": yarn_pending_apps_df["value"],
                        "Min": yarn_pending_apps_df["aggregateStatistics"].apply(
                            pd.Series
                        )["min"],
                        "Max": yarn_pending_apps_df["aggregateStatistics"].apply(
                            pd.Series
                        )["max"],
                    }
                )
                yarn_pending_apps_df["DateTime"] = pd.to_datetime(
                    yarn_pending_apps_df["DateTime"]
                )
                yarn_pending_apps_df = (
                    pd.DataFrame(
                        pd.date_range(
                            yarn_pending_apps_df["DateTime"].min(),
                            yarn_pending_apps_df["DateTime"].max(),
                            freq="H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(yarn_pending_apps_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                yarn_pending_apps_df[
                    "Time"
                ] = yarn_pending_apps_df.DateTime.dt.strftime("%d-%b %H:%M")
                yarn_pending_apps_df = yarn_pending_apps_df.set_index("Time")
                self.logger.info("getPendingApplication successful")
                return yarn_pending_apps_df
            else:
                self.logger.error(
                    "getPendingApplication failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getPendingApplication failed", exc_info=True)
            return None

    def getPendingMemory(self, cluster_name):
        """Get pending memory over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            yarn_pending_memory_df (DataFrame): Pending memory over time.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_pending_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_pending_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_pending_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                yarn_pending_memory = r.json()
                yarn_pending_memory_list = yarn_pending_memory["items"][0][
                    "timeSeries"
                ][0]["data"]
                yarn_pending_memory_df = pd.DataFrame(yarn_pending_memory_list)
                yarn_pending_memory_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            yarn_pending_memory_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": yarn_pending_memory_df["value"],
                        "Min": yarn_pending_memory_df["aggregateStatistics"].apply(
                            pd.Series
                        )["min"],
                        "Max": yarn_pending_memory_df["aggregateStatistics"].apply(
                            pd.Series
                        )["max"],
                    }
                )
                yarn_pending_memory_df["DateTime"] = pd.to_datetime(
                    yarn_pending_memory_df["DateTime"]
                )
                yarn_pending_memory_df = (
                    pd.DataFrame(
                        pd.date_range(
                            yarn_pending_memory_df["DateTime"].min(),
                            yarn_pending_memory_df["DateTime"].max(),
                            freq="H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(yarn_pending_memory_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                yarn_pending_memory_df[
                    "Time"
                ] = yarn_pending_memory_df.DateTime.dt.strftime("%d-%b %H:%M")
                yarn_pending_memory_df = yarn_pending_memory_df.set_index("Time")
                self.logger.info("getPendingMemory successful")
                return yarn_pending_memory_df
            else:
                self.logger.error(
                    "getPendingMemory failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getPendingMemory failed", exc_info=True)
            return None

    def getPendingVcore(self, cluster_name):
        """Get pending vcore over a date range.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            yarn_pending_vcore_df (DataFrame): Pending vcores over time.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_pending_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_pending_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_pending_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                yarn_pending_vcore = r.json()
                yarn_pending_vcore_list = yarn_pending_vcore["items"][0]["timeSeries"][
                    0
                ]["data"]
                yarn_pending_vcore_df = pd.DataFrame(yarn_pending_vcore_list)
                yarn_pending_vcore_df = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(
                            yarn_pending_vcore_df["timestamp"]
                        ).dt.strftime("%Y-%m-%d %H:%M"),
                        "Mean": yarn_pending_vcore_df["value"],
                        "Min": yarn_pending_vcore_df["aggregateStatistics"].apply(
                            pd.Series
                        )["min"],
                        "Max": yarn_pending_vcore_df["aggregateStatistics"].apply(
                            pd.Series
                        )["max"],
                    }
                )
                yarn_pending_vcore_df["DateTime"] = pd.to_datetime(
                    yarn_pending_vcore_df["DateTime"]
                )
                yarn_pending_vcore_df = (
                    pd.DataFrame(
                        pd.date_range(
                            yarn_pending_vcore_df["DateTime"].min(),
                            yarn_pending_vcore_df["DateTime"].max(),
                            freq="H",
                        ),
                        columns=["DateTime"],
                    )
                    .merge(yarn_pending_vcore_df, on=["DateTime"], how="outer")
                    .fillna(0)
                )
                yarn_pending_vcore_df[
                    "Time"
                ] = yarn_pending_vcore_df.DateTime.dt.strftime("%d-%b %H:%M")
                yarn_pending_vcore_df = yarn_pending_vcore_df.set_index("Time")
                self.logger.info("getPendingVcore successful")
                return yarn_pending_vcore_df
            else:
                self.logger.error(
                    "getPendingVcore failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getPendingVcore failed", exc_info=True)
            return None

    def getQueueDetails(self, yarn_rm, yarn_port):
        """Get details about yarn queues.

        Args:
            yarn_rm (str): Yarn resource manager IP.
        Returns:
            yarn_queues_list (list): Yarn queue details
        """

        try:
            r = requests.get(
                "{}://{}:{}/ws/v1/cluster/scheduler".format(
                    self.http, yarn_rm, yarn_port
                )
            )
            if r.status_code == 200:
                yarn_queues = r.json()
                yarn_queues_list = yarn_queues["scheduler"]["schedulerInfo"]["queues"][
                    "queue"
                ]
                self.logger.info("getQueueDetails successful")
                return yarn_queues_list
            else:
                self.logger.error(
                    "getQueueDetails failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getQueueDetails failed", exc_info=True)
            return None

    def getQueueApplication(self, yarn_application_df):
        """Get yarn application count based on different yarn queues.

        Args:
            yarn_application_df (DataFrame): List of yarn application in cluster.
        Returns:
            queue_app_count_df (DataFrame): Queued application count
            queue_elapsed_time_df (DataFrame): Queued application elapsed time
        """

        try:
            queue_app_count_df = pd.DataFrame(
                {"Queue": yarn_application_df["Queue"], "Application Count": 1}
            )
            queue_app_count_df = queue_app_count_df.groupby(["Queue"]).sum()
            queue_elapsed_time_df = pd.DataFrame(
                {
                    "Queue": yarn_application_df["Queue"],
                    "Elapsed Time": yarn_application_df["ElapsedTime"],
                }
            )
            queue_elapsed_time_df = queue_elapsed_time_df.groupby(["Queue"]).sum()
            self.logger.info("getQueueApplication successful")
            return queue_app_count_df, queue_elapsed_time_df
        except Exception as e:
            self.logger.error("getQueueApplication failed", exc_info=True)
            return None

    def getQueuePendingApplication(self, yarn_application_df):
        """Get details about yarn application pending in yarn queues.

        Args:
            yarn_application_df (DataFrame): List of yarn application in cluster.
        Returns:
            app_queue_df (DataFrame): Pending queued application list
            app_queue_usage_df (DataFrame): Pending queued application usage over time.
        """

        try:
            app_queue_df = pd.DataFrame(
                {
                    "Queue": yarn_application_df["Queue"],
                    "Start Time": pd.to_datetime(
                        yarn_application_df["StartedTime"]
                    ).dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Launch Time": pd.to_datetime(
                        yarn_application_df["LaunchTime"]
                    ).dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Wait Time": yarn_application_df["LaunchTime"]
                    - yarn_application_df["StartedTime"],
                }
            )
            app_queue_df["Launch Time"] = pd.to_datetime(
                app_queue_df["Launch Time"], format="%Y-%m-%d"
            )
            app_queue_df["Start Time"] = pd.to_datetime(
                app_queue_df["Start Time"], format="%Y-%m-%d"
            )
            app_queue_usage_df = pd.DataFrame(
                pd.date_range(
                    app_queue_df["Start Time"].dt.strftime("%Y-%m-%d %H:%M").min(),
                    app_queue_df["Launch Time"].dt.strftime("%Y-%m-%d %H:%M").max(),
                    freq="H",
                ),
                columns=["Date"],
            )
            self.logger.info("getQueuePendingApplication successful")
            return app_queue_df, app_queue_usage_df
        except Exception as e:
            self.logger.error("getQueuePendingApplication failed", exc_info=True)
            return None

    def getQueueVcoreMemory(self, yarn_application_df):
        """Get vcore and memory used by yarn queues.

        Args:
            yarn_application_df (DataFrame): List of yarn application in cluster.
        Returns:
            queue_vcore_df (DataFrame): Queue vcores details
            queue_vcore_usage_df (DataFrame): Queue vcores usage over time
            queue_memory_df (DataFrame): Queue memory details
            queue_memory_usage_df (DataFrame): Queue memory usage over time
        """

        try:
            queue_vcore_df = pd.DataFrame(None)
            queue_vcore_df = pd.DataFrame(
                {
                    "Application Id": yarn_application_df["ApplicationId"],
                    "Queue": yarn_application_df["Queue"],
                    "Launch Time": pd.to_datetime(
                        yarn_application_df["LaunchTime"]
                    ).dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Finished Time": pd.to_datetime(
                        yarn_application_df["FinishedTime"]
                    ).dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Vcore": yarn_application_df["VcoreSeconds"]
                    / yarn_application_df["ElapsedTime"],
                    "Vcore Seconds": yarn_application_df["VcoreSeconds"],
                }
            )
            queue_vcore_df["Launch Time"] = pd.to_datetime(
                queue_vcore_df["Launch Time"], format="%Y-%m-%d"
            )
            queue_vcore_df["Finished Time"] = pd.to_datetime(
                queue_vcore_df["Finished Time"], format="%Y-%m-%d"
            )
            queue_vcore_usage_df = pd.DataFrame(
                pd.date_range(
                    queue_vcore_df["Launch Time"].dt.strftime("%Y-%m-%d %H:%M").min(),
                    queue_vcore_df["Finished Time"].dt.strftime("%Y-%m-%d %H:%M").max(),
                    freq="H",
                ),
                columns=["Date"],
            )
            queue_memory_df = pd.DataFrame(None)
            queue_memory_df = pd.DataFrame(
                {
                    "Application Id": yarn_application_df["ApplicationId"],
                    "Queue": yarn_application_df["Queue"],
                    "Launch Time": pd.to_datetime(
                        yarn_application_df["LaunchTime"]
                    ).dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Finished Time": pd.to_datetime(
                        yarn_application_df["FinishedTime"]
                    ).dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Memory": yarn_application_df["MemorySeconds"]
                    / yarn_application_df["ElapsedTime"],
                    "Memory Seconds": yarn_application_df["MemorySeconds"],
                }
            )
            queue_memory_df["Launch Time"] = pd.to_datetime(
                queue_memory_df["Launch Time"], format="%Y-%m-%d"
            )
            queue_memory_df["Finished Time"] = pd.to_datetime(
                queue_memory_df["Finished Time"], format="%Y-%m-%d"
            )
            queue_memory_usage_df = pd.DataFrame(
                pd.date_range(
                    queue_memory_df["Launch Time"].dt.strftime("%Y-%m-%d %H:%M").min(),
                    queue_memory_df["Finished Time"]
                    .dt.strftime("%Y-%m-%d %H:%M")
                    .max(),
                    freq="H",
                ),
                columns=["Date"],
            )
            self.logger.info("getQueueVcoreMemory successful")
            return (
                queue_vcore_df,
                queue_vcore_usage_df,
                queue_memory_df,
                queue_memory_usage_df,
            )
        except Exception as e:
            self.logger.error("getQueueVcoreMemory failed", exc_info=True)
            return None

    def getHbaseDataSize(self):
        """Get HBase storage details.

        Returns:
            base_size (float) : Base size of HBase
            disk_space_consumed (float) : Disk size consumed by HBase
        """

        try:
            base_size = 0
            disk_space_consumed = 0
            out = subprocess.check_output(
                "hdfs dfs -du -h /", shell=True, stderr=subprocess.STDOUT,
            )
            output = str(out)
            lines = output.split("\\n")
            for i in lines:
                if i.split("/")[-1] == "hbase":
                    base_size = ""
                    disk_space_consumed = ""
                    hbase_data = i.split("/")[0].strip()
                    hbase_data = hbase_data.split("'")[1]
                    data_list = hbase_data.split(".")
                    base_size = (
                        data_list[0]
                        + "."
                        + data_list[1][0]
                        + data_list[1][1]
                        + data_list[1][2]
                    )
                    if len(data_list[1]) > 3:
                        for j in data_list[1][3:]:
                            if j != (" "):
                                disk_space_consumed = disk_space_consumed + j
                        disk_space_consumed = disk_space_consumed + "." + data_list[2]
                    else:
                        disk_space_consumed = 0
            self.logger.info("getHbaseDataSize successful")
            return base_size, disk_space_consumed
        except Exception as e:
            self.logger.error("getHbaseDataSize failed", exc_info=True)
            return None

    def getHbaseReplication(self, cluster_name):
        """Get HBase replication factor.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            replication (str): HBase replication factor.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/clusters/{}/services/hbase/config".format(
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
                    "{}://{}:{}/api/v33/clusters/{}/services/hbase/config".format(
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
                    "{}://{}:{}/api/v19/clusters/{}/services/hbase/config".format(
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
                hbase = r.json()
                hbase_config = hbase["items"]
                hbase_replication = "false"
                replication = "No"
                for i in hbase_config:
                    if i["name"] == "hbase_enable_replication":
                        hbase_replication = i["value"]
                        if hbase_replication == "true":
                            replication = "Yes"
                self.logger.info("getHbaseReplication successful")
                return replication
            else:
                self.logger.error(
                    "getHbaseReplication failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getHbaseReplication failed", exc_info=True)
            return None

    def getHbaseSecondaryIndex(self, cluster_name):
        """Get HBase secondary indexing details.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            indexing (str): HBase secondary index value.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/clusters/{}/services/hbase/config".format(
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
                    "{}://{}:{}/api/v33/clusters/{}/services/hbase/config".format(
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
                    "{}://{}:{}/api/v19/clusters/{}/services/hbase/config".format(
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
                hbase = r.json()
                hbase_config = hbase["items"]
                hbase_indexing = "false"
                hbase_replication = "false"
                indexing = "No"
                for i in hbase_config:
                    if i["name"] == "hbase_enable_indexing":
                        hbase_indexing = i["value"]
                    if i["name"] == "hbase_enable_replication":
                        hbase_replication = i["value"]
                if hbase_indexing == hbase_replication == "true":
                    indexing = "Yes"
                self.logger.info("getHbaseSecondaryIndex successful")
                return indexing
            else:
                self.logger.error(
                    "getHbaseSecondaryIndex failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getHbaseSecondaryIndex failed", exc_info=True)
            return None

    def getDynamicAllocationAndSparkResourceManager(self):
        """Get spark config details.

        Returns:
            dynamic_allocation (str): Dynamic Allocation value.
            spark_resource_manager (str): Spark resource manager value.
        """

        try:
            dynamic_allocation = ""
            spark_resource_manager = ""
            spark_config_filename = glob.glob("/etc/spark/conf.*/spark-defaults.conf")
            with open(spark_config_filename[0], "r") as f:
                for line in f:
                    if line.split("=")[0] == "spark.dynamicAllocation.enabled":
                        if line.split("=")[1].strip() == "true":
                            dynamic_allocation = "Enabled"
                        else:
                            dynamic_allocation = "Disabled"
                    elif line.split("=")[0] == "spark.master":
                        spark_resource_manager = line.split("=")[1].strip()
                    else:
                        continue
            self.logger.info("getDynamicAllocationAndSparkResourceManager successful")
            return dynamic_allocation, spark_resource_manager
        except Exception as e:
            self.logger.error(
                "getDynamicAllocationAndSparkResourceManager failed", exc_info=True
            )
            return None

    def getSparkVersion(self):
        """Get Spark version details.

        Returns:
            spark_version (str): Spark version
        """

        try:
            out = subprocess.check_output(
                '(spark-shell --version &> tmp.data ; grep version tmp.data | head -1 | awk "{print $NF}";rm tmp.data)',
                shell=True,
                stderr=subprocess.STDOUT,
            )
            spark_version = str(out.strip())
            if spark_version != "b''":
                spark_version = spark_version.split("'")[1]
                spark_version = spark_version.split("version")[1]
                spark_version = spark_version.strip()
            self.logger.info("getSparkVersion successful")
            return spark_version
        except Exception as e:
            self.logger.error("getSparkVersion failed", exc_info=True)
            return None

    def getSparkApiProgrammingLanguages(self):
        """Get list of languages used by spark programs.

        Returns:
            language_list (str): List of languages separated by comma.
        """

        try:
            command = "hdfs dfs -ls /user/spark/applicationHistory"
            command = command + " | awk ' {print $8} '"
            file_paths = subprocess.check_output(
                command, shell=True, stderr=subprocess.STDOUT,
            )
            file_paths = str(file_paths)
            l_list = file_paths.split("\\n")
            language_list = []
            if len(l_list) > 2:
                l_list.pop(0)
                l_list.pop(-1)
                paths = []
                if len(l_list) > 30:
                    for item in l_list[:10]:
                        paths.append(item)
                    if (len(l_list) % 2) != 0:
                        middle = int(len(l_list) / 2)
                    else:
                        middle = int(len(l_list) / 2) - 1
                    for item in l_list[middle : middle + 10]:
                        paths.append(item)
                    for item in l_list[len(l_list) - 11 :]:
                        paths.append(item)
                    l_list = paths
                for path in l_list:
                    output = subprocess.check_output(
                        "hdfs dfs -cat {}".format(path), shell=True
                    )
                    output = str(output)
                    if output != "b''":
                        language = (output).split('"sun.java.command":')[1]
                        language = language.split(",")[0]
                        if language.find(".py") != -1:
                            if "Python" not in language_list:
                                language_list.append("Python")
                        elif language.find(".java") != -1:
                            if "Java" not in language_list:
                                language_list.append("Java")
                        elif language.find(".scala") != -1:
                            if "Scala" not in language_list:
                                language_list.append("Scala")
                        elif language.find(".R") != -1:
                            if "R" not in language_list:
                                language_list.append("R")
            language_list = ", ".join(language_list)
            self.logger.info("getSparkApiProgrammingLanguages successful")
            return language_list
        except Exception as e:
            self.logger.error("getSparkApiProgrammingLanguages failed", exc_info=True)
            return None

    def retentionPeriodKafka(self):
        """Get retention period of kafka.

        Returns:
            retention_period (str): Kafka retention period
        """

        try:
            period = os.popen(
                "grep -m 1 log.retention.hours /etc/kafka/server.properties",
                # stdout=subprocess.DEVNULL,
                # stderr=subprocess.STDOUT,
            ).read()
            retention_period = int(period.split("=")[1].strip("\n"))
            self.logger.info("retentionPeriodKafka successful")
            return retention_period
        except Exception as e:
            self.logger.error("retentionPeriodKafka failed", exc_info=True)
            return None

    def numTopicsKafka(self, zookeeper_host, zookeeper_port):
        """Get num of topics in kafka.
        
        Args:
            zookeeper_host (str) Zookeeper host IP
            zookeeper_port (str): Zookeeper port number
        Returns:
            num_topics (int): Number of topics in kafka.
        """

        try:
            os.popen(
                "kafka-topics --zookeeper "
                + str(zookeeper_host)
                + ":"
                + str(zookeeper_port)
                + " --list > topics_list.csv",
                # stdout=subprocess.DEVNULL,
                # stderr=subprocess.STDOUT,
            ).read()
            topics_df = pd.read_csv("topics_list.csv", header=None)
            topics_df.columns = ["topics"]
            num_topics = len(topics_df.index)
            self.logger.info("numTopicsKafka successful")
            return num_topics
        except Exception as e:
            self.logger.error("numTopicsKafka failed", exc_info=True)
            return None

    def msgSizeKafka(self, zookeeper_host, zookeeper_port, broker_host, broker_port):
        """Get volume of message in kafka in bytes.
        
        Args:
            zookeeper_host (str) Zookeeper host IP
            zookeeper_port (str): Zookeeper port number
            broker_host (str): Broker host IP
            broker_port (str): Broker port number
        Returns:
            sum_size (int): Message size of Kafka
        """

        try:
            os.popen(
                "kafka-topics --zookeeper "
                + str(zookeeper_host)
                + ":"
                + str(zookeeper_port)
                + " --list > topics_list.csv",
                # stdout=subprocess.DEVNULL,
                # stderr=subprocess.STDOUT,
            ).read()
            topics_df = pd.read_csv("topics_list.csv", header=None)
            topics_df.columns = ["topics"]
            sum_size = 0
            for i in topics_df["topics"]:
                msg_size = os.popen(
                    " kafka-log-dirs     --bootstrap-server "
                    + str(broker_host)
                    + ":"
                    + str(broker_port)
                    + "  --topic-list "
                    + str(i)
                    + "     --describe   | grep '^{'   | jq '[ ..|.size? | numbers ] | add'"
                ).read()
                msg_size = msg_size.strip("\n")
                sum_size = sum_size + int(msg_size)
            self.logger.info("msgSizeKafka successful")
            return sum_size
        except Exception as e:
            self.logger.error("msgSizeKafka failed", exc_info=True)
            return None

    def msgCountKafka(self, zookeeper_host, zookeeper_port, broker_host, broker_port):
        """Get count of messages in kafka topics.

        Args:
            zookeeper_host (str) Zookeeper host IP
            zookeeper_port (str): Zookeeper port number
            broker_host (str): Broker host IP
            broker_port (str): Broker port number
        Returns:
            sum_count (int): Number of messages in Kafka
        """

        try:
            os.popen(
                "kafka-topics --zookeeper "
                + str(zookeeper_host)
                + ":"
                + str(zookeeper_port)
                + " --list > topics_list.csv",
                # stdout=subprocess.DEVNULL,
                # stderr=subprocess.STDOUT,
            ).read()
            topics_df = pd.read_csv("topics_list.csv", header=None)
            topics_df.columns = ["topics"]
            sum_count = 0
            for i in topics_df["topics"]:
                msg_count = os.popen(
                    "kafka-run-class kafka.tools.GetOffsetShell --broker-list "
                    + str(broker_host)
                    + str(":")
                    + str(broker_port)
                    + " --topic "
                    + str(i)
                    + " --time -1 --offsets 1 | awk -F  \":\" '{sum += $3} END {print sum}'"
                ).read()
                msg_count = msg_count.strip("\n")
                sum_count = sum_count + int(msg_count)
            self.logger.info("msgCountKafka successful")
            return sum_count
        except Exception as e:
            self.logger.error("msgCountKafka failed", exc_info=True)
            return None

    def clusterSizeAndBrokerSizeKafka(self):
        """Get per cluster storage and kafka cluster storage in kafka.

        Returns:
            total_size (float): Total size of kafka cluster
            brokersize (DataFrame): Size for each broker.
        """

        try:
            logs_dir = ["kafka-logs", "kafka-logs1"]
            broker_id = 0
            brokersize = pd.DataFrame(columns=["broker_size"])
            j = 0
            for k in logs_dir:
                os.popen(
                    "du -sh /tmp/" + str(k) + "/* > broker_size.csv",
                    # stdout=subprocess.DEVNULL,
                    # stderr=subprocess.STDOUT,
                ).read()
                brokers_df = pd.read_csv("broker_size.csv", header=None)
                brokers_df.columns = ["logs"]
                size_sum = 0
                for i in brokers_df["logs"]:
                    size = i.split("\t", 1)[0]
                    size = float(size.strip("K"))
                    size_sum = size_sum + size
                brokersize.loc[j] = size_sum
                j = j + 1
            total_size = 0
            for i in brokersize["broker_size"]:
                total_size = total_size + float(i)
            broker_list = []
            for index, row in brokersize.iterrows():
                broker_list.append(str(row["broker_size"]) + " KB")
            broker_list = ", ".join(broker_list)
            self.logger.info("clusterSizeAndBrokerSizeKafka successful")
            return total_size, broker_list
        except Exception as e:
            self.logger.error("clusterSizeAndBrokerSizeKafka failed", exc_info=True)
            return None

    def useOfImpala(self):
        """Get impala service in cluster.

        Returns:
            output (str): Impala information for cluster.
        """

        try:
            output = ""
            version_data = json.loads(
                os.popen(
                    "cat /opt/cloudera/parcels/CDH/meta/parcel.json",
                    # stdout=subprocess.DEVNULL,
                    # stderr=subprocess.STDOUT,
                ).read()
            )
            data = version_data["components"]
            df = pd.DataFrame(data)
            services_df = df
            found = 0
            services_df["sub_version"] = services_df.version.str[:5]
            for i in services_df["name"]:
                if i == "impala":
                    found = 1
            if found == 1:
                output = (
                    "Impala found with version: "
                    + services_df.loc[services_df["name"] == i].sub_version.item()
                )
            else:
                output = "Impala is not found"
            self.logger.info("useOfImpala successful")
            return output
        except Exception as e:
            self.logger.error("useOfImpala failed", exc_info=True)
            return None

    def useOfSentry(self):
        """Get sentry service in cluster.

        Returns:
            output (str): Sentry information for cluster.
        """

        try:
            output = ""
            version_data = json.loads(
                os.popen(
                    "cat /opt/cloudera/parcels/CDH/meta/parcel.json",
                    # stdout=subprocess.DEVNULL,
                    # stderr=subprocess.STDOUT,
                ).read()
            )
            data = version_data["components"]
            df = pd.DataFrame(data)
            services_df = df
            found = 0
            services_df["sub_version"] = services_df.version.str[:5]
            for i in services_df["name"]:
                if i == "sentry":
                    found = 1
            if found == 1:
                output = (
                    "Apache Sentry found with version: "
                    + services_df.loc[services_df["name"] == i].sub_version.item()
                )
            else:
                output = "Apache Sentry is not found"
            self.logger.info("useOfSentry successful")
            return output
        except Exception as e:
            self.logger.error("useOfSentry failed", exc_info=True)
            return None

    def useOfKudu(self):
        """Get kudu service in cluster.

        Returns:
            output (str): Kudu information for cluster.
        """

        try:
            output = ""
            version_data = json.loads(
                os.popen(
                    "cat /opt/cloudera/parcels/CDH/meta/parcel.json",
                    # stdout=subprocess.DEVNULL,
                    # stderr=subprocess.STDOUT,
                ).read()
            )
            data = version_data["components"]
            df = pd.DataFrame(data)
            services_df = df
            found = 0
            services_df["sub_version"] = services_df.version.str[:5]
            for i in services_df["name"]:
                if i == "kudu":
                    found = 1
            if found == 1:
                output = (
                    "Apache Kudu found with version: "
                    + services_df.loc[services_df["name"] == i].sub_version.item()
                )
            else:
                output = "Apache Kudu is not found"
            self.logger.info("useOfKudu successful")
            return output
        except Exception as e:
            self.logger.error("useOfKudu failed", exc_info=True)
            return None
