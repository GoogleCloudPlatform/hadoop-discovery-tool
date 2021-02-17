# Importing Required Libraries
from imports import *


# This Class has functions related to Application category
class ApplicationAPI:
    # Initialize Inputs
    def __init__(self, inputs):
        self.inputs = inputs
        self.version = inputs["version"]
        self.cloudera_manager_host_ip = inputs["cloudera_manager_host_ip"]
        self.cloudera_manager_username = inputs["cloudera_manager_username"]
        self.cloudera_manager_password = inputs["cloudera_manager_password"]
        self.cluster_name = inputs["cluster_name"]
        self.logger = inputs["logger"]

    # Get list of all yarn application over a date range
    def getApplicationDetails(self, yarn_rm):
        try:
            r = requests.get("http://{}:8088/ws/v1/cluster/apps".format(yarn_rm))
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
                        (yarn_application_df["StartedTime"] < (end_date))
                        & (yarn_application_df["StartedTime"] >= (start_date))
                        & (yarn_application_df["LaunchTime"] >= (start_date))
                        & (yarn_application_df["FinishedTime"] >= (start_date))
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
                        (yarn_application_df["StartedTime"] < (end_date))
                        & (yarn_application_df["StartedTime"] >= (start_date))
                        & (yarn_application_df["LaunchTime"] >= (start_date))
                        & (yarn_application_df["FinishedTime"] >= (start_date))
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
                        (yarn_application_df["StartedTime"] < (end_date))
                        & (yarn_application_df["StartedTime"] >= (start_date))
                        & (yarn_application_df["FinishedTime"] >= (start_date))
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

    # Get yarn application count based to its type and status
    def getApplicationTypeStatusCount(self, yarn_application_df):
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

    # Get vcore and memory usage of yarn application
    def getApplicationVcoreMemoryUsage(self, yarn_application_df):
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

    # Get details about busrty yarn application
    def getBurstyApplicationDetails(self, yarn_application_df):
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

    # Get details about failed or killed yarn application
    def getFailedApplicationDetails(self, yarn_application_df):
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

    # Get total vcore allocated to yarn
    def getYarnTotalVcore(self, yarn_rm):
        try:
            r = requests.get("http://{}:8088/ws/v1/cluster/metrics".format(yarn_rm))
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

    # Get yarn vcore availability data over a date range
    def getYarnVcoreAvailable(self, cluster_name):
        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_available_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_available_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_available_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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

    # Get yarn vcore allocation data over a date range
    def getYarnVcoreAllocated(self, cluster_name):
        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_allocated_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_allocated_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_allocated_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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

    # Get total memory allocated to yarn
    def getYarnTotalMemory(self, yarn_rm):
        try:
            r = requests.get("http://{}:8088/ws/v1/cluster/metrics".format(yarn_rm))
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

    # Get yarn memory availability data over a date range
    def getYarnMemoryAvailable(self, cluster_name):
        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_available_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_available_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_available_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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

    # Get yarn memory allocation data over a date range
    def getYarnMemoryAllocated(self, cluster_name):
        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_allocated_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_allocated_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_allocated_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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

    # Get vcore and memory breakdown by yarn application
    def getVcoreMemoryByApplication(self, yarn_application_df):
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

    # Get pending application over a date range
    def getPendingApplication(self, cluster_name):
        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_apps_pending_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_apps_pending_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_apps_pending_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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

    # Get pending memory over a date range
    def getPendingMemory(self, cluster_name):
        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_pending_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_pending_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_pending_memory_mb_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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

    # Get pending vcore over a date range
    def getPendingVcore(self, cluster_name):
        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_pending_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_pending_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20total_pending_vcores_across_yarn_pools%20where%20entityName%3Dyarn%20and%20clusterName%20%3D%20{}&to={}".format(
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

    # Get details about yarn queues
    def getQueueDetails(self, yarn_rm):
        try:
            r = requests.get("http://{}:8088/ws/v1/cluster/scheduler".format(yarn_rm))
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

    # Get yarn application count based on different yarn queues
    def getQueueApplication(self, yarn_application_df):
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

    # Get details about yarn application pending in yarn queues
    def getQueuePendingApplication(self, yarn_application_df):
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

    # Get vcore and memory used by yarn queues
    def getQueueVcoreMemory(self, yarn_application_df):
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
