# ------------------------------------------------------------------------------
# This module is used to get the storage related features like the size of
# the Hadoop clusters.Unveiling the usage over the period of time for the
# customized range specified by the user. This module generates the clear output
# of various key specifications of hadoop distributed file system.
# ------------------------------------------------------------------------------

# Importing required libraries
from imports import *


class DataAPI:
    """This Class has functions related to the Cluster Data category.

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

    def totalSizeConfigured(self):
        """Get total storage size and storage at each node for HDFS.

        Returns:
            individual_node_size (list): Total storage of all nodes.
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
            self.logger.info("totalSizeConfigured successful")
            return mapped_df, total_storage
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
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}".format(
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
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity_used%2Bdfs_capacity_used_non_hdfs%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity_used%2Bdfs_capacity_used_non_hdfs%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}".format(
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
                    "{}://{}:{}/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity_used%2Bdfs_capacity_used_non_hdfs%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}".format(
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

    def getHiveConfigItems(self, cluster_name):
        """Get Hive metastore config details from cluster.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            mt_db_host (str): Metastore database host name
            mt_db_name (str): Metastore database name
            mt_db_type (str): Metastore database type
            mt_db_port (str): Metastore database port number
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/clusters/{}/services/hive/config".format(
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
                    "{}://{}:{}/api/v33/clusters/{}/services/hive/config".format(
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
                    "{}://{}:{}/api/v19/clusters/{}/services/hive/config".format(
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
                hive_config = r.json()
                hive_config_items = hive_config["items"]
                mt_db_host = ""
                mt_db_name = ""
                mt_db_type = ""
                mt_db_port = ""
                for i in hive_config_items:
                    if i["name"] == "hive_metastore_database_host":
                        mt_db_host = i["value"]
                    elif i["name"] == "hive_metastore_database_name":
                        mt_db_name = i["value"]
                    elif i["name"] == "hive_metastore_database_port":
                        mt_db_port = i["value"]
                    elif i["name"] == "hive_metastore_database_type":
                        mt_db_type = i["value"]
                self.logger.info("getHiveConfigItems successful")
                return mt_db_host, mt_db_name, mt_db_type, mt_db_port
            else:
                self.logger.error(
                    "getHiveConfigItems failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("getHiveConfigItems failed", exc_info=True)
            return None

    def gethiveMetaStore(self, database_uri, database_type):
        """Get Hive tables and databases details.

        Args:
            database_uri (str): Metastore database connection URI.
            database_type (str): Metastore database type.
        Returns:
            table_df (DataFrame): List of tables and database in hive.
        """

        try:
            engine = create_engine(database_uri)
            table_count = 0
            table_df = pd.DataFrame(
                columns=["Table_Name", "Last_Access_Time", "Data_Type", "Database"]
            )
            if database_type == "postgresql":
                result = engine.execute(
                    """
                select t."TBL_NAME",t."LAST_ACCESS_TIME", d."NAME" 
                from 
                "DBS" as d join "TBLS" as t 
                on 
                t."DB_ID"=d."DB_ID" 
                where 
                d."NAME" not in ('information_schema','sys');
                """
                )
            elif database_type == "mysql":
                result = engine.execute(
                    """
                select t.TBL_NAME,t.LAST_ACCESS_TIME, d.NAME 
                from 
                DBS as d join TBLS as t 
                on 
                t.DB_ID=d.DB_ID 
                where 
                d.NAME not in ('information_schema','sys');
                """
                )
            for row in result:
                table_count = table_count + 1
                table_name = row[0]
                last_access_time = (int(row[1]) + 500) / 1000
                database = row[2]
                table_tmp_df = pd.DataFrame(
                    {
                        "Table_Name": table_name,
                        "Last_Access_Time": datetime.fromtimestamp(
                            last_access_time
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                        "Database": database,
                    },
                    index=[table_count],
                )
                table_df = table_df.append(table_tmp_df)
            table_df["Last_Access_Time"] = pd.to_datetime(table_df["Last_Access_Time"])
            warm = datetime.strptime(self.end_date, "%Y-%m-%d %H:%M") - timedelta(
                days=1
            )
            cold = datetime.strptime(self.end_date, "%Y-%m-%d %H:%M") - timedelta(
                days=3
            )
            table_df.loc[table_df["Last_Access_Time"] > warm, "Data_Type"] = "Hot"
            table_df.loc[
                (table_df["Last_Access_Time"] <= warm)
                & (table_df["Last_Access_Time"] > cold),
                "Data_Type",
            ] = "Warm"
            table_df.loc[(table_df["Last_Access_Time"] <= cold), "Data_Type"] = "Cold"
            table_df["Count"] = 1
            table_df = pd.DataFrame(
                {"Data_Type": table_df["Data_Type"], "Table Count": table_df["Count"]}
            )
            table_df = table_df.groupby(["Data_Type"]).sum()
            self.logger.info("gethiveMetaStore successful")
            return table_df
        except Exception as e:
            self.logger.error("gethiveMetaStore failed", exc_info=True)
            return None

    def getHiveDatabaseInfo(self, database_uri, database_type):
        """Get Hive databases details.

        Args:
            database_uri (str): Metastore database connection URI.
            database_type (str): Metastore database type.
        Returns:
            database_df (DataFrame): List of databases and thier size in hive.
        """

        try:
            engine = create_engine(database_uri)
            table_count = 0
            database_df = pd.DataFrame(columns=["Database", "File_Size", "Count"])
            out = subprocess.check_output(
                'hive -e "show databases"', shell=True, stderr=subprocess.STDOUT
            )
            out = str(out)
            out = out.split("\\n")
            for db in out:
                database_location = ""
                if (
                    (db.find("+--") == -1)
                    and (db.find("database_name") == -1)
                    and (db != "'")
                    and (db.find("WARN") == -1)
                ):
                    if db.find("'") != -1:
                        db = db.split("'")[1]
                    if db.find("|") != -1:
                        db = db.split("|")[1]
                    db = db.strip()
                    if (db != "information_schema") and (db != "sys"):
                        if database_type == "postgresql":
                            result = engine.execute(
                                """
                            SELECT count(t."TBL_ID")
                            FROM
                            "DBS" as d join "TBLS" as t
                            on
                            d."DB_ID"=t."DB_ID"
                            where
                            d."NAME" = '{}'
                            GROUP BY d."DB_ID";
                            """.format(
                                    db
                                )
                            )
                        elif database_type == "mysql":
                            result = engine.execute(
                                """
                            SELECT count(t.TBL_ID)
                            FROM
                            DBS as d join TBLS as t
                            on
                            d.DB_ID=t.DB_ID
                            where
                            d.NAME = '{}'
                            GROUP BY d.DB_ID;
                            """.format(
                                    db
                                )
                            )
                        for row in result:
                            table_count = row[0]
                        database = subprocess.check_output(
                            'hive -e "describe schema {}"'.format(db),
                            shell=True,
                            stderr=subprocess.STDOUT,
                        )
                        database = str(database)
                        if (database.find("\\t")) and (database.find("+--") == -1):
                            database = database.split("\\t")
                            database_location = database[2].strip()
                        elif database.find("\\n"):
                            database = database.split("\\n")
                            for row in database:
                                if (
                                    (row.find("+--") == -1)
                                    and (row.find("db_name") == -1)
                                    and (row != "'")
                                    and (row.find("WARN") == -1)
                                ):
                                    row = row.split("|")
                                    database_location = row[3].strip()
                        database_location = database_location.split(":")[2]
                        database_location = database_location[4:]
                        command = "hdfs dfs -du -s -h {}".format(database_location)
                        command = command + " | awk ' {print $2} '"
                        database_size = subprocess.check_output(
                            command, shell=True, stderr=subprocess.STDOUT
                        )
                        database_size = str(database_size.strip())
                        database_size = database_size.split("'")[1]
                        database_tmp_df = pd.DataFrame(
                            {
                                "Database": db,
                                "File_Size": database_size,
                                "Count": table_count,
                            },
                            index=[table_count],
                        )
                        database_df = database_df.append(database_tmp_df)
            database_df["File_Size"] = database_df["File_Size"].astype(str).astype(int)
            database_df["Count"] = 1
            database_df = database_df.groupby(["Database"]).sum()
            database_df.reset_index(inplace=True)
            self.logger.info("getHiveDatabaseInfo successful")
            return database_df
        except Exception as e:
            self.logger.error("getHiveDatabaseInfo failed", exc_info=True)
            return None

    def getHiveDatabaseCount(self, database_uri, database_type):
        """Get Hive databases count.

        Args:
            database_uri (str): Metastore database connection URI.
            database_type (str): Metastore database type.
        Returns:
            database_count (int): Number of databases in hive.
        """

        try:
            engine = create_engine(database_uri)
            database_count = 0
            if database_type == "postgresql":
                result = engine.execute(
                    """
                select count("DB_ID") from "DBS" where "NAME" not in ('information_schema','sys')
                """
                )
            elif database_type == "mysql":
                result = engine.execute(
                    """
                select count(DB_ID) from DBS where NAME not in ('information_schema','sys')
                """
                )
            for row in result:
                database_count = row[0]
            self.logger.info("getHiveDatabaseCount successful")
            return database_count
        except Exception as e:
            self.logger.error("getHiveDatabaseCount failed", exc_info=True)
            return None

    def getHivePartitionedTableCount(self, database_uri, database_type):
        """Get Hive partitioned and non-partitioned tables details.

        Args:
            database_uri (str): Metastore database connection URI.
            database_type (str): Metastore database type.
        Returns:
            number_of_tables_with_partition (int): Number of tables with partition in hive
            number_of_tables_without_partition (int): Number of tables without partition in hive
        """

        try:
            engine = create_engine(database_uri)
            total_tables = 0
            number_of_tables_without_partition = 0
            number_of_tables_with_partition = 0
            if database_type == "postgresql":
                result = engine.execute(
                    """
                select count(distinct("TBL_ID")) from "PARTITIONS"
                """
                )
            elif database_type == "mysql":
                result = engine.execute(
                    """
                select count(distinct(TBL_ID)) from PARTITIONS
                """
                )
            for row in result:
                number_of_tables_with_partition = row[0]
            if database_type == "postgresql":
                result = engine.execute(
                    """
                select count(t."TBL_NAME") 
                from 
                "DBS" as d join "TBLS" as t 
                on 
                t."DB_ID"=d."DB_ID" 
                where 
                d."NAME" not in ('information_schema','sys');
                """
                )
            elif database_type == "mysql":
                result = engine.execute(
                    """
                select count(t.TBL_NAME) 
                from 
                DBS as d join TBLS as t 
                on 
                t.DB_ID=d.DB_ID 
                where 
                d.NAME not in ('information_schema','sys');
                """
                )
            for row in result:
                total_tables = row[0]
            number_of_tables_without_partition = (
                total_tables - number_of_tables_with_partition
            )
            self.logger.info("getHivePartitionedTableCount successful")
            return number_of_tables_with_partition, number_of_tables_without_partition
        except Exception as e:
            self.logger.error("getHivePartitionedTableCount failed", exc_info=True)
            return None

    def getHiveInternalExternalTables(self, database_uri, database_type):
        """Get Hive internal and external tables count.

        Args:
            database_uri (str): Metastore database connection URI.
            database_type (str): Metastore database type.
        Returns:
            internal_tables (int): Number of internal tables in hive
            external_tables (int): Number of external tables in hive
        """

        try:
            engine = create_engine(database_uri)
            internal_tables = 0
            external_tables = 0
            if database_type == "postgresql":
                result = engine.execute(
                    """
                SELECT count(b."TBL_ID")
                FROM
                "DBS" as a join "TBLS" as b
                on
                a."DB_ID"=b."DB_ID"
                where
                a."NAME" not in('information_schema','sys') and b."TBL_TYPE" = 'MANAGED_TABLE'
                GROUP BY a."DB_ID"
                """
                )
            elif database_type == "mysql":
                result = engine.execute(
                    """
                SELECT count(b.TBL_ID)
                FROM
                DBS as a join TBLS as b
                on
                a.DB_ID=b.DB_ID
                where
                a.NAME not in('information_schema','sys') and b.TBL_TYPE = 'MANAGED_TABLE'
                GROUP BY a.DB_ID
                """
                )
            for row in result:
                internal_tables = row[0]
            if database_type == "postgresql":
                result = engine.execute(
                    """
                SELECT count(b."TBL_ID")
                FROM
                "DBS" as a join "TBLS" as b
                on
                a."DB_ID"=b."DB_ID"
                where
                a."NAME" not in('information_schema','sys') and b."TBL_TYPE" = 'EXTERNAL_TABLE'
                GROUP BY a."DB_ID"
                """
                )
            elif database_type == "mysql":
                result = engine.execute(
                    """
                SELECT count(b.TBL_ID)
                FROM
                DBS as a join TBLS as b
                on
                a.DB_ID=b.DB_ID
                where
                a.NAME not in('information_schema','sys') and b.TBL_TYPE = 'EXTERNAL_TABLE'
                GROUP BY a.DB_ID
                """
                )
            for row in result:
                external_tables = row[0]
            self.logger.info("getHiveInternalExternalTables successful")
            return internal_tables, external_tables
        except Exception as e:
            self.logger.error("getHiveInternalExternalTables failed", exc_info=True)
            return None

    def getHiveExecutionEngine(self):
        """Get Hive execution engine details.

        Returns:
            hive_execution_engine (str): Execution engine used by hive.
        """

        try:
            hive_execution_engine = ""
            hive_execution_engine = subprocess.check_output(
                'hive -e "set hive.execution.engine"',
                shell=True,
                stderr=subprocess.STDOUT,
            )
            hive_execution_engine = str(hive_execution_engine)
            hive_execution_engine = hive_execution_engine.split("\\n")
            for line in hive_execution_engine:
                if line.find("hive.execution.engine") != -1:
                    hive_execution_engine = line.split("=")[1]
                    if hive_execution_engine.find("|") != -1:
                        hive_execution_engine = hive_execution_engine.split("|")[0]
                        hive_execution_engine = hive_execution_engine.strip()
            self.logger.info("getHiveExecutionEngine successful")
            return hive_execution_engine
        except Exception as e:
            self.logger.error("getHiveExecutionEngine failed", exc_info=True)
            return None

