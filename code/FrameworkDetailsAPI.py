# Importing required libraries
from imports import *


class FrameworkDetailsAPI:
    """This Class has functions related to Frameworks and Software Details 
    category.

    Has functions which fetch different frameworks and software metrics 
    from Hadoop cluster like Hadoop version, services version, etc.

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

    def hadoopVersion(self):
        """Get Hadoop major and minor version and Hadoop Distribution.

        Returns:
            hadoop_major (str): Hadoop major version
            hadoop_minor (str): Hadoop miror version
            distribution (str): Hadoop vendor name
        """

        try:
            hversion = os.popen("hadoop version").read()
            hadoop_major = hversion[0:12]
            os.popen("hadoop version > ./data.csv").read()
            dt = "This command was run using "
            a = ""
            with open("./data.csv") as fp:
                with open("./out2.csv", "w") as f1:
                    for line in fp:
                        if dt in line:
                            a = line
                            a = (
                                line.replace(
                                    "This command was run using /opt/cloudera/parcels/",
                                    "",
                                )
                                .replace(
                                    "/jars/hadoop-common-3.1.1.7.1.4.0-203.jar", ""
                                )
                                .replace("", "")
                            )
            hadoop_minor = a[0:9]
            os.popen("rm ./data.csv").read()
            os.popen("rm ./out2.csv").read()
            distribution = ""
            if re.search(r"\bcdh7\b", a):
                distribution = "CDH7"
            elif re.search(r"\bcdh6\b", a):
                distribution = "CDH6"
            elif re.search(r"\bcdh5\b", a):
                distribution = "CDH5"
            self.logger.info("hadoopVersion successful")
            return hadoop_major, hadoop_minor, distribution
        except Exception as e:
            self.logger.error("hadoopVersion failed", exc_info=True)
            return None

    def versionMapping(self, cluster_name):
        """Get list of services installed in cluster with their versions.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            list_services_installed_df (DataFrame): List of services installed.
            new_ref_df (DataFrame): Services mapped with their version.
        """

        try:
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
                version_related = r.json()
                version_related_count = version_related["items"]
                list_apache_services = []
                for i in version_related_count:
                    version_related_show = version_related
                    for displayname in version_related_show["items"]:
                        displyName = displayname["displayName"].lower()
                        list_apache_services.append(displyName)
                list_apache_services = list(set(list_apache_services))
                list_services_installed_df = pd.DataFrame(
                    list_apache_services, columns=["name"]
                )
                version_data = json.loads(
                    os.popen("cat /opt/cloudera/parcels/CDH/meta/parcel.json").read()
                )
                data = version_data["components"]
                df_service_version = pd.DataFrame(data)
                new_ref_df = list_services_installed_df.merge(
                    df_service_version, how="left"
                )
                new_ref_df_nan = new_ref_df[new_ref_df.isna().any(axis=1)]["name"]
                for i in new_ref_df_nan.iteritems():
                    found = df_service_version[
                        df_service_version["name"].str.contains(i[1])
                    ]
                    if found.empty:
                        pass
                    else:
                        exist = new_ref_df[new_ref_df["name"].str.contains(i[1])]
                        if exist.empty:
                            break
                        else:
                            new_ref_df = new_ref_df.append(found)
                new_ref_df = new_ref_df.drop_duplicates()
                new_ref_df.dropna(subset=["pkg_release"], inplace=True)
                new_ref_df["sub_version"] = new_ref_df.version.str[:5]
                new_ref_df = new_ref_df.drop(["version"], axis=1)
                new_ref_df = new_ref_df.reset_index(drop=True)
                self.logger.info("versionMapping successful")
                return list_services_installed_df, new_ref_df
            else:
                self.logger.error(
                    "versionMapping failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("versionMapping failed", exc_info=True)
            return None
