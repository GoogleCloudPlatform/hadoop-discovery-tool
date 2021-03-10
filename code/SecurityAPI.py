# ------------------------------------------------------------------------------
# This module will use the cloudera API and CLI commands for the retrieval of
# Security related features. This module will spot the other third party tools
# whichever is integrated with the Hadoop cluster to enhanced security.
# ------------------------------------------------------------------------------

# Importing required libraries
from imports import *


class SecurityAPI:
    """This Class has functions related to the Security category.

    Has functions which fetch different security metrics from Hadoop 
    cluster like kerberos details, AD server details, etc.

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

    def clusterKerberosInfo(self, cluster_name):
        """Get Kerberos details in a cluster.
        
        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            cluster_kerberos_info (str): Kerberos information of cluster.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/clusters/{}/kerberosInfo".format(
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
                    "{}://{}:{}/api/v19/clusters/{}/kerberosInfo".format(
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
                    "{}://{}:{}/api/v19/clusters/{}/kerberosInfo".format(
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
                cluster_kerberos_info = r.json()
                kerberized_status = str(cluster_kerberos_info["kerberized"])
                if kerberized_status == "True":
                    cluster_kerberos_info = "Cluster is kerberized"
                else:
                    cluster_kerberos_info = "Cluster is not kerberized"
                self.logger.info("clusterKerberosInfo successful")
                return cluster_kerberos_info
            else:
                self.logger.error(
                    "clusterKerberosInfo failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
        except Exception as e:
            self.logger.error("clusterKerberosInfo failed", exc_info=True)
            return None

    def ADServerNameAndPort(self, cluster_name):
        """Get AD server details for a cluster.
        
        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            ADServer (str): Url and port of AD server.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/cm/deployment".format(
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
                    "{}://{}:{}/api/v19/cm/deployment".format(
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
                    "{}://{}:{}/api/v19/cm/deployment".format(
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
                ad_server = r.json()
                ADServer = None
                with open(
                    "Discovery_Report/{}/AD_server_port.json".format(cluster_name), "w"
                ) as fp:
                    json.dump(ad_server, fp, indent=4)
                ad_server = ad_server["managerSettings"]
                for i in ad_server["items"]:
                    if i["name"] == "LDAP_URL":
                        ADServer = i["value"]
                self.logger.info("ADServerNameAndPort successful")
                return ADServer
            else:
                self.logger.error(
                    "ADServerNameAndPort failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("ADServerNameAndPort failed", exc_info=True)
            return None

    def adServerBasedDN(self, cluster_name):
        """Get AD server details based on domain name.

        Args:
            cluster_name (str): Cluster name present in cloudera manager.
        Returns:
            Server_dn (str): Domain name of LDAP bind parameter.
        """

        try:
            r = None
            if self.version == 7:
                r = requests.get(
                    "{}://{}:{}/api/v41/cm/deployment".format(
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
                    "{}://{}:{}/api/v19/cm/deployment".format(
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
                    "{}://{}:{}/api/v19/cm/deployment".format(
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
                ad_server = r.json()
                Server_dn = None
                with open(
                    "Discovery_Report/{}/AD_server_DN.json".format(cluster_name), "w"
                ) as fp:
                    json.dump(ad_server, fp, indent=4)
                ad_server = ad_server["managerSettings"]
                for i in ad_server["items"]:
                    if i["name"] == "LDAP_BIND_DN":
                        Server_dn = i["value"]
                self.logger.info("adServerBasedDN successful")
                return Server_dn
            else:
                self.logger.error(
                    "adServerBasedDN failed due to invalid API call. HTTP Response: ",
                    r.status_code,
                )
                return None
        except Exception as e:
            self.logger.error("adServerBasedDN failed", exc_info=True)
            return None

    def keytabFilesInfo(self):
        """Get AD server details based on domain name.

        Returns:
            keytab (str): Keytab files information.
        """

        try:
            keytab = None
            os.popen(
                'find / -iname "*.keytab" > ./keytab.txt 2>/dev/null',
                # stdout=subprocess.DEVNULL,
                # stderr=subprocess.STDOUT,
            ).read()
            with open("./keytab.txt", "r") as read_obj:
                for line in read_obj:
                    if "keytab" in line:
                        keytab = "Keytab exist"
                        break
                    else:
                        keytab = "Keytab not exist"
                        break
            os.popen("rm ./keytab.txt")
            self.logger.info("keytabFilesInfo successful")
            return keytab
        except Exception as e:
            self.logger.error("keytabFilesInfo failed", exc_info=True)
            return None

    def sslStatus(self):
        """Get SSL staus of various services.

        Returns:
            Mr_ssl (str): MapReduce SSL status
            hdfs_ssl (str): HDFS SSL status
            yarn_ssl (str): Yarn SSL status
        """

        try:
            path_status = path.exists("/etc/hadoop/conf/hdfs-site.xml")
            if path_status == True:
                xml_data = subprocess.Popen(
                    "cat /etc/hadoop/conf/hdfs-site.xml | grep HTTPS_ONLY",
                    shell=True,
                    stdout=subprocess.PIPE,
                    encoding="utf-8",
                )
                out, err = xml_data.communicate()
                if out.find("HTTPS_ONLY") == -1:
                    hdfs_ssl = "SSL on HDFS is not enabled"
                else:
                    hdfs_ssl = "SSL on HDFS is enabled"
            else:
                hdfs_ssl = None
            path_status = path.exists("/etc/hadoop/conf/yarn-site.xml")
            if path_status == True:
                xml_data = subprocess.Popen(
                    "cat /etc/hadoop/conf/yarn-site.xml | grep HTTPS_ONLY",
                    shell=True,
                    stdout=subprocess.PIPE,
                    encoding="utf-8",
                )
                out, err = xml_data.communicate()
                if out.find("HTTPS_ONLY") == -1:
                    yarn_ssl = "SSL on Yarn is not enabled"
                else:
                    yarn_ssl = "SSL on Yarn is enabled"
            else:
                yarn_ssl = None
            path_status = path.exists("/etc/hadoop/conf/mapred-site.xml")
            if path_status == True:
                xml_data = subprocess.Popen(
                    "cat /etc/hadoop/conf/mapred-site.xml | grep HTTPS_ONLY",
                    shell=True,
                    stdout=subprocess.PIPE,
                    encoding="utf-8",
                )
                out, err = xml_data.communicate()
                if out.find("HTTPS_ONLY") == -1:
                    Mr_ssl = "SSL on Mapreduce is not enabled"
                else:
                    Mr_ssl = "SSL on Mapreduce is enabled"
            else:
                Mr_ssl = None
            self.logger.info("sslStatus successful")
            return Mr_ssl, hdfs_ssl, yarn_ssl
        except Exception as e:
            self.logger.error("sslStatus failed", exc_info=True)
            return None

    def kerberosHttpAuth(self):
        """Get kerberos status of various services.

        Returns:
            hue_flag (str): Hue kerberos status
            mapred_flag (str): MapReduce kerberos status
            hdfs_flag (str): HDFS kerberos status
            yarn_flag_1 (str): Yarn kerberos status
            yarn_flag_2 (str): Yarn kerberos status
        """

        try:
            hue_flag = 0
            yarn_flag_1 = 0
            yarn_flag_2 = 0
            mapred_flag = 0
            hdfs_flag = 0
            path_status = path.exists("/etc/hue/conf/hue.ini")
            if path_status == True:
                dt1 = subprocess.Popen(
                    "cat /etc/hue/conf/hue.ini",
                    shell=True,
                    stdout=subprocess.PIPE,
                    encoding="utf-8",
                )
                out, err = dt1.communicate()
                var_dt = []
                out1 = out.splitlines()
                get_dt = 0
                var_dt = []
                res_dt = ["## kinit_path="]
                for i in out1:
                    if "[[kerberos]]" in i:
                        get_dt = 1
                    if (get_dt == 1) and " Configuration options for" not in i:
                        var_dt.append(i)
                    if "Configuration options for" in i:
                        get_dt = 0
                var_dt = [x.lstrip() for x in var_dt]
                var_dt = "".join(map(str, var_dt))
                if var_dt.find("## kinit_path=") != -1:
                    hue_flag = 0
                else:
                    hue_flag = 1
            else:
                hue_flag = None
            path_status = path.exists("/etc/hadoop/conf/yarn-site.xmll")
            if path_status == True:
                xml_data = subprocess.check_output(
                    "cat /etc/hadoop/conf/yarn-site.xml",
                    shell=True,
                    stderr=subprocess.STDOUT,
                )
                root = ET.fromstring(xml_data)
                for val in root.findall("property"):
                    name = val.find("name").text
                    if "yarn.resourcemanager.keytab" not in name:
                        root.remove(val)
                if len(list(root)) == 0:
                    yarn_flag_1 = 0
                else:
                    yarn_flag_1 = 1
            else:
                yarn_flag1 = None
            path_status = path.exists("/etc/hadoop/conf/yarn-site.xml")
            if path_status == True:
                xml_data2 = subprocess.check_output(
                    "cat /etc/hadoop/conf/yarn-site.xml",
                    shell=True,
                    stderr=subprocess.STDOUT,
                )
                root2 = ET.fromstring(xml_data2)
                for val2 in root2.findall("property"):
                    name2 = val2.find("name").text
                    if "yarn.nodemanager.keytab" not in name2:
                        root2.remove(val2)
                if len(list(root2)) == 0:
                    yarn_flag_2 = 0
                else:
                    yarn_flag_2 = 1
            else:
                yarn_flag2 = None
            path_status = path.exists("/etc/hadoop/conf/mapred-site.xml")
            if path_status == True:
                xml_data3 = subprocess.check_output(
                    "cat /etc/hadoop/conf/mapred-site.xml",
                    shell=True,
                    stderr=subprocess.STDOUT,
                )
                root3 = ET.fromstring(xml_data3)
                for val3 in root3.findall("property"):
                    name3 = val3.find("name").text
                    if "mapreduce.jobhistory.keytab" not in name3:
                        root3.remove(val3)
                if len(list(root3)) == 0:
                    mapred_flag = 0
                else:
                    mapred_flag = 1
            else:
                mapred_flag = None
            path_status = path.exists("/etc/hadoop/conf/hdfs-site.xml")
            if path_status == True:
                xml_data = subprocess.Popen(
                    "cat /etc/hadoop/conf/hdfs-site.xml | grep kerberos.principal",
                    shell=True,
                    stdout=subprocess.PIPE,
                    encoding="utf-8",
                )
                out, err = xml_data.communicate()
                if not out:
                    hdfs_flag = 1
                else:
                    hdfs_flag = 0
            else:
                hdfs_flag = None
            self.logger.info("kerberosHttpAuth successful")
            return hue_flag, hdfs_flag, yarn_flag_1, yarn_flag_2, mapred_flag
        except Exception as e:
            self.logger.error("kerberosHttpAuth failed", exc_info=True)
            return None

    def checkLuks(self):
        """Get LUKS information in cluster.

        Returns:
            luks_detect (str): LUKS information.
        """

        try:
            os.popen("blkid > block.csv").read()
            columns = [
                "block",
                "section",
                "UUID",
                "TYPE",
                "part1",
                "part2",
                "part3",
                "part4",
            ]
            luks_detect = pd.read_csv(
                "block.csv", names=columns, delimiter=r"\s+", header=None
            )
            os.popen("rm block.csv").read()
            luks_detect.drop(
                columns=["UUID", "part1", "part2", "part3", "part4"], inplace=True
            )
            luks_detect["TYPE_LOWER"] = luks_detect["TYPE"].str.lower()
            self.logger.info("checkLuks successful")
            return luks_detect
        except Exception as e:
            self.logger.error("checkLuks failed", exc_info=True)
            return None

    def portUsed(self):
        """Get port number for different services.

        Returns:
            port_df (DataGrame): port number for different services.
        """

        try:
            port_df = pd.DataFrame(columns=["service", "port"])
            os.popen("find / -name oozie-site.xml > oozie_port.csv").read()
            with open("oozie_port.csv") as fp:
                for line in fp:
                    if "-oozie-OOZIE_SERVER/oozie-site.xml" in line:
                        xml_oozie = line
            os.popen("rm oozie_port.csv").read()
            dt_xml = os.popen("cat " + xml_oozie).read()
            myxml = fromstring(dt_xml)
            for val in myxml.findall("property"):
                name = val.find("name").text
                if "oozie.base.url" not in name:
                    myxml.remove(val)
            value = myxml[0][1].text
            value = " ".join(value.split(":", 2)[2:])
            value = " ".join(value.split("/", 1)[:1])
            if line == "":
                line = pd.NaT
                df_port = {"service": "Oozie Port", "port": pd.NaT}
            else:
                line = line
                df_port = {"service": "Oozie Port", "port": value}
            port_df = port_df.append(df_port, ignore_index=True)
            hdfs_line = ""
            path_status = path.exists("/etc/hadoop/conf/core-site.xml")
            if path_status == True:
                xml_data = os.popen("cat /etc/hadoop/conf/core-site.xml").read()
                root = ET.fromstring(xml_data)
                for val in root.findall("property"):
                    name = val.find("name").text
                    if "fs.defaultFS" not in name:
                        root.remove(val)
                value = root[0][1].text
                value = " ".join(value.split(":", 2)[2:])
                if value == "":
                    line = pd.NaT
                    df_port = {"service": "HDFS Port", "port": pd.NaT}
                else:
                    line = hdfs_line
                    df_port = {"service": "HDFS Port", "port": value}
                port_df = port_df.append(df_port, ignore_index=True)
            yarn_line = ""
            path_status = path.exists("/etc/hadoop/conf/yarn-site.xml")
            if path_status == True:
                xml_data = os.popen("cat /etc/hadoop/conf/yarn-site.xml").read()
                root = ET.fromstring(xml_data)
                for val in root.findall("property"):
                    name = val.find("name").text
                    if "yarn.resourcemanager.address" not in name:
                        root.remove(val)
                value = root[0][1].text
                value = " ".join(value.split(":", 2)[1:])
                if value == "":
                    line = pd.NaT
                    df_port = {"service": "Yarn Port", "port": pd.NaT}
                else:
                    line = yarn_line
                    df_port = {"service": "Yarn Port", "port": value}
                port_df = port_df.append(df_port, ignore_index=True)
            mapred_line = ""
            path_status = path.exists("/etc/hadoop/conf/mapred-site.xml")
            if path_status == True:
                xml_data = os.popen("cat /etc/hadoop/conf/mapred-site.xml").read()
                root = ET.fromstring(xml_data)
                for val in root.findall("property"):
                    name = val.find("name").text
                    if "mapreduce.jobhistory.address" not in name:
                        root.remove(val)
                value = root[0][1].text
                value = " ".join(value.split(":", 2)[1:])
                if value == "":
                    line = pd.NaT
                    df_port = {"service": "Mapreduce Port", "port": pd.NaT}
                else:
                    line = mapred_line
                    df_port = {"service": "Mapreduce Port", "port": value}
                port_df = port_df.append(df_port, ignore_index=True)
            kafka_line = ""
            path_status = path.exists("/etc/kafka/server.properties")
            if path_status == True:
                os.popen("cat /etc/kafka/server.properties > kafka_port.csv").read()
                with open("kafka_port.csv") as fp:
                    for kafka_line in fp:
                        if "listeners=PLAINTEXT://" in kafka_line:
                            break
                os.popen("rm kafka_port.csv").read()
                kafka_line = " ".join(kafka_line.split(":", 2)[2:])
                if kafka_line == "":
                    line = pd.NaT
                    df_port = {"service": "Kafka Port", "port": pd.NaT}
                else:
                    line = kafka_line
                    df_port = {"service": "Kafka Port", "port": line.rstrip()}
                port_df = port_df.append(df_port, ignore_index=True)
            spark_line = ""
            path_status = path.exists("/etc/spark/conf/spark-defaults.conf")
            if path_status == True:
                os.popen(
                    "cat /etc/spark/conf/spark-defaults.conf > spark_data.csv"
                ).read()
                with open("spark_data.csv") as fp:
                    for spark_line in fp:
                        if "spark.shuffle.service.port" in spark_line:
                            break
                os.popen("rm spark_data.csv").read()
                spark_line = " ".join(spark_line.split("=", 1)[1:])
                if spark_line == "":
                    line = pd.NaT
                    df_port = {"service": "Spark Port", "port": pd.NaT}
                else:
                    line = spark_line
                    df_port = {"service": "Spark Port", "port": line.rstrip()}
                port_df = port_df.append(df_port, ignore_index=True)
            kerberos_line = ""
            path_status = path.exists("/var/kerberos/krb5kdc/kdc.conf")
            if path_status == True:
                os.popen("cat /var/kerberos/krb5kdc/kdc.conf > spark_data.csv").read()
                with open("spark_data.csv") as fp:
                    for kerberos_line in fp:
                        if "kdc_tcp_ports" in kerberos_line:
                            break
                os.popen("rm spark_data.csv").read()
                kerberos_line = " ".join(kerberos_line.split("=", 1)[1:])
                if kerberos_line == "":
                    line = pd.NaT
                    df_port = {"service": "Kerberos Port", "port": pd.NaT}
                else:
                    line = kerberos_line
                    df_port = {"service": "Kerberos Port", "port": line.rstrip()}
                port_df = port_df.append(df_port, ignore_index=True)
            zookeeper_line = ""
            dt = os.popen('find / -name "zoo.cfg"').read()
            res_list = dt.splitlines()
            for i in res_list:
                if "/etc/zookeeper/conf.dist/zoo.cfg" in i:
                    intermediate_list = os.popen("cat " + i).read()
                    new_res_list = intermediate_list.splitlines()
                    res = [string for string in new_res_list if "clientPort=" in string]
                    listToStr = " ".join([str(elem) for elem in res])
                    zookeeper_line = " ".join(listToStr.split("=", 1)[1:])
            if line == "":
                line = pd.NaT
                df_port = {"service": "Zookeeper Port", "port": pd.NaT}
            else:
                line = zookeeper_line
                df_port = {"service": "Zookeeper Port", "port": line.rstrip()}
            port_df = port_df.append(df_port, ignore_index=True)
            port_df = port_df.dropna()
            self.logger.info("portUsed successful")
            return port_df
        except Exception as e:
            self.logger.error("portUsed failed", exc_info=True)
            return None

    def keyList(self):
        """Get list of keys in cluster.

        Returns:
            key_list (str): list of keys.
        """

        try:
            key_list = subprocess.Popen(
                "hadoop key list", shell=True, stdout=subprocess.PIPE, encoding="utf-8"
            )
            out, err = key_list.communicate()
            out = out.splitlines()
            out1 = str(out)
            substring = "no valid (non-transient) providers"
            substring_in_list = any(substring in out1 for string in out)
            if substring_in_list == True:
                key_list = None
            else:
                out = out[1:]
                key_list = out
                key_list = ", ".join(key_list)
            self.logger.info("keyList successful")
            return key_list
        except Exception as e:
            self.logger.error("keyList failed", exc_info=True)
            return None

    def encryptionZone(self):
        """Get list of encryption zone in cluster.

        Returns:
            enc_zoneList (DataGrame): list of encryption zone.
        """

        try:
            enc_zoneList = pd.DataFrame()
            xml_data = subprocess.Popen(
                "sudo hdfs crypto -listZones",
                shell=True,
                stdout=subprocess.PIPE,
                encoding="utf-8",
            )
            out, err = xml_data.communicate()
            if not out.strip():
                enc_zoneList = None
            else:
                intermediate_out = out.splitlines()
                intermediate_out.pop(-1)
                splitted_search = [x.split("\n") for x in intermediate_out]
                enc_zoneList = pd.DataFrame(splitted_search, columns=["data"])
                enc_zoneList["data"] = enc_zoneList["data"].str.split(
                    " ", n=1, expand=True
                )
            self.logger.info("encryptionZone successful")
            return enc_zoneList
        except Exception as e:
            self.logger.error("encryptionZone failed", exc_info=True)
            return None
