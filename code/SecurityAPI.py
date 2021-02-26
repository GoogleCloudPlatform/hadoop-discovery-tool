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
                    "{}://{}:{}/api/v33/clusters/{}/kerberosInfo".format(
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
                    "{}://{}:{}/api/v33/cm/deployment".format(
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
                    "{}://{}:{}/api/v33/cm/deployment".format(
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
            keytab = ""
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
