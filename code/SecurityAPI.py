# Importing Required Libraries
from imports import *


# This Class has functions related to Security category
class SecurityAPI:
    # Initialize Inputs
    def __init__(self, inputs):
        self.inputs = inputs
        self.version = inputs["version"]
        self.cloudera_manager_host_ip = inputs["cloudera_manager_host_ip"]
        self.cloudera_manager_username = inputs["cloudera_manager_username"]
        self.cloudera_manager_password = inputs["cloudera_manager_password"]
        self.cluster_name = inputs["cluster_name"]
        self.logger = inputs["logger"]

    # Get Kerberos details in a cluster
    def clusterKerberosInfo(self, clusterName):
        try:
            cluster_name = clusterName
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/clusters/{}/kerberosInfo".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "http://{}:7180/api/v33/clusters/{}/kerberosInfo".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "http://{}:7180/api/v19/clusters/{}/kerberosInfo".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            if r.status_code == 200:
                cluster_kerberos_info = r.json()
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

    # Get AD server details for a cluster
    def ADServerNameAndPort(self, clusterName):
        try:
            cluster_name = clusterName
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/cm/deployment".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "http://{}:7180/api/v33/cm/deployment".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "http://{}:7180/api/v19/cm/deployment".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            if r.status_code == 200:
                ad_server = r.json()
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

    # Get AD server details based on domain name
    def adServerBasedDN(self, clusterName):
        try:
            cluster_name = clusterName
            r = None
            if self.version == 7:
                r = requests.get(
                    "http://{}:7180/api/v41/cm/deployment".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 6:
                r = requests.get(
                    "http://{}:7180/api/v33/cm/deployment".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            elif self.version == 5:
                r = requests.get(
                    "http://{}:7180/api/v19/cm/deployment".format(
                        self.cloudera_manager_host_ip, cluster_name
                    ),
                    auth=HTTPBasicAuth(
                        self.cloudera_manager_username, self.cloudera_manager_password
                    ),
                )
            if r.status_code == 200:
                ad_server = r.json()
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
