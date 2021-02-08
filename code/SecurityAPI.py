from imports import *
class SecurityAPI:

    def ADServerNameAndPort(clusterName):
        cluster_name=clusterName
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/cm/deployment'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/cm/deployment'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/cm/deployment'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        ad_server = r.json()
        with open('Discovery_Report/{}/AD_server_port.json'.format(cluster_name), 'w') as fp:
            json.dump(ad_server, fp,  indent=4)
        ad_server = ad_server['managerSettings']
        for i in ad_server['items']:
            if i['name'] == 'LDAP_URL':
                ADServer = i['value']
        return ADServer

    def adServerBasedDN(clusterName):
        cluster_name=clusterName
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/cm/deployment'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/cm/deployment'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/cm/deployment'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        ad_server = r.json()
        with open('Discovery_Report/{}/AD_server_DN.json'.format(cluster_name), 'w') as fp:
            json.dump(ad_server, fp,  indent=4)
        ad_server = ad_server['managerSettings']
        for i in ad_server['items']:
            if i['name'] == 'LDAP_BIND_DN':
                Server_dn = i['value']
        return Server_dn
