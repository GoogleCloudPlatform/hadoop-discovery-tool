from imports import *
class HardwareOSAPI:
    
    def osVersion():
        os_version = os.popen('cat /etc/*-release').read()
        os_version=os_version.replace('\n',',')
        os_version = os_version.split(",")
        os_version_series = pd.Series(data=os_version).T
        os_version = os_version_series.iloc[18]
        return os_version



    def clusterItems():
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/clusters'.format(cloudera_manager_host_ip),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/clusters'.format(cloudera_manager_host_ip),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version ==5:
            r = requests.get('http://{}:7180/api/v19/clusters'.format(cloudera_manager_host_ip),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        cluster = r.json()
        with open('Discovery_Report/clusters.json', 'w') as fp:
            json.dump(cluster, fp,  indent=4)
        cluster_items = cluster['items']
        return cluster_items

    def clusterHostItems(clusterName):
        cluster_name=clusterName
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/clusters/{}/hosts'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/clusters/{}/hosts'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version ==5:
            r = requests.get('http://{}:7180/api/v19/clusters/{}/hosts'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        cluster_host = r.json()
        clusterHostLen=len(cluster_host['items'])
        with open('Discovery_Report/{}/clusters_host.json'.format(cluster_name), 'w') as fp:
            json.dump(cluster_host, fp,  indent=4)
        cluster_host_items = cluster_host['items']
        return cluster_host_items,clusterHostLen

    def clusterServiceItem(clusterName):
        cluster_name=clusterName
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/clusters/{}/services'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/clusters/{}/services'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/clusters/{}/services'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        cluster_services = r.json()
        with open('Discovery_Report/{}/cluster_services.json'.format(cluster_name), 'w') as fp:
            json.dump(cluster_services, fp,  indent=4)
        cluster_service_item = cluster_services['items']
        return cluster_service_item


    def clusterKerberosInfo(clusterName):
        cluster_name=clusterName
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/clusters/{}/kerberosInfo'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/clusters/{}/kerberosInfo'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/clusters/{}/kerberosInfo'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        cluster_kerberos_info = r.json()
        return cluster_kerberos_info

    def hostData(hostId):
        hostid=hostId
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/hosts/{}'.format(cloudera_manager_host_ip,hostid),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/hosts/{}'.format(cloudera_manager_host_ip,hostid),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/hosts/{}'.format(cloudera_manager_host_ip,hostid),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        host_data = r.json()
        return host_data

    def getHadoopDetails(clusterName):
        cluster_name=clusterName
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/clusters/{}/services'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/clusters/{}/services'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/clusters/{}/services'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        r_data = r.json()
        hadoop_distro=r_data['items'][0]['serviceVersion']
        return hadoop_distro
    
    def clusterTotalCores(clusterName):
        cluster_name=clusterName
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_cores_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_cores_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_cores_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        cluster_total_cores=r.json()
        with open('Discovery_Report/{}/cluster_total_cores.json'.format(cluster_name), 'w') as fp:
            json.dump(cluster_total_cores, fp,  indent=4)
        cluster_total_cores_list = cluster_total_cores['items'][0]['timeSeries'][0]['data']
        cluster_total_cores_df = pd.DataFrame(cluster_total_cores_list)
        cluster_total_cores_df = pd.DataFrame({'DateTime' : pd.to_datetime(cluster_total_cores_df['timestamp']).dt.strftime("%Y-%m-%d %H:%M"),'Mean' : cluster_total_cores_df['value']})
        cluster_total_cores_df['DateTime'] = pd.to_datetime(cluster_total_cores_df['DateTime'])
        cluster_total_cores_df = pd.DataFrame(pd.date_range(cluster_total_cores_df['DateTime'].min(),cluster_total_cores_df['DateTime'].max(),freq='1H'),columns= ['DateTime']).merge(cluster_total_cores_df,on=['DateTime'],how='outer').fillna(0)
        cluster_total_cores_df['Time'] = cluster_total_cores_df.DateTime.dt.strftime('%d-%b %H:%M')
        cluster_total_cores_df = cluster_total_cores_df.set_index('Time')
        return cluster_total_cores_df
     
    def clusterCpuUsage(clusterName):
        cluster_name=clusterName
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20cpu_percent_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20cpu_percent_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20cpu_percent_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        cluster_cpu_usage=r.json()
        with open('Discovery_Report/{}/cluster_cpu_usage.json'.format(cluster_name), 'w') as fp:
            json.dump(cluster_cpu_usage, fp,  indent=4)
        cluster_cpu_usage_list = cluster_cpu_usage['items'][0]['timeSeries'][0]['data']
        cluster_cpu_usage_df = pd.DataFrame(cluster_cpu_usage_list) 
        cluster_cpu_usage_df = pd.DataFrame({'DateTime' : pd.to_datetime(cluster_cpu_usage_df['timestamp']).dt.strftime("%Y-%m-%d %H:%M"),'Mean' : cluster_cpu_usage_df['value'],'Min' : cluster_cpu_usage_df['aggregateStatistics'].apply(pd.Series)['min'],'Max' : cluster_cpu_usage_df['aggregateStatistics'].apply(pd.Series)['max']})
        cluster_cpu_usage_df['DateTime'] = pd.to_datetime(cluster_cpu_usage_df['DateTime'])
        cluster_cpu_usage_avg = cluster_cpu_usage_df['Mean'].sum()/cluster_cpu_usage_df['DateTime'].count()
        cluster_cpu_usage_df = pd.DataFrame(pd.date_range(cluster_cpu_usage_df['DateTime'].min(),cluster_cpu_usage_df['DateTime'].max(),freq='H'),columns= ['DateTime']).merge(cluster_cpu_usage_df,on=['DateTime'],how='outer').fillna(0)
        cluster_cpu_usage_df['Time'] = cluster_cpu_usage_df.DateTime.dt.strftime('%d-%b %H:%M')
        cluster_cpu_usage_df = cluster_cpu_usage_df.set_index('Time')
        return cluster_cpu_usage_df,cluster_cpu_usage_avg

    def clusterTotalMemory(clusterName):
        cluster_name=clusterName
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        cluster_total_memory=r.json()
        with open('Discovery_Report/{}/cluster_total_memory.json'.format(cluster_name), 'w') as fp:
            json.dump(cluster_total_memory, fp,  indent=4)
        cluster_total_memory_list = cluster_total_memory['items'][0]['timeSeries'][0]['data']
        cluster_total_memory_df = pd.DataFrame(cluster_total_memory_list)
        cluster_total_memory_df = pd.DataFrame({'DateTime' : pd.to_datetime(cluster_total_memory_df['timestamp']).dt.strftime("%Y-%m-%d %H:%M"),'Mean' : cluster_total_memory_df['value']/1024/1024/1024})
        cluster_total_memory_df['DateTime'] = pd.to_datetime(cluster_total_memory_df['DateTime'])
        cluster_total_memory_df = pd.DataFrame(pd.date_range(cluster_total_memory_df['DateTime'].min(),cluster_total_memory_df['DateTime'].max(),freq='1H'),columns= ['DateTime']).merge(cluster_total_memory_df,on=['DateTime'],how='outer').fillna(0)
        cluster_total_memory_df['Time'] = cluster_total_memory_df.DateTime.dt.strftime('%d-%b %H:%M')
        cluster_total_memory_df = cluster_total_memory_df.set_index('Time')
        return cluster_total_memory_df
     
    def clusterMemoryUsage(clusterName):
        cluster_name=clusterName
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20100*total_physical_memory_used_across_hosts/total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20100*total_physical_memory_used_across_hosts/total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=SELECT%20%20%20%20100*total_physical_memory_used_across_hosts/total_physical_memory_total_across_hosts%20WHERE%20%20%20%20category%3DCLUSTER%20%20%20%20AND%20clusterName%3D{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        cluster_memory_usage=r.json()
        with open('Discovery_Report/{}/cluster_memory_usage.json'.format(cluster_name), 'w') as fp:
            json.dump(cluster_memory_usage, fp,  indent=4)
        cluster_memory_usage_list = cluster_memory_usage['items'][0]['timeSeries'][0]['data']
        cluster_memory_usage_df = pd.DataFrame(cluster_memory_usage_list) 
        cluster_memory_usage_df = pd.DataFrame({'DateTime' : pd.to_datetime(cluster_memory_usage_df['timestamp']).dt.strftime("%Y-%m-%d %H:%M"),'Mean' : cluster_memory_usage_df['value']})
        cluster_memory_usage_df['DateTime'] = pd.to_datetime(cluster_memory_usage_df['DateTime'])
        cluster_memory_usage_avg = cluster_memory_usage_df['Mean'].sum()/cluster_memory_usage_df['DateTime'].count()
        cluster_memory_usage_df = pd.DataFrame(pd.date_range(cluster_memory_usage_df['DateTime'].min(),cluster_memory_usage_df['DateTime'].max(),freq='H'),columns= ['DateTime']).merge(cluster_memory_usage_df,on=['DateTime'],how='outer').fillna(0)
        cluster_memory_usage_df['Time'] = cluster_memory_usage_df.DateTime.dt.strftime('%d-%b %H:%M')
        cluster_memory_usage_df = cluster_memory_usage_df.set_index('Time')
        return cluster_memory_usage_df,cluster_memory_usage_avg
