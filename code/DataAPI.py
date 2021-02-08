from imports import *
class DataAPI:

    def totalSizeConfigured():
        os.popen('hdfs dfsadmin -report > ./data.csv').read()
        dt="Live datanodes "
        list_of_results=[]           
        flag=0
        with open('data.csv') as fp:
            with open("out2.csv", "w") as f1:
                for line in fp: 
                    if(dt in line):
                        flag=1
                    if(dt not in line and flag==1):
                        line = line.replace(': ','  ')
                        f1.write(re.sub('[^\S\r\n]{2,}',',',line))
        dataframe = pd.read_csv("out2.csv")
        dataframe.dropna(axis=0, how='all',inplace=True)
        dataframe.to_csv('sizeConfigured.csv', index=False)
        dataframe = pd.read_csv("sizeConfigured.csv",names=['key','value'])
        list_Hostnames = []
        list_Configured_Capacity = []
        count_row = dataframe.shape[0]
        for i in range(count_row) : 
            if(dataframe.loc[i, "key"] == 'Hostname'):
                list_Hostnames.append(dataframe.loc[i, "value"])
            if(dataframe.loc[i, "key"] == 'Configured Capacity'):
                list_Configured_Capacity.append(dataframe.loc[i, "value"])            
        dictionary = {'Hostname':list_Hostnames,'Configured_Capacity':list_Configured_Capacity}
        mapped_df = pd.DataFrame(dictionary)
        os.popen('rm data.csv').read()   
        os.popen('rm out2.csv').read() 
        mapped_df[['Configured_Capacity_bytes','Configured_Capacity']]= mapped_df.Configured_Capacity.str.split('\(|\)', expand=True).iloc[:,[0,1]]
        mapped_df['Configured_Capacity_bytes'] = mapped_df['Configured_Capacity_bytes'].astype(int)
        total_storage = (mapped_df['Configured_Capacity_bytes'].sum())/(1024*1024*1024)
        individual_node_size = mapped_df['Configured_Capacity'].tolist()
        return individual_node_size,total_storage

    def replicationFactor():
        replication_factor=os.popen('hdfs getconf -confKey dfs.replication').read()
        return replication_factor
        
    def getTrashStatus():
        xml_data=os.popen('cat /etc/hadoop/conf/core-site.xml').read()
        root = ET.fromstring(xml_data)
        for val in root.findall('property'):
            name = val.find('name').text
            if 'trash' not in name:
                root.remove(val)
        trash_value=int(root[0][1].text)
        trash_flag=""
        if trash_value > 0:
            trash_flag="Enabled"
        else:
            trash_flag="Disabled"
        return trash_flag

    def getCliresult(clipath):
        path=clipath
        out = subprocess.Popen(['hadoop','fs','-du','-h',path],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout,stderr = out.communicate()
        hdfs_root_dir = stdout
        return hdfs_root_dir



    def getHdfsCapacity(clusterName):
        cluster_name=clusterName
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        hdfs_capacity=r.json()
        with open('Discovery_Report/{}/hdfs_capacity.json'.format(cluster_name), 'w') as fp:
            json.dump(hdfs_capacity, fp,  indent=4)
        hdfs_capacity_list = hdfs_capacity['items'][0]['timeSeries'][0]['data']
        hdfs_capacity_df = pd.DataFrame(hdfs_capacity_list)
        hdfs_capacity_df = pd.DataFrame({'DateTime' : pd.to_datetime(hdfs_capacity_df['timestamp']).dt.strftime("%Y-%m-%d %H:%M"),'Mean' : hdfs_capacity_df['value']/1024/1024/1024})
        hdfs_capacity_df['DateTime'] = pd.to_datetime(hdfs_capacity_df['DateTime'])
        hdfs_capacity_df = pd.DataFrame(pd.date_range(hdfs_capacity_df['DateTime'].min(),hdfs_capacity_df['DateTime'].max(),freq='H'),columns= ['DateTime']).merge(hdfs_capacity_df,on=['DateTime'],how='outer').fillna(0)
        hdfs_capacity_df['Time'] = hdfs_capacity_df.DateTime.dt.strftime('%d-%b %H:%M')
        hdfs_capacity_df = hdfs_capacity_df.set_index('Time')
        return hdfs_capacity_df

    def getHdfsCapacityUsed(clusterName):
        cluster_name=clusterName
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity_used%2Bdfs_capacity_used_non_hdfs%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity_used%2Bdfs_capacity_used_non_hdfs%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/timeseries?contentType=application%2Fjson&from={}&desiredRollup=HOURLY&mustUseDesiredRollup=true&query=select%20dfs_capacity_used%2Bdfs_capacity_used_non_hdfs%20where%20entityName%3Dhdfs%20and%20clusterName%20%3D%20{}&to={}'.format(cloudera_manager_host_ip,start_date,cluster_name,end_date),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        hdfs_capacity_used=r.json()
        with open('Discovery_Report/{}/hdfs_capacity_used.json'.format(cluster_name), 'w') as fp:
            json.dump(hdfs_capacity_used, fp,  indent=4)
        hdfs_capacity_used_list = hdfs_capacity_used['items'][0]['timeSeries'][0]['data']
        hdfs_capacity_used_df = pd.DataFrame(hdfs_capacity_used_list)
        hdfs_capacity_used_df = pd.DataFrame({'DateTime' : pd.to_datetime(hdfs_capacity_used_df['timestamp']).dt.strftime("%Y-%m-%d %H:%M"),'Mean' : hdfs_capacity_used_df['value']/1024/1024/1024})
        hdfs_capacity_used_df['DateTime'] = pd.to_datetime(hdfs_capacity_used_df['DateTime'])
        hdfs_capacity_used_avg = hdfs_capacity_used_df['Mean'].sum()/hdfs_capacity_used_df['DateTime'].count()
        hdfs_capacity_used_df = pd.DataFrame(pd.date_range(hdfs_capacity_used_df['DateTime'].min(),hdfs_capacity_used_df['DateTime'].max(),freq='H'),columns= ['DateTime']).merge(hdfs_capacity_used_df,on=['DateTime'],how='outer').fillna(0)
        hdfs_capacity_used_df['Time'] = hdfs_capacity_used_df.DateTime.dt.strftime('%d-%b %H:%M')
        hdfs_capacity_used_df = hdfs_capacity_used_df.set_index('Time')
        return hdfs_capacity_used_df
