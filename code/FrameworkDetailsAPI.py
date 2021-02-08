from imports import *
class FrameworkDetailsAPI:
        
    def hadoopVersion():
        hversion=os.popen('hadoop version').read()
        hadoop_major=hversion[0:12]
        os.popen('hadoop version > ./data.csv').read()
        dt="This command was run using "
        a=""
        with open('./data.csv') as fp:
            with open("./out2.csv", "w") as f1:
                for line in fp: 
                    if(dt in line):
                        a=line
                        a=line.replace("This command was run using /opt/cloudera/parcels/","").replace("/jars/hadoop-common-3.1.1.7.1.4.0-203.jar","").replace("","")
        hadoop_minor=a[0:9]
        os.popen('rm ./data.csv').read()
        os.popen('rm ./out2.csv').read() 
        distribution=""
        if re.search(r'\bcdh7\b', a):
            distribution="CDH7"
        elif re.search(r'\bcdh6\b', a):
            distribution="CDH6"
        elif re.search(r'\bcdh5\b', a):
            distribution="CDH5"
        return hadoop_major,hadoop_minor,distribution
    


    def versionMapping(clusterName):
        cluster_name=clusterName
        r = None
        if version == 7:
            r = requests.get('http://{}:7180/api/v41/clusters/{}/services'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 6:
            r = requests.get('http://{}:7180/api/v33/clusters/{}/services'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        elif version == 5:
            r = requests.get('http://{}:7180/api/v19/clusters/{}/services'.format(cloudera_manager_host_ip,cluster_name),auth = HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password))
        version_related = r.json()
        version_related_count = version_related['items']
        list_apache_services=[]
        for i in version_related_count:
            version_related_show=version_related
            for displayname in version_related_show['items']:
                displyName=displayname['displayName'].lower()
                list_apache_services.append(displyName)
        list_apache_services = list(set(list_apache_services)) 
        list_services_installed_df = pd.DataFrame (list_apache_services,columns=['name'])
        
        #Service Versions Mapping (YARN, Spark, Hive etc)
        version_data=json.loads(os.popen('cat /opt/cloudera/parcels/CDH/meta/parcel.json').read())
        data=version_data['components']
        df_service_version = pd.DataFrame(data)
        new_ref_df=list_services_installed_df.merge(df_service_version, how='left')
        new_ref_df_nan=new_ref_df[new_ref_df.isna().any(axis=1)]['name']
        for i in new_ref_df_nan.iteritems():
            found=df_service_version[df_service_version['name'].str.contains(i[1])]
            if found.empty:
                pass
            else:
                exist=new_ref_df[new_ref_df['name'].str.contains(i[1])]
                if exist.empty:
                      break
                else:
                    new_ref_df=new_ref_df.append(found)
        new_ref_df = new_ref_df.drop_duplicates()
        new_ref_df.dropna(subset = ['pkg_release'], inplace=True)
        new_ref_df['sub_version'] = new_ref_df.version.str[:5]
        new_ref_df=new_ref_df.drop(['version'], axis = 1) 
        new_ref_df=new_ref_df.reset_index(drop=True)
        return list_services_installed_df,new_ref_df
