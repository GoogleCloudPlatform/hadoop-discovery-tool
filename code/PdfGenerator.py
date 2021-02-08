from imports import *
from HardwareOSAPI import *
from FrameworkDetailsAPI import *
from DataAPI import *
from SecurityAPI import *
from ApplicationAPI import *
class PdfGenerator(HardwareOSAPI, DataAPI, FrameworkDetailsAPI, ApplicationAPI, SecurityAPI):

    def run_5():
        obj1=HardwareOSAPI
        obj2=DataAPI
        obj3=FrameworkDetailsAPI
        obj_app=ApplicationAPI
        yarn_rm = ""
        hive_server2 = ""
        #Using cloudera Api fetching cluster host and services
        if os.path.exists("Discovery_Report"):
            shutil.rmtree("Discovery_Report")
        os.makedirs("Discovery_Report")
        yarn_rm = ""
        hive_server2 = ""

        pdf = FPDF(format=(250,350))
        pdf.add_page()
        pdf.set_font('Arial', 'B', 16)
        pdf.cell(230, 10, "Hadoop Discovery Report",0,ln=1,align = 'C')
        pdf.set_font('Arial','', 12)
        pdf.cell(230, 8, "Report Date Range : Start  {} ".format(date_range_start),0,ln=1,align='R')
        pdf.cell(0, 8, "End  {} ".format(date_range_end),0,ln=1,align='R')
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Cluster Information",0,ln=1)
        pdf.set_font('Arial','', 12)
        cluster_items=obj1.clusterItems()
        
        #Number of cluster configured
        pdf.cell(230, 8, "Number of Cluster Configured : {}".format(len(cluster_items)),0,ln=1)
        pdf.cell(230, 8, "Cluster Details : ",0,ln=1)
        pdf.set_font('Arial','B', 11)
        cluster_df = pd.DataFrame(cluster_items,columns =['name','fullVersion','entityStatus'])
        cluster_df.index = cluster_df.index + 1
        cluster_df = cluster_df.rename(columns={"name": "Cluster Name", "fullVersion": "Cloudera Version","entityStatus": "Health Status"})
        pdf.set_fill_color(r = 66, g = 133, b = 244)
        #pdf.set_fill_color(r = 98, g = 98, b = 255)
        
        #Cluster Details
        pdf.set_text_color(r = 255, g = 255, b = 255)
        pdf.cell(40, 5, 'Cluster Name', 1, 0, 'C',True)
        pdf.cell(40, 5, 'Cloudera Version', 1, 0, 'C',True)
        pdf.cell(50, 5, 'Health Status', 1, 1,'C',True)
        pdf.set_text_color(r = 1, g = 1, b = 1)
        pdf.set_fill_color(r = 244, g = 244, b = 244)
        pdf.set_font('Arial','', 11)
        for pos in range(0, len(cluster_df)):
            x = pdf.get_x()
            y = pdf.get_y()
            if(y > 300):
                pdf.add_page()
                x = pdf.get_x()
                y = pdf.get_y()
            line_width = 1
            line_width = max(line_width,pdf.get_string_width(cluster_df['Cluster Name'].iloc[pos]))
            cell_y = line_width/39.0
            line_width = max(line_width,pdf.get_string_width(cluster_df['Cloudera Version'].iloc[pos]))
            cell_y = max(cell_y,line_width/39.0)
            line_width = max(line_width,pdf.get_string_width(cluster_df['Health Status'].iloc[pos]))
            cell_y = max(cell_y,line_width/49.0)     
            cell_y = line_width/39.0
            cell_y = math.ceil(cell_y)
            cell_y = max(cell_y,1)
            cell_y = cell_y*5
            
            line_width = pdf.get_string_width(cluster_df['Cluster Name'].iloc[pos])
            y_pos = line_width/39.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos,1)
            y_pos = cell_y/y_pos
            pdf.multi_cell(40, y_pos, "{}".format(cluster_df['Cluster Name'].iloc[pos]), 1, 'C',fill = True)
            pdf.set_xy(x+40,y)
            
            line_width = pdf.get_string_width(cluster_df['Cloudera Version'].iloc[pos])
            y_pos = line_width/39.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos,1)
            y_pos = cell_y/y_pos
            pdf.multi_cell(40, y_pos, "{}".format(cluster_df['Cloudera Version'].iloc[pos]), 1, 'C',fill = True)
            pdf.set_xy(x+80,y)
            
            line_width = pdf.get_string_width(cluster_df['Health Status'].iloc[pos])
            y_pos = line_width/49.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos,1)
            y_pos = cell_y/y_pos
            pdf.multi_cell(50, y_pos, "{}".format(cluster_df['Health Status'].iloc[pos]), 1, 'C',fill = True)
        cluster_count = 0
        host_counts = 0
        service_count = 0
        pdf.cell(190, 5, "",0,ln=1)
        pdf.set_font('Arial','', 12)
        
        #OS Version (Eg: Centos 7.8.2003)
        os_version = obj1.osVersion()
        
        clusterName=""
        for i in cluster_items:
            cluster_name = i['name']
            clusterName=cluster_name
            if os.path.exists("Discovery_Report/{}".format(cluster_name)):
                shutil.rmtree("Discovery_Report/{}".format(cluster_name))
            os.makedirs("Discovery_Report/{}".format(cluster_name))
            cluster_count = cluster_count+1
            
            #Listing details for Cluster
            pdf.cell(230, 8,"Listing Details for Cluster {} - {}".format(cluster_count,cluster_name),0,ln=1)
            cluster_host_items,clusterHostLen=obj1.clusterHostItems(cluster_name)
            if os.path.exists('Discovery_Report/{}/host_details.json'.format(cluster_name)):
                os.remove('Discovery_Report/{}/host_details.json'.format(cluster_name))
            
            #pdf.cell(230, 8, "Date range for the report : {} - {}".format(date_range_start,date_range_end),0,ln=1) 
            
            #Number of host in the cluster
            #pdf.cell(230, 8, "Number of host in the cluster : {}".format(clusterHostLen),0,ln=1)   
            pdf.cell(230, 8, "Number of Host               : {}".format(clusterHostLen),0,ln=1)
            host_df = pd.DataFrame(columns=['Hostname', 'Host ip','Number of cores','Physical Memory','Health Status','Distribution'])
            for j in cluster_host_items:
                host_counts = host_counts+1
                host_count = obj1.hostData(j['hostId'])
                for role in host_count['roleRefs']:
                    if "RESOURCEMANAGER" in role['roleName'].upper():
                        yarn_rm = host_count['ipAddress']
                    if "HIVESERVER2" in role['roleName'].upper():
                        hive_server2 = host_count['ipAddress']
                with open('Discovery_Report/{}/host_details.json'.format(cluster_name), 'a') as fp:
                    json.dump(host_count, fp,  indent=4)
                host_tmp_df = pd.DataFrame({'Hostname': host_count['hostname'], 'Host IP': host_count['ipAddress'],'Cores' : host_count['numCores'],'Memory' : "{: .2f} GB".format(float(host_count['totalPhysMemBytes'])/1024/1024/1024),'Health Status' : host_count['entityStatus'],}, index=[host_count])
                host_df = host_df.append(host_tmp_df)
            
            #Total Data Nodes in the cluster
            tot_datanode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                data_count =obj1.hostData(j['hostId'])
                for role in data_count['roleRefs']:
                    #re.search(r'\b' DATANODE r'\b', role['roleName'].upper()) re.search(r'\bNAMENODE\b', role['roleName'])
                    if re.search(r'\bDATANODE\b', role['roleName']):
                        #print(role['roleName'])
                        host_tmp_df_1 = pd.DataFrame({'Cores' : data_count['numCores'],'Memory' : "{: .2f}".format(float(data_count['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        tot_datanode_df = tot_datanode_df.append(host_tmp_df_1)
            var_total_datanode=tot_datanode_df['Memory'].tolist()
            len_datanodes=len(var_total_datanode)
            #pdf.cell(230, 8, "Number of Datanodes in the cluster : {}".format(len_datanodes),0,ln=1)
            pdf.cell(230, 8, "Number of DataNodes    : {}".format(len_datanodes),0,ln=1)
            

            #Total Name Nodes in the cluster
            tot_namenode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                node_count =obj1.hostData(j['hostId'])
                for role in node_count['roleRefs']:
                    #re.search(r'\b' NAMENODE r'\b', role['roleName'].upper()) re.search(r'\bNAMENODE\b', role['roleName'])
                    if re.search(r'\bNAMENODE\b', role['roleName']):
                        #print(role['roleName'])
                        host_tmp_df_1 = pd.DataFrame({'Cores' : node_count['numCores'],'Memory' : "{: .2f}".format(float(node_count['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        tot_namenode_df = tot_namenode_df.append(host_tmp_df_1)
            var_total_namemode=tot_namenode_df['Memory'].tolist()
            len_namenodes=len(var_total_namemode)
            #pdf.cell(230, 8, "Number of Namenodes in the cluster : {}".format(len_namenodes),0,ln=1)
            pdf.cell(230, 8, "Number of NameNodes  : {}".format(len_namenodes),0,ln=1)
            
            #Number of Edge Nodes
            num_edgenode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                redgenode=obj1.hostData(j['hostId'])
                for role in redgenode['roleRefs']:
                    if re.search(r'\bGATEWAY\b', role['roleName']) and "hdfs" in role['serviceName']:
                        host_tmp_df_1 = pd.DataFrame({'Host IP': redgenode['ipAddress']}, index=[host_count])
                        num_edgenode_df = num_edgenode_df.append(host_tmp_df_1)
            #pdf.cell(230, 8,"Number of Edge Nodes : {} ".format(len(num_edgenode_df)),0,ln=1)
            pdf.cell(230, 8,"Number of Edge Nodes  : {} ".format(len(num_edgenode_df)),0,ln=1)
            
            #Host Details
            pdf.cell(230, 8, "Host Details : ",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(90, 5, 'Hostname', 1, 0, 'C',True)
            pdf.cell(16, 5, 'Host IP', 1, 0, 'C',True)
            pdf.cell(10, 5, 'Cores', 1, 0, 'C',True)
            pdf.cell(20, 5, 'Memory', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Health Status', 1, 0, 'C',True)
            pdf.cell(65, 5, 'Distribution', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(host_df)):
                pdf.cell(90, 5, "{}".format(host_df['Hostname'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(16, 5, "{}".format(host_df['Host IP'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(10, 5, "{}".format(host_df['Cores'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(20, 5, "{}".format(host_df['Memory'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{}".format(host_df['Health Status'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(65, 5, os_version, 1, 1, 'C',True)   
                
            cluster_service_item=obj1.clusterServiceItem(cluster_name)
            service_df = pd.DataFrame(columns=['Service Name','Health Status','Health Concerns'])
            for k in cluster_service_item:
                if(k['serviceState']!='STARTED'):
                    continue
                service_count = service_count + 1
                concerns = ""
                if(k['entityStatus']!='GOOD_HEALTH'):
                    for l in k['healthChecks']:
                        if(l['summary']!='GOOD'):
                            if(concerns == ""):
                                concerns = l['name']
                            else:
                                concerns = concerns + "\n" + l['name']
                service_tmp_df = pd.DataFrame({'Service Name': k['name'], 'Health Status': k['entityStatus'],'Health Concerns' : concerns}, index=[service_count])
                service_df = service_df.append(service_tmp_df)
            
            #Services running in the cluster
            pdf.set_font('Arial','', 12)
            pdf.cell(230, 3, "",0,ln=1)
            pdf.cell(230, 8, "Services Running in the Cluster : ",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(60, 5, 'Service Name', 1, 0, 'C',True)
            pdf.cell(60, 5, 'Health Status', 1, 0, 'C',True)
            pdf.cell(90, 5, 'Health Concerns', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(service_df)):
                x = pdf.get_x()
                y = pdf.get_y()
                if(y > 300):
                    pdf.add_page()
                    x = pdf.get_x()
                    y = pdf.get_y()
                line_width = 0
                line_width = max(line_width,pdf.get_string_width(service_df['Service Name'].iloc[pos]))
                cell_y = line_width/59.0
                line_width = max(line_width,pdf.get_string_width(service_df['Health Status'].iloc[pos]))
                cell_y = max(cell_y,line_width/59.0)
                line_width = max(line_width,pdf.get_string_width(service_df['Health Concerns'].iloc[pos]))
                cell_y = max(cell_y,line_width/89.0)
                #cell_y = line_width/59.0
                cell_y = math.ceil(cell_y)
                cell_y = max(cell_y,1)
                cell_y = cell_y*5
                line_width = pdf.get_string_width(service_df['Service Name'].iloc[pos])
                y_pos = line_width/59.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(60, y_pos, "{}".format(service_df['Service Name'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+60,y)
                line_width = pdf.get_string_width(service_df['Health Status'].iloc[pos])
                y_pos = line_width/59.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(60, y_pos, "{}".format(service_df['Health Status'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+120,y)
                line_width = pdf.get_string_width(service_df['Health Concerns'].iloc[pos])
                y_pos = line_width/89.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(90, y_pos, "{}".format(service_df['Health Concerns'].iloc[pos]), 1, 'C',fill = True)
            
        for data in cluster_items: 
        #Memory Allocated per node  
            memory_edgenode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                edge_memory =obj1.hostData(j['hostId'])
                for role in edge_memory['roleRefs']:
                    #re.search(r'\b' NAMENODE r'\b', role['roleName'].upper()) re.search(r'\bNAMENODE\b', role['roleName'])
                    if re.search(r'\bGATEWAY\b', role['roleName']) and "hdfs" in role['serviceName']:
                        #print(role['roleName'])
                        host_tmp_df_1 = pd.DataFrame({'Nodename':role['roleName'],'Memory' : "{: .2f}".format(float(edge_memory['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        memory_edgenode_df = memory_edgenode_df.append(host_tmp_df_1)

            #print(memory_edgenode_df)
            if memory_edgenode_df.empty:                
                pdf.cell(230, 8,"Edgenode Detected : Zero",0,ln=1)
            else:
                pdf.cell(230, 8, "Memory Allocated to edge nodes : ",0,ln=1)
                pdf.set_font('Arial','B', 11)
                pdf.set_fill_color(r = 66, g = 133, b = 244)
                pdf.set_text_color(r = 255, g = 255, b = 255)
                pdf.cell(100, 5, 'Nodename', 1, 0, 'C',True)
                pdf.cell(30, 5, 'Memory', 1, 1, 'C',True)
                pdf.set_text_color(r = 1, g = 1, b = 1)
                pdf.set_fill_color(r = 244, g = 244, b = 244)
                pdf.set_font('Arial','', 11)
                for pos in range(0, len(memory_edgenode_df)):
                    # print(memory_edgenode_df['Nodename'])
                    # print(memory_edgenode_df['Memory'])
                    pdf.cell(100, 5, "{}".format(memory_edgenode_df['Nodename'].iloc[pos]), 1, 0, 'C',True)
                    pdf.cell(30, 5, "{}".format(memory_edgenode_df['Memory'].iloc[pos]), 1, 1, 'C',True)
            
            clints_onEdgenode=pd.DataFrame(columns=[])
            for j in cluster_host_items:
                edge_services =obj1.hostData(j['hostId'])
                for role in edge_services['roleRefs']:
                    #re.search(r'\b' NAMENODE r'\b', role['roleName'].upper()) re.search(r'\bNAMENODE\b', role['roleName'])
                    if "GATEWAY" in role['roleName']:
                        a=pd.DataFrame({'service':role['serviceName']}, index=[host_count])
                        clints_onEdgenode=clints_onEdgenode.append(a)
            clints_onEdgenode=clints_onEdgenode.drop_duplicates()
            pdf.cell(230, 8, "Clients Installed on Gateway : ",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(70, 5, 'Services', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(clints_onEdgenode)):
                pdf.cell(70, 5, "{}".format(clints_onEdgenode['service'].iloc[pos]), 1, 1, 'C',True)
            
            #Kerberos Details
            pdf.set_font('Arial','', 12)
            pdf.cell(230, 3, "",0,ln=1)
            pdf.cell(230, 8, "Kerberos Details :",0,ln=1)
            cluster_kerberos_info=obj1.clusterKerberosInfo(cluster_name)
            with open('Discovery_Report/{}/cluster_kerberos_info.json'.format(cluster_name), 'w') as fp:
                json.dump(cluster_kerberos_info, fp,  indent=4)
            kerberized_status = str(cluster_kerberos_info['kerberized'])
            if(kerberized_status=="True"):                
                pdf.cell(230, 5, "Cluster is Kerberized",0,ln=1)                 
            else:                               
                pdf.cell(230, 5, "Cluster is not Kerberized",0,ln=1)
            
            pdf.cell(230, 8, "Cluster Details :",0,ln=1)
            
            
        #Total Memory Assigned to all the Master Nodes
        tot_memory_master_df = pd.DataFrame(columns=[])
        for j in cluster_host_items:
            rmasternode=obj1.hostData(j['hostId'])
            for role in rmasternode['roleRefs']:                
                if re.search(r'\bNAMENODE\b', role['roleName']) and "hdfs" in role['serviceName']:                   
                    host_tmp_df_1 = pd.DataFrame({'Cores' : rmasternode['numCores'],'Memory' : "{: .2f}".format(float(rmasternode['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                    tot_memory_master_df = tot_memory_master_df.append(host_tmp_df_1)
        var_memory_master=tot_memory_master_df['Memory'].tolist()
        tot_memory_master = 0 
        itr=0
        for itr in var_memory_master:  
            tot_memory_master += float(itr)      
        pdf.cell(230, 8,"Total Memory Assigned to All the MasterNodes : {: .2f} GB  ".format(tot_memory_master),0,ln=1)
        length_memory_master=len(var_memory_master)
        itr=0
        while itr < length_memory_master:
            pdf.cell(230, 8,"    Memory Available in MasterNode {} : {} GB ".format(itr+1,var_memory_master[itr]),0,ln=1)
            itr=itr+1
        
        #Total Memory Assigned to all the Data Nodes and per node
        tot_memory_datanode_df = pd.DataFrame(columns=[])
        for j in cluster_host_items:
            rdatanode=obj1.hostData(j['hostId'])
            for role in rdatanode['roleRefs']:
                if re.search(r'\bDATANODE\b', role['roleName']) and "hdfs" in role['serviceName']:
                    host_tmp_df_1 = pd.DataFrame({'Cores' : rdatanode['numCores'],'Memory' : "{: .2f}".format(float(rdatanode['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                    tot_memory_datanode_df = tot_memory_datanode_df.append(host_tmp_df_1)
        var_memory_datanode=tot_memory_datanode_df['Memory'].tolist()
        tot_memory_datanode = 0 
        itr=0
        for itr in var_memory_datanode:  
            tot_memory_datanode += float(itr)      
        pdf.cell(230, 8,"Total Memory Assigned to All the DataNodes : {: .2f} GB  ".format(tot_memory_datanode),0,ln=1)
        length_memory_datanode=len(var_memory_datanode)
        itr=0
        while itr < length_memory_datanode:
            pdf.cell(230, 8,"    Memory Available in DataNode {}     : {} GB ".format(itr+1,var_memory_datanode[itr]),0,ln=1)
            itr=itr+1
        
            
        #Using cloudera apis fetching cluster resource avalilability over a period of time
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Cluster Metrics",0,ln=1)
        pdf.set_font('Arial','', 12)
        cluster_total_cores_df=obj1.clusterTotalCores(cluster_name)
        cluster_cpu_usage_df,cluster_cpu_usage_avg=obj1.clusterCpuUsage(cluster_name)
        cluster_total_memory_df=obj1.clusterTotalMemory(cluster_name)
        cluster_memory_usage_df,cluster_memory_usage_avg=obj1.clusterMemoryUsage(cluster_name)

        #Total Cores Assigned to all the master nodes
        tot_core_masternode = 0 
        for i in cluster_items:            
            tot_cores_masternode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                rmasternode=obj1.hostData(j['hostId'])
                for role in rmasternode['roleRefs']:
                    if re.search(r'\bNAMENODE\b', role['roleName']) and "hdfs" in role['serviceName']:
                        host_tmp_df_1 = pd.DataFrame({'Cores' : rmasternode['numCores'],'Memory' : "{: .2f}".format(float(rmasternode['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        tot_cores_masternode_df = tot_cores_masternode_df.append(host_tmp_df_1)
            var_core_master=tot_cores_masternode_df['Cores'].tolist()
            for ele in var_core_master:  
                tot_core_masternode += int(ele)         
        
            
        #Total Cores Assigned to all the Data Nodes
        tot_core_datanode = 0
        for i in cluster_items:            
            tot_cores_datanode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                rdatanode=obj1.hostData(j['hostId'])
                for role in rdatanode['roleRefs']:
                    if re.search(r'\bDATANODE\b', role['roleName']) and "hdfs" in role['serviceName']:
                        host_tmp_df_1 = pd.DataFrame({'Cores' : rdatanode['numCores'],'Memory' : "{: .2f}".format(float(rdatanode['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        tot_cores_datanode_df = tot_cores_datanode_df.append(host_tmp_df_1)
            var_core_data=tot_cores_datanode_df['Cores'].tolist() 
            for ele in var_core_data:  
                tot_core_datanode += int(ele)
            
                
        total_cores_cluster=tot_core_datanode+tot_core_masternode    
        pdf.cell(230, 3, "",0,ln=1)
        pdf.cell(230, 5, "Total CPU Core in the Cluster                            : {: .0f}".format(total_cores_cluster),0,ln=1)
        pdf.cell(230, 8,"Total Cores Assigned to the MasterNode        : {} ".format(tot_core_masternode),0,ln=1)
        length_core_master=len(var_core_master)
        itr=0
        while itr < length_core_master:
            pdf.cell(230, 8,"    Cores Assigned to MasterNode {} : {} ".format(itr+1,var_core_master[itr]),0,ln=1)
            itr=itr+1     
        pdf.cell(230, 8,"Total Cores Assigned to All the DataNodes    : {} ".format(tot_core_datanode),0,ln=1)
        length_core_data=len(var_core_data)
        itr=0
        while itr < length_core_data:
            pdf.cell(230, 8,"    Cores Assigned to DataNode {}     : {} ".format(itr+1,var_core_data[itr]),0,ln=1)
            itr=itr+1       
        #print("Total CPU core in the cluster : {: .0f}".format(cluster_total_cores_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']))
        pdf.cell(230, 5, "Total Memory in the Cluster                              : {: .2f} GB".format(cluster_total_memory_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        pdf.cell(230, 3, "",0,ln=1)
        #pdf.cell(230, 5, "Time Duration in consideration : {} to {} ".format(cluster_total_cores_df['DateTime'].min(),cluster_total_cores_df['DateTime'].max()),0,ln=1)
        pdf.cell(230, 5, "Time Duration in Consideration : {} - {} ".format(datetime.strptime(str(cluster_total_cores_df['DateTime'].min()), '%Y-%m-%d %H:%M:%S').strftime('%b %d %Y'),datetime.strptime(str(cluster_total_cores_df['DateTime'].max()), '%Y-%m-%d %H:%M:%S').strftime('%b %d %Y')),0,ln=1)
        pdf.cell(230, 3, "",0,ln=1)
        pdf.cell(230, 5, "Average Cluster CPU Utilization is {: .2f}%".format(cluster_cpu_usage_avg),0,ln=1)
        pdf.cell(230, 5, "Average Cluster Memory Utilization is {: .2f}%".format(cluster_memory_usage_avg),0,ln=1)

        plt.figure(1)
        cluster_total_cores_plot = cluster_total_cores_df['Mean'].plot(color="steelblue",label='Available Cores')
        cluster_total_cores_plot.set_ylabel('Total CPU Cores')
        cluster_total_cores_plot.legend()
        plt.title("Cluster Vcore Availability")
        plt.savefig('cluster_total_cores_plot.png')
        pdf.image('cluster_total_cores_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')

        plt.figure(2)
        cluster_cpu_usage_plot = cluster_cpu_usage_df['Max'].plot(color="red",linestyle="--",label='Max Core Allocated',linewidth=1)
        cluster_cpu_usage_plot = cluster_cpu_usage_df['Mean'].plot(color="steelblue",label='Mean Cores Allocated')
        cluster_cpu_usage_plot = cluster_cpu_usage_df['Min'].plot(color="lime",linestyle="--",label='Min Cores Allocated',linewidth=1)
        cluster_cpu_usage_plot.legend()
        cluster_cpu_usage_plot.set_ylabel('CPU Utilization %')
        plt.title("Cluster Vcore Usage")
        plt.savefig('cluster_cpu_usage_plot.png')
        pdf.image('cluster_cpu_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')

        plt.figure(3)
        cluster_total_memory_plot = cluster_total_memory_df['Mean'].plot(color="steelblue",label='Avaliable Memory')
        cluster_total_memory_plot.set_ylabel('Total Memory(GB)')
        cluster_total_memory_plot.legend()
        plt.title("Cluster Memory Availability")
        plt.savefig('cluster_total_memory_plot.png')
        pdf.image('cluster_total_memory_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')

        plt.figure(4)
        cluster_memory_usage_plot = cluster_memory_usage_df['Mean'].plot(color="steelblue",label='Memory Allocated')
        cluster_memory_usage_plot.legend()
        cluster_memory_usage_plot.set_ylabel('Memory Utilization %')
        plt.title("Cluster Memory Usage")
        plt.savefig('cluster_memory_usage_plot.png')
        pdf.image('cluster_memory_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        
        
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Frameworks and Software Details",0,ln=1)
        pdf.set_font('Arial','', 12)
        # clouderaEnabled=obj3.checkCloudera()
        # if(clouderaEnabled==1):
            # pdf.cell(230, 8,"Cloudera Enabled ",0,ln=1)    
        # else:
            # pdf.cell(230, 8,"",0,ln=1)  
        
        hadoopVersionMajor,hadoopVersionMinor,distribution=obj3.hadoopVersion()
        pdf.cell(230, 8,"Hadoop Major Version Is     : {} ".format(hadoopVersionMajor),0,ln=1)
        pdf.cell(230, 8,"Hadoop Minor Version Is     : {} ".format(hadoopVersionMinor),0,ln=1)
        pdf.cell(230, 8,"Hadoop Distribution Is        : {} ".format(distribution),0,ln=1)
        
        # List of installed Apache Services (Eg. Hadoop, Spark, Kafka, Yarn, Hive, etc.)
        list_services_installed_df,new_ref_df=obj3.versionMapping(cluster_name)
        pdf.cell(230, 8, "List of Services Installed  : ",0,ln=1)
        pdf.set_font('Arial','B', 11)
        pdf.set_fill_color(r = 66, g = 133, b = 244)
        pdf.set_text_color(r = 255, g = 255, b = 255)
        pdf.cell(70, 5, 'Name', 1, 0, 'C',True)
        pdf.cell(70, 5, 'Vesrion', 1, 1, 'C',True)
        pdf.set_text_color(r = 1, g = 1, b = 1)
        pdf.set_fill_color(r = 244, g = 244, b = 244)
        pdf.set_font('Arial','', 11)
        for pos in range(0, len(new_ref_df)):
            pdf.cell(70, 5, "{}".format(new_ref_df['name'].iloc[pos]), 1, 0, 'C',True)
            pdf.cell(70, 5, "{}".format(new_ref_df['sub_version'].iloc[pos]), 1, 1, 'C',True)
        
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Data Section",0,ln=1)
        pdf.set_font('Arial','', 12)

        individual_node_size,total_storage=obj2.totalSizeConfigured()
        pdf.cell(230, 8,"Total Size Configured in the Cluster: {: .2f} GB ".format(total_storage),0,ln=1)
        #pdf.cell(230, 8,"Size Configured for individual nodes: {: .2f} GB ".format(total_storage),0,ln=1)
        # itr=0
        # while itr < individual_node_size:
            # pdf.cell(230, 8,"    Size Configured for node {} : {} GB ".format(itr+1,individual_node_size[itr]),0,ln=1)
            # itr=itr+1
        
        #Replication Factor
        replication_factor=obj2.replicationFactor()
        pdf.cell(230, 8,"Replication Factor                             : {} ".format(replication_factor),0,ln=1)    

        #check trash interval setup in the clsuter e.g Filesystem Trash Interval property to 1440
        trash_flag=obj2.getTrashStatus()
        pdf.cell(230, 8,"Trash Interval Setup in the Cluster  : {} ".format(trash_flag),0,ln=1)


        #HDFS Storage Available/Usage
        hdfs_capacity_df=obj2.getHdfsCapacity(clusterName)

        hdfs_capacity_used_df=obj2.getHdfsCapacityUsed(clusterName)

        hdfs_storage_config = hdfs_capacity_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']
        hdfs_storage_used = hdfs_capacity_used_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']

        pdf.cell(230, 3, "",0,ln=1)
        #pdf.cell(230, 5, "HDFS Storage Available : {: .0f} GB".format(hdfs_capacity_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        #pdf.cell(230, 5, "HDFS Storage Used : {: .0f} GB".format(hdfs_capacity_used_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        pdf.cell(230, 5, "HDFS Storage Available : {: .0f} GB".format(hdfs_capacity_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        pdf.cell(230, 5, "HDFS Storage Used       : {: .0f} GB".format(hdfs_capacity_used_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        pdf.cell(230, 3, "",0,ln=1)

        plt.figure(19)
        pdf.cell(230, 3, "",0,ln=1)
        hdfs_usage_plot = hdfs_capacity_df['Mean'].plot(color="steelblue",label='Storage Available')
        hdfs_usage_plot = hdfs_capacity_used_df['Mean'].plot(color="darkorange",label='Storage Used')
        hdfs_usage_plot.legend()
        hdfs_usage_plot.set_ylabel('HDFS Capacity(GB)')
        plt.title("HDFS Usage")
        plt.savefig('hdfs_usage_plot.png')
        pdf.image('hdfs_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')

        
        #HDFS Size Breakdow
        pdf.set_font('Arial','B', 12)
        pdf.cell(230, 10, "HDFS Size Breakdown",0,ln=1)
        pdf.set_font('Arial','', 12)

        hdfs_root_dir=obj2.getCliresult("/")

        for i in hdfs_root_dir.splitlines():
            hdfs_dir = i.split()
            if(len(hdfs_dir) == 5):
                hdfs_dir[0] = hdfs_dir[0] + b' ' + hdfs_dir[1]
                hdfs_dir[1] = hdfs_dir[2] + b' ' + hdfs_dir[3]
                hdfs_dir[2] = hdfs_dir[4]
            pdf.cell(230, 5, "{} - (Size = {} , Disk Space = {})".format(str(hdfs_dir[2],'utf-8'),str(hdfs_dir[0],'utf-8'),str(hdfs_dir[1],'utf-8')),0,ln=1)
            hdfs_inner_dir=obj2.getCliresult(hdfs_dir[2])
            for j in hdfs_inner_dir.splitlines():
                hdfs_inner_dir = j.split()
                if(len(hdfs_inner_dir) == 5):
                    hdfs_inner_dir[0] = hdfs_inner_dir[0] + b' ' + hdfs_inner_dir[1]
                    hdfs_inner_dir[1] = hdfs_inner_dir[2] + b' ' + hdfs_inner_dir[3]
                    hdfs_inner_dir[2] = hdfs_inner_dir[4]
                pdf.cell(230, 5, "    |-- {} - (Size = {} , Disk Space = {})".format(str(hdfs_inner_dir[2],'utf-8'),str(hdfs_inner_dir[0],'utf-8'),str(hdfs_inner_dir[1],'utf-8')),0,ln=1)
            pdf.cell(230, 3, "",0,ln=1)
        yarn_application_df = obj_app.getApplicationDetails(yarn_rm)
        app_count_df, app_type_count_df, app_status_count_df = obj_app.getApplicationTypeStatusCount(yarn_application_df)
        app_vcore_df, app_memory_df = obj_app.getApplicationVcoreMemoryUsage(yarn_application_df)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Yarn Application Metrics",0,ln=1)
        pdf.cell(230, 10, "",0,ln=1)
        pdf.set_font('Arial','', 12)
        pdf.set_font('Arial','B', 11)
        pdf.set_fill_color(r = 66, g = 133, b = 244)
        pdf.set_text_color(r = 255, g = 255, b = 255)
        pdf.cell(40, 5, 'Application Type', 1, 0, 'C',True)
        pdf.cell(40, 5, 'Status', 1, 0, 'C',True)
        pdf.cell(30, 5, 'Count', 1, 1, 'C',True)
        pdf.set_text_color(r = 1, g = 1, b = 1)
        pdf.set_fill_color(r = 244, g = 244, b = 244)
        pdf.set_font('Arial','', 11)
        for pos in range(0, len(app_count_df)):
            pdf.cell(40, 5, "{}".format(app_count_df['Application Type'].iloc[pos]), 1, 0, 'C',True)
            pdf.cell(40, 5, "{}".format(app_count_df['Status'].iloc[pos]), 1, 0, 'C',True)
            pdf.cell(30, 5, "{}".format(app_count_df['Count'].iloc[pos]), 1, 1, 'C',True)
        pdf.set_font('Arial','', 12)
        pdf.cell(230, 15, "",0,ln=1)
        x = pdf.get_x()
        y = pdf.get_y()
        plt.figure()
        app_type_count_pie_plot = app_type_count_df.plot.pie(y='Count', figsize=(6, 6),autopct='%.1f%%',title="Yarn Application by Type")
        plt.savefig('app_type_count_pie_plot.png')
        pdf.image('app_type_count_pie_plot.png', x = 15, y = None, w = 95, h = 95, type = '', link = '')
        pdf.set_xy(x,y)
        plt.figure()
        app_status_count_pie_plot = app_status_count_df.plot.pie(y='Count', figsize=(6, 6),autopct='%.1f%%',title="Yarn Application by Status")
        plt.savefig('app_status_count_pie_plot.png')
        pdf.image('app_status_count_pie_plot.png', x = 130, y = None, w = 95, h = 95, type = '', link = '')
        pdf.cell(230, 30, "",0,ln=1)
        plt.figure()
        x = pdf.get_x()
        y = pdf.get_y()
        app_vcore_plot = app_vcore_df.plot.pie(y='Vcore', figsize=(6, 6),autopct='%.1f%%',title="Yarn Application Vcore Usage")
        plt.savefig('app_vcore_plot.png')
        pdf.image('app_vcore_plot.png', x = 15, y = None, w = 95, h = 95, type = '', link = '')
        pdf.set_xy(x,y)
        plt.figure()
        app_memory_plot = app_memory_df.plot.pie(y='Memory', figsize=(6, 6),autopct='%.1f%%',title="Yarn Application Memory Usage")
        plt.savefig('app_memory_plot.png')
        pdf.image('app_memory_plot.png', x = 130, y = None, w = 95, h = 95, type = '', link = '')
        app_vcore_df, app_vcore_usage_df, app_memory_df, app_memory_usage_df = obj_app.getVcoreMemoryByApplication(yarn_application_df)
        pdf.add_page()
        plt.figure()
        for i in app_vcore_df['Application Type'].unique():
            app_vcore_df_temp = pd.DataFrame(None)
            app_vcore_df_temp = app_vcore_df[app_vcore_df['Application Type']==i]
            app_vcore_usage_df[i] = 0
            for index, row in app_vcore_df_temp.iterrows():
                val = (row['Launch Time'],0)
                if(val not in app_vcore_usage_df['Date']):
                    app_vcore_usage_df.loc[len(app_vcore_usage_df)] = val
                val = (row['Finished Time'],0)
                if(val not in app_vcore_usage_df['Date']):
                    app_vcore_usage_df.loc[len(app_vcore_usage_df)] = val
                app_vcore_usage_df.loc[(app_vcore_usage_df['Date']>= row['Launch Time']) & (app_vcore_usage_df['Date']< row['Finished Time']),i] = app_vcore_usage_df.loc[(app_vcore_usage_df['Date']>= row['Launch Time']) & (app_vcore_usage_df['Date']< row['Finished Time'])][i] + row['Vcore']
            app_vcore_usage_plot = app_vcore_usage_df.set_index('Date')[i].plot(label=i)
            app_vcore_usage_df = app_vcore_usage_df.drop([i], axis = 1)
        app_vcore_usage_plot.legend()
        app_vcore_usage_plot.set_ylabel('Application Vcores')
        plt.title("Vcore Breakdown By Application Type")
        plt.savefig('app_vcore_usage_plot.png')
        pdf.image('app_vcore_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        for i in app_memory_df['Application Type'].unique():
            app_memory_df_temp = pd.DataFrame(None)
            app_memory_df_temp = app_memory_df[app_memory_df['Application Type']==i]
            app_memory_usage_df[i] = 0
            for index, row in app_memory_df_temp.iterrows():
                val = (row['Launch Time'],0)
                if(val not in app_memory_usage_df['Date']):
                    app_memory_usage_df.loc[len(app_memory_usage_df)] = val
                val = (row['Finished Time'],0)
                if(val not in app_memory_usage_df['Date']):
                    app_memory_usage_df.loc[len(app_memory_usage_df)] = val
                app_memory_usage_df.loc[(app_memory_usage_df['Date']>= row['Launch Time']) & (app_memory_usage_df['Date']< row['Finished Time']),i] = app_memory_usage_df.loc[(app_memory_usage_df['Date']>= row['Launch Time']) & (app_memory_usage_df['Date']< row['Finished Time'])][i] + row['Memory']
            app_memory_usage_plot = app_memory_usage_df.set_index('Date')[i].plot(label=i)
            app_memory_usage_df = app_memory_usage_df.drop([i], axis = 1)
        app_memory_usage_plot.legend()
        app_memory_usage_plot.set_ylabel('Application Memory')
        plt.title("Memory Breakdown By Application Type")
        plt.savefig('app_memory_usage_plot.png')
        pdf.image('app_memory_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        yarn_pending_apps_df, yarn_pending_vcore_df, yarn_pending_memory_df = obj_app.getPendingApplication(cluster_name), obj_app.getPendingVcore(cluster_name), obj_app.getPendingMemory(cluster_name)
        pdf.add_page()
        plt.figure()
        yarn_pending_apps_plot = yarn_pending_apps_df['Max'].plot(color="steelblue",label='Pending Applications')
        yarn_pending_apps_plot.legend()
        yarn_pending_apps_plot.set_ylabel('Application Count')
        plt.title("Total Pending Applications Across YARN Pools")
        plt.savefig('yarn_pending_apps_plot.png')
        pdf.image('yarn_pending_apps_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_pending_vcore_plot = yarn_pending_vcore_df['Mean'].plot(color="steelblue",label='Pending Vcores')
        yarn_pending_vcore_plot.legend()
        yarn_pending_vcore_plot.set_ylabel('Vcores')
        plt.title("Total Pending VCores Across YARN Pools")
        plt.savefig('yarn_pending_vcore_plot.png')
        pdf.image('yarn_pending_vcore_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_pending_memory_plot = yarn_pending_memory_df['Mean'].plot(color="steelblue",label='Pending Memory')
        yarn_pending_memory_plot.legend()
        yarn_pending_memory_plot.set_ylabel('Memory (MB)')
        plt.title("Total Pending Memory Across YARN Pools")
        plt.savefig('yarn_pending_memory_plot.png')
        pdf.image('yarn_pending_memory_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        bursty_app_time_df, bursty_app_vcore_df, bursty_app_mem_df = obj_app.getBurstyApplicationDetails(yarn_application_df)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Bursty Applications",0,ln=1)
        if(bursty_app_time_df.size != 0):
            pdf.set_font('Arial', '', 12)
            pdf.cell(230, 5, "Bursty Applications - Elapsed Time",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(110, 5, 'Application Name', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Min Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Mean Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Max Time', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(bursty_app_time_df)):
                pdf.cell(110, 5, "{}".format(bursty_app_time_df['Application Name'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_time_df['Min'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_time_df['Mean'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_time_df['Max'].iloc[pos]), 1, 1, 'C',True)
            pdf.cell(230, 5, "",0,ln=1)
            plt.figure()
            bursty_app_time_df = bursty_app_time_df.set_index('Application Name')
            bursty_app_time_plot = bursty_app_time_df.plot.barh( stacked=True).legend(loc='upper center', ncol=3)
            plt.title("Bursty Applications - Elapsed Time")
            plt.xlabel("Time(secs)")
            plt.ylabel("Applications")
            plt.tight_layout()
            plt.savefig('bursty_app_time_plot.png')
            pdf.image('bursty_app_time_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
            pdf.cell(230, 5, "",0,ln=1)
            pdf.set_font('Arial', '', 12)
            pdf.cell(230, 5, "Bursty Applications - Vcore Seconds",0,ln=1)   
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(110, 5, 'Application Name', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Min Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Mean Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Max Time', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(bursty_app_vcore_df)):
                pdf.cell(110, 5, "{}".format(bursty_app_vcore_df['Application Name'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_vcore_df['Min'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_vcore_df['Mean'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_vcore_df['Max'].iloc[pos]), 1, 1, 'C',True)
            pdf.cell(230, 5, "",0,ln=1)
            plt.figure()
            bursty_app_vcore_df = bursty_app_vcore_df.set_index('Application Name')
            bursty_app_vcore_plot = bursty_app_vcore_df.plot.barh( stacked=True).legend(loc='upper center', ncol=3)
            plt.title("Bursty Applications - Vcore Seconds")
            plt.xlabel("Time(secs)")
            plt.ylabel("Applications")
            plt.tight_layout()
            plt.savefig('bursty_app_vcore_plot.png')
            pdf.image('bursty_app_vcore_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
            pdf.cell(230, 5, "",0,ln=1)
            pdf.set_font('Arial', '', 12)
            pdf.cell(230, 5, "Bursty Applications - Memory Seconds",0,ln=1)   
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(110, 5, 'Application Name', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Min Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Mean Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Max Time', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(bursty_app_mem_df)):
                pdf.cell(110, 5, "{}".format(bursty_app_mem_df['Application Name'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_mem_df['Min'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_mem_df['Mean'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_mem_df['Max'].iloc[pos]), 1, 1, 'C',True)
            pdf.cell(230, 5, "",0,ln=1)
            plt.figure()
            bursty_app_mem_df = bursty_app_mem_df.set_index('Application Name')
            bursty_app_mem_plot = bursty_app_mem_df.plot.barh( stacked=True).legend(loc='upper center', ncol=3)
            plt.title("Bursty Applications - Memory Seconds")
            plt.xlabel("Memory Seconds")
            plt.ylabel("Applications")
            plt.tight_layout()
            plt.savefig('bursty_app_mem_plot.png')
            pdf.image('bursty_app_mem_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        yarn_failed_app = obj_app.getFailedApplicationDetails(yarn_application_df)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Failed Applications",0,ln=1)
        pdf.set_font('Arial', '', 12)
        pdf.cell(230, 5, "Run time of Failed/Killed Applications = {: .2f} seconds".format(yarn_failed_app['ElapsedTime'].sum()),0,ln=1)
        pdf.cell(230, 5, "Vcores Seconds Used by Failed/Killed Applications = {} seconds".format(yarn_failed_app['MemorySeconds'].sum()),0,ln=1)
        pdf.cell(230, 5, "Memory Seconds Used Failed/Killed Applications = {} seconds".format(yarn_failed_app['VcoreSeconds'].sum()),0,ln=1)
        if(yarn_failed_app.size != 0):
            yarn_failed_app = yarn_failed_app.head(10)
            pdf.set_font('Arial','', 11)
            pdf.cell(230, 3, "",0,ln=1)
            pdf.cell(230, 8, "Top long running failed application diagnostics : ",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(40, 5, 'App Id', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Final Status', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Elapsed Time', 1, 0, 'C',True)
            pdf.cell(130, 5, 'Diagnostics', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(yarn_failed_app)):
                x = pdf.get_x()
                y = pdf.get_y()
                if(y > 300):
                    pdf.add_page()
                    x = pdf.get_x()
                    y = pdf.get_y()
                diag = yarn_failed_app['Diagnostics'].iloc[pos][:300]
                line_width = 0
                line_width = max(line_width,pdf.get_string_width(yarn_failed_app['ApplicationId'].iloc[pos]))
                cell_y = line_width/39.0
                line_width = max(line_width,pdf.get_string_width(yarn_failed_app['FinalStatus'].iloc[pos]))
                cell_y = max(cell_y,line_width/29.0)
                line_width = max(line_width,pdf.get_string_width(str(yarn_failed_app['ElapsedTime'].iloc[pos])))
                cell_y = max(cell_y,line_width/29.0)
                line_width = max(line_width,pdf.get_string_width(diag))
                cell_y = max(cell_y,line_width/129.0)
                cell_y = math.ceil(cell_y)
                cell_y = max(cell_y,1)
                cell_y = cell_y*5
                line_width = pdf.get_string_width(yarn_failed_app['ApplicationId'].iloc[pos])
                y_pos = line_width/39.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(40, y_pos, "{}".format(yarn_failed_app['ApplicationId'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+40,y)
                line_width = pdf.get_string_width(yarn_failed_app['FinalStatus'].iloc[pos])
                y_pos = line_width/29.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(30, y_pos, "{}".format(yarn_failed_app['FinalStatus'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+70,y)
                line_width = pdf.get_string_width(str(yarn_failed_app['ElapsedTime'].iloc[pos]))
                y_pos = line_width/29.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(30, y_pos, "{}".format(yarn_failed_app['ElapsedTime'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+100,y)
                line_width = pdf.get_string_width(diag)
                y_pos = line_width/129.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(130, y_pos, "{}".format(diag), 1, 'C',fill = True)
        yarn_total_vcores_count, yarn_vcore_available_df, [yarn_vcore_allocated_avg, yarn_vcore_allocated_df, yarn_vcore_allocated_pivot_df] = obj_app.getYarnTotalVcore(yarn_rm), obj_app.getYarnVcoreAvailable(cluster_name), obj_app.getYarnVcoreAllocated(cluster_name)
        yarn_total_memory_count, yarn_memory_available_df, [yarn_memory_allocated_avg, yarn_memory_allocated_df, yarn_memory_allocated_pivot_df] = obj_app.getYarnTotalMemory(yarn_rm), obj_app.getYarnMemoryAvailable(cluster_name), obj_app.getYarnMemoryAllocated(cluster_name)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Yarn Metrics",0,ln=1)
        pdf.set_font('Arial','', 12)
        pdf.cell(230, 3, "",0,ln=1)
        pdf.cell(230, 5, "Total Yarn Vcore : {:.0f}".format(yarn_total_vcores_count),0,ln=1)
        pdf.cell(230, 5, "Total Yarn Memory : {:.0f} GB".format(yarn_total_memory_count),0,ln=1)
        pdf.cell(230, 3, "",0,ln=1)
        pdf.cell(230, 5, "Average No. of Vcores Used : {: .2f}".format(yarn_vcore_allocated_avg),0,ln=1)
        pdf.cell(230, 5, "Average Yarn Memory Used : {:.0f} MB".format(yarn_memory_allocated_avg),0,ln=1)
        plt.figure()
        yarn_vcore_usage_plot = yarn_vcore_available_df['Mean'].plot(color="steelblue",label='Vcores Available')
        yarn_vcore_usage_plot = yarn_vcore_allocated_df['Mean'].plot(color="darkorange",label='Vcores Allocated (Mean)')
        yarn_vcore_usage_plot = yarn_vcore_allocated_df['Max'].plot(color="red",label='Vcores Allocated (Max)',linestyle="--",linewidth=1)
        yarn_vcore_usage_plot.legend()
        yarn_vcore_usage_plot.set_ylabel('Total Vcore Usage')
        plt.title("Yarn Vcore Usage")
        plt.savefig('yarn_vcore_usage_plot.png')
        pdf.image('yarn_vcore_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_memory_usage_plot = yarn_memory_available_df['Mean'].plot(color="steelblue",label='Memory Available')
        yarn_memory_usage_plot = yarn_memory_allocated_df['Mean'].plot(color="darkorange",label='Memory Allocated (Mean)')
        yarn_memory_usage_plot = yarn_memory_allocated_df['Max'].plot(color="red",label='Memory Allocated (Max)',linestyle="--",linewidth=1)
        yarn_memory_usage_plot.legend()
        yarn_memory_usage_plot.set_ylabel('Total Yarn Memory(MB)')
        plt.title("Yarn Memory Usage")
        plt.savefig('yarn_memory_usage_plot.png')
        pdf.image('yarn_memory_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_vcore_usage_heatmap = sns.heatmap(yarn_vcore_allocated_pivot_df,cmap="OrRd")
        plt.title('Yarn Vcore Usage')
        plt.savefig('yarn_vcore_usage_heatmap.png')
        pdf.image('yarn_vcore_usage_heatmap.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_memory_usage_heatmap = sns.heatmap(yarn_memory_allocated_pivot_df,cmap="OrRd")
        plt.title('Yarn Memory Usage')
        plt.savefig('yarn_memory_usage_heatmap.png')
        pdf.image('yarn_memory_usage_heatmap.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        yarn_queues_list, [queue_app_count_df, queue_elapsed_time_df], [queue_vcore_df, queue_vcore_usage_df, queue_memory_df, queue_memory_usage_df] = obj_app.getQueueDetails(yarn_rm), obj_app.getQueueApplication(yarn_application_df), obj_app.getQueueVcoreMemory(yarn_application_df)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Yarn Queues",0,ln=1)
        pdf.cell(230, 5, "",0,ln=1)
        pdf.set_font('Arial','', 12)
        def yarn_queue(yarn_queues_list,count):
            for queue in yarn_queues_list:
                if('queues' in queue):
                    pdf.cell(10*count, 5, "", 0, 0)
                    pdf.cell(30, 5, "|-- {} - (Absolute Capacity - {}, Max Capacity - {})".format(queue['queueName'],queue['absoluteCapacity'],queue['absoluteMaxCapacity']), 0, ln=1)
                    yarn_queue(queue['queues']['queue'],count+1)
                else:
                    pdf.cell(10*count, 5, "", 0, 0)
                    pdf.cell(30, 5, "|-- {} - (Absolute Capacity - {}, Max Capacity - {})".format(queue['queueName'],queue['absoluteCapacity'],queue['absoluteMaxCapacity']), 0, ln=1)
        pdf.cell(230, 5, "Queue Structure : ",0,ln=1)
        pdf.cell(230, 5, "Root - (Absolute Capacity - 100, Max Capacity - 100)",0,ln=1)
        yarn_queue(yarn_queues_list,1)
        pdf.cell(230, 10, "",0,ln=1)
        def make_autopct(values):
            def my_autopct(pct):
                total = sum(values)
                val = int(round(pct*total/100.0))
                return '{p:.2f}%  ({v:d})'.format(p=pct,v=val)
            return my_autopct
        plt.figure()
        x = pdf.get_x()
        y = pdf.get_y()
        queue_app_count_plot = queue_app_count_df.plot.pie(y='Application Count', figsize=(6, 6),autopct=make_autopct(queue_app_count_df['Application Count']),title="Queue Application Count (Weekly)")
        plt.savefig('queue_app_count_plot.png')
        pdf.image('queue_app_count_plot.png', x = 15, y = None, w = 95, h = 95, type = '', link = '')
        pdf.set_xy(x,y)
        plt.figure()
        queue_elapsed_time_plot = queue_elapsed_time_df.plot.pie(y='Elapsed Time', figsize=(6, 6),autopct='%.1f%%',title="Queue Elapsed Time (Weekly)")
        plt.savefig('queue_elapsed_time_plot.png')
        pdf.image('queue_elapsed_time_plot.png', x = 130, y = None, w = 95, h = 95, type = '', link = '')
        plt.figure()
        for i in queue_vcore_df['Queue'].unique():
            queue_vcore_df_temp = pd.DataFrame(None)
            queue_vcore_df_temp = queue_vcore_df[queue_vcore_df['Queue']==i]
            queue_vcore_usage_df[i] = 0
            for index, row in queue_vcore_df_temp.iterrows():
                val = (row['Launch Time'],0)
                if(val not in queue_vcore_usage_df['Date']):
                    queue_vcore_usage_df.loc[len(queue_vcore_usage_df)] = val
                val = (row['Finished Time'],0)
                if(val not in queue_vcore_usage_df['Date']):
                    queue_vcore_usage_df.loc[len(queue_vcore_usage_df)] = val
                queue_vcore_usage_df.loc[(queue_vcore_usage_df['Date']>= row['Launch Time']) & (queue_vcore_usage_df['Date']< row['Finished Time']),i] = queue_vcore_usage_df.loc[(queue_vcore_usage_df['Date']>= row['Launch Time']) & (queue_vcore_usage_df['Date']< row['Finished Time'])][i] + row['Vcore']
            queue_vcore_usage_plot = queue_vcore_usage_df.set_index('Date')[i].plot(label=i)
            queue_vcore_usage_df = queue_vcore_usage_df.drop([i], axis = 1)
        queue_vcore_usage_plot.legend()
        queue_vcore_usage_plot.set_ylabel('Application Vcores')
        plt.title("Vcore Breakdown By Queue")
        plt.savefig('queue_vcore_usage_plot.png')
        pdf.image('queue_vcore_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        for i in queue_memory_df['Queue'].unique():
            queue_memory_df_temp = pd.DataFrame(None)
            queue_memory_df_temp = queue_memory_df[queue_memory_df['Queue']==i]
            queue_memory_usage_df[i] = 0
            for index, row in queue_memory_df_temp.iterrows():
                val = (row['Launch Time'],0)
                if(val not in queue_memory_usage_df['Date']):
                    queue_memory_usage_df.loc[len(queue_memory_usage_df)] = val
                val = (row['Finished Time'],0)
                if(val not in queue_memory_usage_df['Date']):
                    queue_memory_usage_df.loc[len(queue_memory_usage_df)] = val
                queue_memory_usage_df.loc[(queue_memory_usage_df['Date']>= row['Launch Time']) & (queue_memory_usage_df['Date']< row['Finished Time']),i] = queue_memory_usage_df.loc[(queue_memory_usage_df['Date']>= row['Launch Time']) & (queue_memory_usage_df['Date']< row['Finished Time'])][i] + row['Memory']
            queue_memory_usage_plot = queue_memory_usage_df.set_index('Date')[i].plot(label=i)
            queue_memory_usage_df = queue_memory_usage_df.drop([i], axis = 1)
        queue_memory_usage_plot.legend()
        queue_memory_usage_plot.set_ylabel('Application Memory')
        plt.title("Memory Breakdown By Queue")
        plt.savefig('queue_memory_usage_plot.png')
        pdf.image('queue_memory_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        pdf.output('Discovery_Report/{}.pdf'.format(cluster_name), 'F')

    def run_6():
        obj1=HardwareOSAPI
        obj2=DataAPI
        obj3=FrameworkDetailsAPI
        obj_app=ApplicationAPI
        yarn_rm = ""
        hive_server2 = ""        
        if os.path.exists("Discovery_Report"):
            shutil.rmtree("Discovery_Report")
        os.makedirs("Discovery_Report")
        yarn_rm = ""
        hive_server2 = ""

        pdf = FPDF(format=(250,350))
        pdf.add_page()
        pdf.set_font('Arial', 'B', 16)
        pdf.cell(230, 10, "Hadoop Discovery Report",0,ln=1,align = 'C')
        pdf.set_font('Arial','', 12)
        pdf.cell(230, 8, "Report Date Range : Start  {} ".format(date_range_start),0,ln=1,align='R')
        pdf.cell(0, 8, "End  {} ".format(date_range_end),0,ln=1,align='R')
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Cluster Information",0,ln=1)
        pdf.set_font('Arial','', 12)
        cluster_items=obj1.clusterItems()
        
        #Number of cluster configured
        pdf.cell(230, 8, "Number of Cluster Configured : {}".format(len(cluster_items)),0,ln=1)
        pdf.cell(230, 8, "Cluster Details : ",0,ln=1)
        pdf.set_font('Arial','B', 11)
        cluster_df = pd.DataFrame(cluster_items,columns =['name','fullVersion','entityStatus'])
        cluster_df.index = cluster_df.index + 1
        cluster_df = cluster_df.rename(columns={"name": "Cluster Name", "fullVersion": "Cloudera Version","entityStatus": "Health Status"})
        pdf.set_fill_color(r = 66, g = 133, b = 244)        
        
        #Cluster Details
        pdf.set_text_color(r = 255, g = 255, b = 255)
        pdf.cell(40, 5, 'Cluster Name', 1, 0, 'C',True)
        pdf.cell(40, 5, 'Cloudera Version', 1, 0, 'C',True)
        pdf.cell(50, 5, 'Health Status', 1, 1,'C',True)
        pdf.set_text_color(r = 1, g = 1, b = 1)
        pdf.set_fill_color(r = 244, g = 244, b = 244)
        pdf.set_font('Arial','', 11)
        for pos in range(0, len(cluster_df)):
            x = pdf.get_x()
            y = pdf.get_y()
            if(y > 300):
                pdf.add_page()
                x = pdf.get_x()
                y = pdf.get_y()
            line_width = 1
            line_width = max(line_width,pdf.get_string_width(cluster_df['Cluster Name'].iloc[pos]))
            cell_y = line_width/39.0
            line_width = max(line_width,pdf.get_string_width(cluster_df['Cloudera Version'].iloc[pos]))
            cell_y = max(cell_y,line_width/39.0)
            line_width = max(line_width,pdf.get_string_width(cluster_df['Health Status'].iloc[pos]))
            cell_y = max(cell_y,line_width/49.0)     
            cell_y = line_width/39.0
            cell_y = math.ceil(cell_y)
            cell_y = max(cell_y,1)
            cell_y = cell_y*5
            
            line_width = pdf.get_string_width(cluster_df['Cluster Name'].iloc[pos])
            y_pos = line_width/39.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos,1)
            y_pos = cell_y/y_pos
            pdf.multi_cell(40, y_pos, "{}".format(cluster_df['Cluster Name'].iloc[pos]), 1, 'C',fill = True)
            pdf.set_xy(x+40,y)
            
            line_width = pdf.get_string_width(cluster_df['Cloudera Version'].iloc[pos])
            y_pos = line_width/39.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos,1)
            y_pos = cell_y/y_pos
            pdf.multi_cell(40, y_pos, "{}".format(cluster_df['Cloudera Version'].iloc[pos]), 1, 'C',fill = True)
            pdf.set_xy(x+80,y)
            
            line_width = pdf.get_string_width(cluster_df['Health Status'].iloc[pos])
            y_pos = line_width/49.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos,1)
            y_pos = cell_y/y_pos
            pdf.multi_cell(50, y_pos, "{}".format(cluster_df['Health Status'].iloc[pos]), 1, 'C',fill = True)
        cluster_count = 0
        host_counts = 0
        service_count = 0
        pdf.cell(190, 5, "",0,ln=1)
        pdf.set_font('Arial','', 12)
        
        #OS Version (Eg: Centos 7.8.2003)
        os_version = obj1.osVersion()
        
        clusterName=""
        for i in cluster_items:
            cluster_name = i['name']
            clusterName=cluster_name
            if os.path.exists("Discovery_Report/{}".format(cluster_name)):
                shutil.rmtree("Discovery_Report/{}".format(cluster_name))
            os.makedirs("Discovery_Report/{}".format(cluster_name))
            cluster_count = cluster_count+1
            
            #Listing details for Cluster
            pdf.cell(230, 8,"Listing Details for Cluster {} - {}".format(cluster_count,cluster_name),0,ln=1)
            cluster_host_items,clusterHostLen=obj1.clusterHostItems(cluster_name)
            if os.path.exists('Discovery_Report/{}/host_details.json'.format(cluster_name)):
                os.remove('Discovery_Report/{}/host_details.json'.format(cluster_name))
            
            #Number of host in the cluster            
            pdf.cell(230, 8, "Number of Host               : {}".format(clusterHostLen),0,ln=1)
            host_df = pd.DataFrame(columns=['Hostname', 'Host ip','Number of cores','Physical Memory','Health Status'])
            for j in cluster_host_items:
                host_counts = host_counts+1
                host_count = obj1.hostData(j['hostId'])
                for role in host_count['roleRefs']:
                    if "RESOURCEMANAGER" in role['roleName'].upper():
                        yarn_rm = host_count['ipAddress']
                    if "HIVESERVER2" in role['roleName'].upper():
                        hive_server2 = host_count['ipAddress']
                with open('Discovery_Report/{}/host_details.json'.format(cluster_name), 'a') as fp:
                    json.dump(host_count, fp,  indent=4)
                host_tmp_df = pd.DataFrame({'Hostname': host_count['hostname'], 'Host IP': host_count['ipAddress'],'Cores' : host_count['numCores'],'Memory' : "{: .2f} GB".format(float(host_count['totalPhysMemBytes'])/1024/1024/1024),'Health Status' : host_count['entityStatus'],}, index=[host_counts])
                host_df = host_df.append(host_tmp_df)
            
            #Total Data Nodes in the cluster
            tot_datanode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                data_count =obj1.hostData(j['hostId'])
                for role in data_count['roleRefs']:                    
                    if re.search(r'\bDATANODE\b', role['roleName']):                        
                        host_tmp_df_1 = pd.DataFrame({'Cores' : data_count['numCores'],'Memory' : "{: .2f}".format(float(data_count['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        tot_datanode_df = tot_datanode_df.append(host_tmp_df_1)
            var_total_datanode=tot_datanode_df['Memory'].tolist()
            len_datanodes=len(var_total_datanode)            
            pdf.cell(230, 8, "Number of DataNodes    : {}".format(len_datanodes),0,ln=1)
            

            #Total Name Nodes in the cluster
            tot_namenode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                node_count =obj1.hostData(j['hostId'])
                for role in node_count['roleRefs']:                    
                    if re.search(r'\bNAMENODE\b', role['roleName']):                        
                        host_tmp_df_1 = pd.DataFrame({'Cores' : node_count['numCores'],'Memory' : "{: .2f}".format(float(node_count['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        tot_namenode_df = tot_namenode_df.append(host_tmp_df_1)
            var_total_namemode=tot_namenode_df['Memory'].tolist()
            len_namenodes=len(var_total_namemode)            
            pdf.cell(230, 8, "Number of NameNodes  : {}".format(len_namenodes),0,ln=1)
            
            #Number of Edge Nodes
            num_edgenode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                redgenode=obj1.hostData(j['hostId'])
                for role in redgenode['roleRefs']:
                    if re.search(r'\bGATEWAY\b', role['roleName']) and "hdfs" in role['serviceName']:
                        host_tmp_df_1 = pd.DataFrame({'Host IP': redgenode['ipAddress']}, index=[host_count])
                        num_edgenode_df = num_edgenode_df.append(host_tmp_df_1)           
            pdf.cell(230, 8,"Number of Edge Nodes  : {} ".format(len(num_edgenode_df)),0,ln=1)
            
            #Host Details
            pdf.cell(230, 8, "Host Details : ",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(90, 5, 'Hostname', 1, 0, 'C',True)
            pdf.cell(16, 5, 'Host IP', 1, 0, 'C',True)
            pdf.cell(10, 5, 'Cores', 1, 0, 'C',True)
            pdf.cell(20, 5, 'Memory', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Health Status', 1, 0, 'C',True)
            pdf.cell(65, 5, 'Distribution', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(host_df)):
                pdf.cell(90, 5, "{}".format(host_df['Hostname'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(16, 5, "{}".format(host_df['Host IP'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(10, 5, "{}".format(host_df['Cores'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(20, 5, "{}".format(host_df['Memory'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{}".format(host_df['Health Status'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(65, 5, os_version, 1, 1, 'C',True)   
                
            cluster_service_item=obj1.clusterServiceItem(cluster_name)
            service_df = pd.DataFrame(columns=['Service Name','Health Status','Health Concerns'])
            for k in cluster_service_item:
                if(k['serviceState']!='STARTED'):
                    continue
                service_count = service_count + 1
                concerns = ""
                if(k['entityStatus']!='GOOD_HEALTH'):
                    for l in k['healthChecks']:
                        if(l['summary']!='GOOD'):
                            if(concerns == ""):
                                concerns = l['name']
                            else:
                                concerns = concerns + "\n" + l['name']
                service_tmp_df = pd.DataFrame({'Service Name': k['name'], 'Health Status': k['entityStatus'],'Health Concerns' : concerns}, index=[service_count])
                service_df = service_df.append(service_tmp_df)
            
            #Services running in the cluster
            pdf.set_font('Arial','', 12)
            pdf.cell(230, 3, "",0,ln=1)
            pdf.cell(230, 8, "Services Running in the Cluster : ",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(60, 5, 'Service Name', 1, 0, 'C',True)
            pdf.cell(60, 5, 'Health Status', 1, 0, 'C',True)
            pdf.cell(90, 5, 'Health Concerns', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(service_df)):
                x = pdf.get_x()
                y = pdf.get_y()
                if(y > 300):
                    pdf.add_page()
                    x = pdf.get_x()
                    y = pdf.get_y()
                line_width = 0
                line_width = max(line_width,pdf.get_string_width(service_df['Service Name'].iloc[pos]))
                cell_y = line_width/59.0
                line_width = max(line_width,pdf.get_string_width(service_df['Health Status'].iloc[pos]))
                cell_y = max(cell_y,line_width/59.0)
                line_width = max(line_width,pdf.get_string_width(service_df['Health Concerns'].iloc[pos]))
                cell_y = max(cell_y,line_width/89.0)
                #cell_y = line_width/59.0
                cell_y = math.ceil(cell_y)
                cell_y = max(cell_y,1)
                cell_y = cell_y*5
                line_width = pdf.get_string_width(service_df['Service Name'].iloc[pos])
                y_pos = line_width/59.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(60, y_pos, "{}".format(service_df['Service Name'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+60,y)
                line_width = pdf.get_string_width(service_df['Health Status'].iloc[pos])
                y_pos = line_width/59.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(60, y_pos, "{}".format(service_df['Health Status'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+120,y)
                line_width = pdf.get_string_width(service_df['Health Concerns'].iloc[pos])
                y_pos = line_width/89.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(90, y_pos, "{}".format(service_df['Health Concerns'].iloc[pos]), 1, 'C',fill = True)
            
        for data in cluster_items: 
        #Memory Allocated per node  
            memory_edgenode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                edge_memory =obj1.hostData(j['hostId'])
                for role in edge_memory['roleRefs']:                    
                    if re.search(r'\bGATEWAY\b', role['roleName']) and "hdfs" in role['serviceName']:                        
                        host_tmp_df_1 = pd.DataFrame({'Nodename':role['roleName'],'Memory' : "{: .2f}".format(float(edge_memory['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        memory_edgenode_df = memory_edgenode_df.append(host_tmp_df_1)
            
            if memory_edgenode_df.empty:                
                pdf.cell(230, 8,"Edgenode Detected : Zero",0,ln=1)
            else:
                pdf.cell(230, 8, "Memory Allocated to edge nodes : ",0,ln=1)
                pdf.set_font('Arial','B', 11)
                pdf.set_fill_color(r = 66, g = 133, b = 244)
                pdf.set_text_color(r = 255, g = 255, b = 255)
                pdf.cell(100, 5, 'Nodename', 1, 0, 'C',True)
                pdf.cell(30, 5, 'Memory', 1, 1, 'C',True)
                pdf.set_text_color(r = 1, g = 1, b = 1)
                pdf.set_fill_color(r = 244, g = 244, b = 244)
                pdf.set_font('Arial','', 11)
                for pos in range(0, len(memory_edgenode_df)):                    
                    pdf.cell(100, 5, "{}".format(memory_edgenode_df['Nodename'].iloc[pos]), 1, 0, 'C',True)
                    pdf.cell(30, 5, "{}".format(memory_edgenode_df['Memory'].iloc[pos]), 1, 1, 'C',True)
            
            clints_onEdgenode=pd.DataFrame(columns=[])
            for j in cluster_host_items:
                edge_services =obj1.hostData(j['hostId'])
                for role in edge_services['roleRefs']:                    
                    if "GATEWAY" in role['roleName']:
                        a=pd.DataFrame({'service':role['serviceName']}, index=[host_count])
                        clints_onEdgenode=clints_onEdgenode.append(a)
            clints_onEdgenode=clints_onEdgenode.drop_duplicates()
            pdf.cell(230, 8, "Clients Installed on Gateway : ",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(70, 5, 'Services', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(clints_onEdgenode)):
                pdf.cell(70, 5, "{}".format(clints_onEdgenode['service'].iloc[pos]), 1, 1, 'C',True)
            
            #Kerberos Details
            pdf.set_font('Arial','', 12)
            pdf.cell(230, 3, "",0,ln=1)
            pdf.cell(230, 8, "Kerberos Details :",0,ln=1)
            cluster_kerberos_info=obj1.clusterKerberosInfo(cluster_name)
            with open('Discovery_Report/{}/cluster_kerberos_info.json'.format(cluster_name), 'w') as fp:
                json.dump(cluster_kerberos_info, fp,  indent=4)
            kerberized_status = str(cluster_kerberos_info['kerberized'])
            if(kerberized_status=="True"):                
                pdf.cell(230, 5, "Cluster is Kerberized",0,ln=1)                 
            else:                               
                pdf.cell(230, 5, "Cluster is not Kerberized",0,ln=1)
            
            pdf.cell(230, 8, "Cluster Details :",0,ln=1)
            
            
        #Total Memory Assigned to all the Master Nodes
        tot_memory_master_df = pd.DataFrame(columns=[])
        for j in cluster_host_items:
            rmasternode=obj1.hostData(j['hostId'])
            for role in rmasternode['roleRefs']:                
                if re.search(r'\bNAMENODE\b', role['roleName']) and "hdfs" in role['serviceName']:                   
                    host_tmp_df_1 = pd.DataFrame({'Cores' : rmasternode['numCores'],'Memory' : "{: .2f}".format(float(rmasternode['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                    tot_memory_master_df = tot_memory_master_df.append(host_tmp_df_1)
        var_memory_master=tot_memory_master_df['Memory'].tolist()
        tot_memory_master = 0 
        itr=0
        for itr in var_memory_master:  
            tot_memory_master += float(itr)      
        pdf.cell(230, 8,"Total Memory Assigned to All the MasterNodes : {: .2f} GB  ".format(tot_memory_master),0,ln=1)
        length_memory_master=len(var_memory_master)
        itr=0
        while itr < length_memory_master:
            pdf.cell(230, 8,"    Memory Available in MasterNode {} : {} GB ".format(itr+1,var_memory_master[itr]),0,ln=1)
            itr=itr+1
        
        #Total Memory Assigned to all the Data Nodes and per node
        tot_memory_datanode_df = pd.DataFrame(columns=[])
        for j in cluster_host_items:
            rdatanode=obj1.hostData(j['hostId'])
            for role in rdatanode['roleRefs']:
                if re.search(r'\bDATANODE\b', role['roleName']) and "hdfs" in role['serviceName']:
                    host_tmp_df_1 = pd.DataFrame({'Cores' : rdatanode['numCores'],'Memory' : "{: .2f}".format(float(rdatanode['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                    tot_memory_datanode_df = tot_memory_datanode_df.append(host_tmp_df_1)
        var_memory_datanode=tot_memory_datanode_df['Memory'].tolist()
        tot_memory_datanode = 0 
        itr=0
        for itr in var_memory_datanode:  
            tot_memory_datanode += float(itr)      
        pdf.cell(230, 8,"Total Memory Assigned to All the DataNodes : {: .2f} GB  ".format(tot_memory_datanode),0,ln=1)
        length_memory_datanode=len(var_memory_datanode)
        itr=0
        while itr < length_memory_datanode:
            pdf.cell(230, 8,"    Memory Available in DataNode {}     : {} GB ".format(itr+1,var_memory_datanode[itr]),0,ln=1)
            itr=itr+1
        
            
        #Using cloudera apis fetching cluster resource avalilability over a period of time
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Cluster Metrics",0,ln=1)
        pdf.set_font('Arial','', 12)
        cluster_total_cores_df=obj1.clusterTotalCores(cluster_name)
        cluster_cpu_usage_df,cluster_cpu_usage_avg=obj1.clusterCpuUsage(cluster_name)
        cluster_total_memory_df=obj1.clusterTotalMemory(cluster_name)
        cluster_memory_usage_df,cluster_memory_usage_avg=obj1.clusterMemoryUsage(cluster_name)

        #Total Cores Assigned to all the master nodes
        tot_core_masternode = 0 
        for i in cluster_items:            
            tot_cores_masternode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                rmasternode=obj1.hostData(j['hostId'])
                for role in rmasternode['roleRefs']:
                    if re.search(r'\bNAMENODE\b', role['roleName']) and "hdfs" in role['serviceName']:
                        host_tmp_df_1 = pd.DataFrame({'Cores' : rmasternode['numCores'],'Memory' : "{: .2f}".format(float(rmasternode['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        tot_cores_masternode_df = tot_cores_masternode_df.append(host_tmp_df_1)
            var_core_master=tot_cores_masternode_df['Cores'].tolist()
            for ele in var_core_master:  
                tot_core_masternode += int(ele)         
        
            
        #Total Cores Assigned to all the Data Nodes
        tot_core_datanode = 0
        for i in cluster_items:            
            tot_cores_datanode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                rdatanode=obj1.hostData(j['hostId'])
                for role in rdatanode['roleRefs']:
                    if re.search(r'\bDATANODE\b', role['roleName']) and "hdfs" in role['serviceName']:
                        host_tmp_df_1 = pd.DataFrame({'Cores' : rdatanode['numCores'],'Memory' : "{: .2f}".format(float(rdatanode['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        tot_cores_datanode_df = tot_cores_datanode_df.append(host_tmp_df_1)
            var_core_data=tot_cores_datanode_df['Cores'].tolist() 
            for ele in var_core_data:  
                tot_core_datanode += int(ele)
            
                
        total_cores_cluster=tot_core_datanode+tot_core_masternode    
        pdf.cell(230, 3, "",0,ln=1)
        pdf.cell(230, 5, "Total CPU Core in the Cluster                            : {: .0f}".format(total_cores_cluster),0,ln=1)
        pdf.cell(230, 8,"Total Cores Assigned to the MasterNode        : {} ".format(tot_core_masternode),0,ln=1)
        length_core_master=len(var_core_master)
        itr=0
        while itr < length_core_master:
            pdf.cell(230, 8,"    Cores assigned to MasterNode {} : {} ".format(itr+1,var_core_master[itr]),0,ln=1)
            itr=itr+1     
        pdf.cell(230, 8,"Total Cores Assigned to All the DataNodes    : {} ".format(tot_core_datanode),0,ln=1)
        length_core_data=len(var_core_data)
        itr=0
        while itr < length_core_data:
            pdf.cell(230, 8,"    Cores Assigned to DataNode {}     : {} ".format(itr+1,var_core_data[itr]),0,ln=1)
            itr=itr+1       
        #print("Total CPU core in the cluster : {: .0f}".format(cluster_total_cores_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']))
        pdf.cell(230, 5, "Total Memory in the Cluster                              : {: .2f} GB".format(cluster_total_memory_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        pdf.cell(230, 3, "",0,ln=1)
        #pdf.cell(230, 5, "Time Duration in consideration : {} to {} ".format(cluster_total_cores_df['DateTime'].min(),cluster_total_cores_df['DateTime'].max()),0,ln=1)
        pdf.cell(230, 5, "Time Duration in Consideration : {} - {} ".format(datetime.strptime(str(cluster_total_cores_df['DateTime'].min()), '%Y-%m-%d %H:%M:%S').strftime('%b %d %Y'),datetime.strptime(str(cluster_total_cores_df['DateTime'].max()), '%Y-%m-%d %H:%M:%S').strftime('%b %d %Y')),0,ln=1)
        pdf.cell(230, 3, "",0,ln=1)
        pdf.cell(230, 5, "Average Cluster CPU Utilization is {: .2f}%".format(cluster_cpu_usage_avg),0,ln=1)
        pdf.cell(230, 5, "Average Cluster Memory Utilization is {: .2f}%".format(cluster_memory_usage_avg),0,ln=1)

        plt.figure(1)
        cluster_total_cores_plot = cluster_total_cores_df['Mean'].plot(color="steelblue",label='Available Cores')
        cluster_total_cores_plot.set_ylabel('Total CPU Cores')
        cluster_total_cores_plot.legend()
        plt.title("Cluster Vcore Availability")
        plt.savefig('cluster_total_cores_plot.png')
        pdf.image('cluster_total_cores_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')

        plt.figure(2)
        cluster_cpu_usage_plot = cluster_cpu_usage_df['Max'].plot(color="red",linestyle="--",label='Max Core Allocated',linewidth=1)
        cluster_cpu_usage_plot = cluster_cpu_usage_df['Mean'].plot(color="steelblue",label='Mean Cores Allocated')
        cluster_cpu_usage_plot = cluster_cpu_usage_df['Min'].plot(color="lime",linestyle="--",label='Min Cores Allocated',linewidth=1)
        cluster_cpu_usage_plot.legend()
        cluster_cpu_usage_plot.set_ylabel('CPU Utilization %')
        plt.title("Cluster Vcore Usage")
        plt.savefig('cluster_cpu_usage_plot.png')
        pdf.image('cluster_cpu_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')

        plt.figure(3)
        cluster_total_memory_plot = cluster_total_memory_df['Mean'].plot(color="steelblue",label='Avaliable Memory')
        cluster_total_memory_plot.set_ylabel('Total Memory(GB)')
        cluster_total_memory_plot.legend()
        plt.title("Cluster Memory Availability")
        plt.savefig('cluster_total_memory_plot.png')
        pdf.image('cluster_total_memory_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')

        plt.figure(4)
        cluster_memory_usage_plot = cluster_memory_usage_df['Mean'].plot(color="steelblue",label='Memory Allocated')
        cluster_memory_usage_plot.legend()
        cluster_memory_usage_plot.set_ylabel('Memory Utilization %')
        plt.title("Cluster Memory Usage")
        plt.savefig('cluster_memory_usage_plot.png')
        pdf.image('cluster_memory_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        
        
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Frameworks and Software Details",0,ln=1)
        pdf.set_font('Arial','', 12)
        # clouderaEnabled=obj3.checkCloudera()
        # if(clouderaEnabled==1):
            # pdf.cell(230, 8,"Cloudera Enabled ",0,ln=1)    
        # else:
            # pdf.cell(230, 8,"",0,ln=1)  
        
        hadoopVersionMajor,hadoopVersionMinor,distribution=obj3.hadoopVersion()
        pdf.cell(230, 8,"Hadoop Major Version Is     : {} ".format(hadoopVersionMajor),0,ln=1)
        pdf.cell(230, 8,"Hadoop Minor Version Is     : {} ".format(hadoopVersionMinor),0,ln=1)
        pdf.cell(230, 8,"Hadoop Distribution Is        : {} ".format(distribution),0,ln=1)
        
        # List of installed Apache Services (Eg. Hadoop, Spark, Kafka, Yarn, Hive, etc.)
        list_services_installed_df,new_ref_df=obj3.versionMapping(cluster_name)
        pdf.cell(230, 8, "List of Services Installed  : ",0,ln=1)
        pdf.set_font('Arial','B', 11)
        pdf.set_fill_color(r = 66, g = 133, b = 244)
        pdf.set_text_color(r = 255, g = 255, b = 255)
        pdf.cell(70, 5, 'Name', 1, 0, 'C',True)
        pdf.cell(70, 5, 'Vesrion', 1, 1, 'C',True)
        pdf.set_text_color(r = 1, g = 1, b = 1)
        pdf.set_fill_color(r = 244, g = 244, b = 244)
        pdf.set_font('Arial','', 11)
        for pos in range(0, len(new_ref_df)):
            pdf.cell(70, 5, "{}".format(new_ref_df['name'].iloc[pos]), 1, 0, 'C',True)
            pdf.cell(70, 5, "{}".format(new_ref_df['sub_version'].iloc[pos]), 1, 1, 'C',True)
        
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Data Section",0,ln=1)
        pdf.set_font('Arial','', 12)

        individual_node_size,total_storage=obj2.totalSizeConfigured()
        pdf.cell(230, 8,"Total Size Configured in the Cluster: {: .2f} GB ".format(total_storage),0,ln=1)
        #pdf.cell(230, 8,"Size Configured for individual nodes: {: .2f} GB ".format(total_storage),0,ln=1)
        # itr=0
        # while itr < individual_node_size:
            # pdf.cell(230, 8,"    Size Configured for node {} : {} GB ".format(itr+1,individual_node_size[itr]),0,ln=1)
            # itr=itr+1
        
        #Replication Factor
        replication_factor=obj2.replicationFactor()
        pdf.cell(230, 8,"Replication Factor                             : {} ".format(replication_factor),0,ln=1)    

        #check trash interval setup in the clsuter e.g Filesystem Trash Interval property to 1440
        trash_flag=obj2.getTrashStatus()
        pdf.cell(230, 8,"Trash Interval Setup in the Cluster  : {} ".format(trash_flag),0,ln=1)


        #HDFS Storage Available/Usage
        hdfs_capacity_df=obj2.getHdfsCapacity(clusterName)

        hdfs_capacity_used_df=obj2.getHdfsCapacityUsed(clusterName)

        hdfs_storage_config = hdfs_capacity_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']
        hdfs_storage_used = hdfs_capacity_used_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']

        pdf.cell(230, 3, "",0,ln=1)
        #pdf.cell(230, 5, "HDFS Storage Available : {: .0f} GB".format(hdfs_capacity_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        #pdf.cell(230, 5, "HDFS Storage Used : {: .0f} GB".format(hdfs_capacity_used_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        pdf.cell(230, 5, "HDFS Storage Available : {: .0f} GB".format(hdfs_capacity_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        pdf.cell(230, 5, "HDFS Storage Used       : {: .0f} GB".format(hdfs_capacity_used_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        pdf.cell(230, 3, "",0,ln=1)

        plt.figure(19)
        pdf.cell(230, 3, "",0,ln=1)
        hdfs_usage_plot = hdfs_capacity_df['Mean'].plot(color="steelblue",label='Storage Available')
        hdfs_usage_plot = hdfs_capacity_used_df['Mean'].plot(color="darkorange",label='Storage Used')
        hdfs_usage_plot.legend()
        hdfs_usage_plot.set_ylabel('HDFS Capacity(GB)')
        plt.title("HDFS Usage")
        plt.savefig('hdfs_usage_plot.png')
        pdf.image('hdfs_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')

        
        #HDFS Size Breakdow
        pdf.set_font('Arial','B', 12)
        pdf.cell(230, 10, "HDFS Size Breakdown",0,ln=1)
        pdf.set_font('Arial','', 12)

        hdfs_root_dir=obj2.getCliresult("/")

        for i in hdfs_root_dir.splitlines():
            hdfs_dir = i.split()
            if(len(hdfs_dir) == 5):
                hdfs_dir[0] = hdfs_dir[0] + b' ' + hdfs_dir[1]
                hdfs_dir[1] = hdfs_dir[2] + b' ' + hdfs_dir[3]
                hdfs_dir[2] = hdfs_dir[4]
            pdf.cell(230, 5, "{} - (Size = {} , Disk Space = {})".format(str(hdfs_dir[2],'utf-8'),str(hdfs_dir[0],'utf-8'),str(hdfs_dir[1],'utf-8')),0,ln=1)
            hdfs_inner_dir=obj2.getCliresult(hdfs_dir[2])
            for j in hdfs_inner_dir.splitlines():
                hdfs_inner_dir = j.split()
                if(len(hdfs_inner_dir) == 5):
                    hdfs_inner_dir[0] = hdfs_inner_dir[0] + b' ' + hdfs_inner_dir[1]
                    hdfs_inner_dir[1] = hdfs_inner_dir[2] + b' ' + hdfs_inner_dir[3]
                    hdfs_inner_dir[2] = hdfs_inner_dir[4]
                pdf.cell(230, 5, "    |-- {} - (Size = {} , Disk Space = {})".format(str(hdfs_inner_dir[2],'utf-8'),str(hdfs_inner_dir[0],'utf-8'),str(hdfs_inner_dir[1],'utf-8')),0,ln=1)
            pdf.cell(230, 3, "",0,ln=1)
        yarn_application_df = obj_app.getApplicationDetails(yarn_rm)
        app_count_df, app_type_count_df, app_status_count_df = obj_app.getApplicationTypeStatusCount(yarn_application_df)
        app_vcore_df, app_memory_df = obj_app.getApplicationVcoreMemoryUsage(yarn_application_df)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Yarn Application Metrics",0,ln=1)
        pdf.cell(230, 10, "",0,ln=1)
        pdf.set_font('Arial','', 12)
        pdf.set_font('Arial','B', 11)
        pdf.set_fill_color(r = 66, g = 133, b = 244)
        pdf.set_text_color(r = 255, g = 255, b = 255)
        pdf.cell(40, 5, 'Application Type', 1, 0, 'C',True)
        pdf.cell(40, 5, 'Status', 1, 0, 'C',True)
        pdf.cell(30, 5, 'Count', 1, 1, 'C',True)
        pdf.set_text_color(r = 1, g = 1, b = 1)
        pdf.set_fill_color(r = 244, g = 244, b = 244)
        pdf.set_font('Arial','', 11)
        for pos in range(0, len(app_count_df)):
            pdf.cell(40, 5, "{}".format(app_count_df['Application Type'].iloc[pos]), 1, 0, 'C',True)
            pdf.cell(40, 5, "{}".format(app_count_df['Status'].iloc[pos]), 1, 0, 'C',True)
            pdf.cell(30, 5, "{}".format(app_count_df['Count'].iloc[pos]), 1, 1, 'C',True)
        pdf.set_font('Arial','', 12)
        pdf.cell(230, 15, "",0,ln=1)
        x = pdf.get_x()
        y = pdf.get_y()
        plt.figure()
        app_type_count_pie_plot = app_type_count_df.plot.pie(y='Count', figsize=(6, 6),autopct='%.1f%%',title="Yarn Application by Type")
        plt.savefig('app_type_count_pie_plot.png')
        pdf.image('app_type_count_pie_plot.png', x = 15, y = None, w = 95, h = 95, type = '', link = '')
        pdf.set_xy(x,y)
        plt.figure()
        app_status_count_pie_plot = app_status_count_df.plot.pie(y='Count', figsize=(6, 6),autopct='%.1f%%',title="Yarn Application by Status")
        plt.savefig('app_status_count_pie_plot.png')
        pdf.image('app_status_count_pie_plot.png', x = 130, y = None, w = 95, h = 95, type = '', link = '')
        pdf.cell(230, 30, "",0,ln=1)
        plt.figure()
        x = pdf.get_x()
        y = pdf.get_y()
        app_vcore_plot = app_vcore_df.plot.pie(y='Vcore', figsize=(6, 6),autopct='%.1f%%',title="Yarn Application Vcore Usage")
        plt.savefig('app_vcore_plot.png')
        pdf.image('app_vcore_plot.png', x = 15, y = None, w = 95, h = 95, type = '', link = '')
        pdf.set_xy(x,y)
        plt.figure()
        app_memory_plot = app_memory_df.plot.pie(y='Memory', figsize=(6, 6),autopct='%.1f%%',title="Yarn Application Memory Usage")
        plt.savefig('app_memory_plot.png')
        pdf.image('app_memory_plot.png', x = 130, y = None, w = 95, h = 95, type = '', link = '')
        app_vcore_df, app_vcore_usage_df, app_memory_df, app_memory_usage_df = obj_app.getVcoreMemoryByApplication(yarn_application_df)
        pdf.add_page()
        plt.figure()
        for i in app_vcore_df['Application Type'].unique():
            app_vcore_df_temp = pd.DataFrame(None)
            app_vcore_df_temp = app_vcore_df[app_vcore_df['Application Type']==i]
            app_vcore_usage_df[i] = 0
            for index, row in app_vcore_df_temp.iterrows():
                val = (row['Launch Time'],0)
                if(val not in app_vcore_usage_df['Date']):
                    app_vcore_usage_df.loc[len(app_vcore_usage_df)] = val
                val = (row['Finished Time'],0)
                if(val not in app_vcore_usage_df['Date']):
                    app_vcore_usage_df.loc[len(app_vcore_usage_df)] = val
                app_vcore_usage_df.loc[(app_vcore_usage_df['Date']>= row['Launch Time']) & (app_vcore_usage_df['Date']< row['Finished Time']),i] = app_vcore_usage_df.loc[(app_vcore_usage_df['Date']>= row['Launch Time']) & (app_vcore_usage_df['Date']< row['Finished Time'])][i] + row['Vcore']
            app_vcore_usage_plot = app_vcore_usage_df.set_index('Date')[i].plot(label=i)
            app_vcore_usage_df = app_vcore_usage_df.drop([i], axis = 1)
        app_vcore_usage_plot.legend()
        app_vcore_usage_plot.set_ylabel('Application Vcores')
        plt.title("Vcore Breakdown By Application Type")
        plt.savefig('app_vcore_usage_plot.png')
        pdf.image('app_vcore_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        for i in app_memory_df['Application Type'].unique():
            app_memory_df_temp = pd.DataFrame(None)
            app_memory_df_temp = app_memory_df[app_memory_df['Application Type']==i]
            app_memory_usage_df[i] = 0
            for index, row in app_memory_df_temp.iterrows():
                val = (row['Launch Time'],0)
                if(val not in app_memory_usage_df['Date']):
                    app_memory_usage_df.loc[len(app_memory_usage_df)] = val
                val = (row['Finished Time'],0)
                if(val not in app_memory_usage_df['Date']):
                    app_memory_usage_df.loc[len(app_memory_usage_df)] = val
                app_memory_usage_df.loc[(app_memory_usage_df['Date']>= row['Launch Time']) & (app_memory_usage_df['Date']< row['Finished Time']),i] = app_memory_usage_df.loc[(app_memory_usage_df['Date']>= row['Launch Time']) & (app_memory_usage_df['Date']< row['Finished Time'])][i] + row['Memory']
            app_memory_usage_plot = app_memory_usage_df.set_index('Date')[i].plot(label=i)
            app_memory_usage_df = app_memory_usage_df.drop([i], axis = 1)
        app_memory_usage_plot.legend()
        app_memory_usage_plot.set_ylabel('Application Memory')
        plt.title("Memory Breakdown By Application Type")
        plt.savefig('app_memory_usage_plot.png')
        pdf.image('app_memory_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        yarn_pending_apps_df, yarn_pending_vcore_df, yarn_pending_memory_df = obj_app.getPendingApplication(cluster_name), obj_app.getPendingVcore(cluster_name), obj_app.getPendingMemory(cluster_name)
        pdf.add_page()
        plt.figure()
        yarn_pending_apps_plot = yarn_pending_apps_df['Max'].plot(color="steelblue",label='Pending Applications')
        yarn_pending_apps_plot.legend()
        yarn_pending_apps_plot.set_ylabel('Application Count')
        plt.title("Total Pending Applications Across YARN Pools")
        plt.savefig('yarn_pending_apps_plot.png')
        pdf.image('yarn_pending_apps_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_pending_vcore_plot = yarn_pending_vcore_df['Mean'].plot(color="steelblue",label='Pending Vcores')
        yarn_pending_vcore_plot.legend()
        yarn_pending_vcore_plot.set_ylabel('Vcores')
        plt.title("Total Pending VCores Across YARN Pools")
        plt.savefig('yarn_pending_vcore_plot.png')
        pdf.image('yarn_pending_vcore_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_pending_memory_plot = yarn_pending_memory_df['Mean'].plot(color="steelblue",label='Pending Memory')
        yarn_pending_memory_plot.legend()
        yarn_pending_memory_plot.set_ylabel('Memory (MB)')
        plt.title("Total Pending Memory Across YARN Pools")
        plt.savefig('yarn_pending_memory_plot.png')
        pdf.image('yarn_pending_memory_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        bursty_app_time_df, bursty_app_vcore_df, bursty_app_mem_df = obj_app.getBurstyApplicationDetails(yarn_application_df)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Bursty Applications",0,ln=1)
        if(bursty_app_time_df.size != 0):
            pdf.set_font('Arial', '', 12)
            pdf.cell(230, 5, "Bursty Applications - Elapsed Time",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(110, 5, 'Application Name', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Min Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Mean Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Max Time', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(bursty_app_time_df)):
                pdf.cell(110, 5, "{}".format(bursty_app_time_df['Application Name'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_time_df['Min'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_time_df['Mean'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_time_df['Max'].iloc[pos]), 1, 1, 'C',True)
            pdf.cell(230, 5, "",0,ln=1)
            plt.figure()
            bursty_app_time_df = bursty_app_time_df.set_index('Application Name')
            bursty_app_time_plot = bursty_app_time_df.plot.barh( stacked=True).legend(loc='upper center', ncol=3)
            plt.title("Bursty Applications - Elapsed Time")
            plt.xlabel("Time(secs)")
            plt.ylabel("Applications")
            plt.tight_layout()
            plt.savefig('bursty_app_time_plot.png')
            pdf.image('bursty_app_time_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
            pdf.cell(230, 5, "",0,ln=1)
            pdf.set_font('Arial', '', 12)
            pdf.cell(230, 5, "Bursty Applications - Vcore Seconds",0,ln=1)   
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(110, 5, 'Application Name', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Min Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Mean Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Max Time', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(bursty_app_vcore_df)):
                pdf.cell(110, 5, "{}".format(bursty_app_vcore_df['Application Name'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_vcore_df['Min'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_vcore_df['Mean'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_vcore_df['Max'].iloc[pos]), 1, 1, 'C',True)
            pdf.cell(230, 5, "",0,ln=1)
            plt.figure()
            bursty_app_vcore_df = bursty_app_vcore_df.set_index('Application Name')
            bursty_app_vcore_plot = bursty_app_vcore_df.plot.barh( stacked=True).legend(loc='upper center', ncol=3)
            plt.title("Bursty Applications - Vcore Seconds")
            plt.xlabel("Time(secs)")
            plt.ylabel("Applications")
            plt.tight_layout()
            plt.savefig('bursty_app_vcore_plot.png')
            pdf.image('bursty_app_vcore_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
            pdf.cell(230, 5, "",0,ln=1)
            pdf.set_font('Arial', '', 12)
            pdf.cell(230, 5, "Bursty Applications - Memory Seconds",0,ln=1)   
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(110, 5, 'Application Name', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Min Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Mean Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Max Time', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(bursty_app_mem_df)):
                pdf.cell(110, 5, "{}".format(bursty_app_mem_df['Application Name'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_mem_df['Min'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_mem_df['Mean'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_mem_df['Max'].iloc[pos]), 1, 1, 'C',True)
            pdf.cell(230, 5, "",0,ln=1)
            plt.figure()
            bursty_app_mem_df = bursty_app_mem_df.set_index('Application Name')
            bursty_app_mem_plot = bursty_app_mem_df.plot.barh( stacked=True).legend(loc='upper center', ncol=3)
            plt.title("Bursty Applications - Memory Seconds")
            plt.xlabel("Memory Seconds")
            plt.ylabel("Applications")
            plt.tight_layout()
            plt.savefig('bursty_app_mem_plot.png')
            pdf.image('bursty_app_mem_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        yarn_failed_app = obj_app.getFailedApplicationDetails(yarn_application_df)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Failed Applications",0,ln=1)
        pdf.set_font('Arial', '', 12)
        pdf.cell(230, 5, "Run time of Failed/Killed Applications = {: .2f} seconds".format(yarn_failed_app['ElapsedTime'].sum()),0,ln=1)
        pdf.cell(230, 5, "Vcores Seconds Used by Failed/Killed Applications = {} seconds".format(yarn_failed_app['MemorySeconds'].sum()),0,ln=1)
        pdf.cell(230, 5, "Memory Seconds Used Failed/Killed Applications = {} seconds".format(yarn_failed_app['VcoreSeconds'].sum()),0,ln=1)
        if(yarn_failed_app.size != 0):
            yarn_failed_app = yarn_failed_app.head(10)
            pdf.set_font('Arial','', 11)
            pdf.cell(230, 3, "",0,ln=1)
            pdf.cell(230, 8, "Top long running failed application diagnostics : ",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(40, 5, 'App Id', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Final Status', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Elapsed Time', 1, 0, 'C',True)
            pdf.cell(130, 5, 'Diagnostics', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(yarn_failed_app)):
                x = pdf.get_x()
                y = pdf.get_y()
                if(y > 300):
                    pdf.add_page()
                    x = pdf.get_x()
                    y = pdf.get_y()
                diag = yarn_failed_app['Diagnostics'].iloc[pos][:300]
                line_width = 0
                line_width = max(line_width,pdf.get_string_width(yarn_failed_app['ApplicationId'].iloc[pos]))
                cell_y = line_width/39.0
                line_width = max(line_width,pdf.get_string_width(yarn_failed_app['FinalStatus'].iloc[pos]))
                cell_y = max(cell_y,line_width/29.0)
                line_width = max(line_width,pdf.get_string_width(str(yarn_failed_app['ElapsedTime'].iloc[pos])))
                cell_y = max(cell_y,line_width/29.0)
                line_width = max(line_width,pdf.get_string_width(diag))
                cell_y = max(cell_y,line_width/129.0)
                cell_y = math.ceil(cell_y)
                cell_y = max(cell_y,1)
                cell_y = cell_y*5
                line_width = pdf.get_string_width(yarn_failed_app['ApplicationId'].iloc[pos])
                y_pos = line_width/39.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(40, y_pos, "{}".format(yarn_failed_app['ApplicationId'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+40,y)
                line_width = pdf.get_string_width(yarn_failed_app['FinalStatus'].iloc[pos])
                y_pos = line_width/29.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(30, y_pos, "{}".format(yarn_failed_app['FinalStatus'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+70,y)
                line_width = pdf.get_string_width(str(yarn_failed_app['ElapsedTime'].iloc[pos]))
                y_pos = line_width/29.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(30, y_pos, "{}".format(yarn_failed_app['ElapsedTime'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+100,y)
                line_width = pdf.get_string_width(diag)
                y_pos = line_width/129.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(130, y_pos, "{}".format(diag), 1, 'C',fill = True)
        yarn_total_vcores_count, yarn_vcore_available_df, [yarn_vcore_allocated_avg, yarn_vcore_allocated_df, yarn_vcore_allocated_pivot_df] = obj_app.getYarnTotalVcore(yarn_rm), obj_app.getYarnVcoreAvailable(cluster_name), obj_app.getYarnVcoreAllocated(cluster_name)
        yarn_total_memory_count, yarn_memory_available_df, [yarn_memory_allocated_avg, yarn_memory_allocated_df, yarn_memory_allocated_pivot_df] = obj_app.getYarnTotalMemory(yarn_rm), obj_app.getYarnMemoryAvailable(cluster_name), obj_app.getYarnMemoryAllocated(cluster_name)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Yarn Metrics",0,ln=1)
        pdf.set_font('Arial','', 12)
        pdf.cell(230, 3, "",0,ln=1)
        pdf.cell(230, 5, "Total Yarn Vcore : {:.0f}".format(yarn_total_vcores_count),0,ln=1)
        pdf.cell(230, 5, "Total Yarn Memory : {:.0f} GB".format(yarn_total_memory_count),0,ln=1)
        pdf.cell(230, 3, "",0,ln=1)
        pdf.cell(230, 5, "Average No. of Vcores Used : {: .2f}".format(yarn_vcore_allocated_avg),0,ln=1)
        pdf.cell(230, 5, "Average Yarn Memory Used : {:.0f} MB".format(yarn_memory_allocated_avg),0,ln=1)
        plt.figure()
        yarn_vcore_usage_plot = yarn_vcore_available_df['Mean'].plot(color="steelblue",label='Vcores Available')
        yarn_vcore_usage_plot = yarn_vcore_allocated_df['Mean'].plot(color="darkorange",label='Vcores Allocated (Mean)')
        yarn_vcore_usage_plot = yarn_vcore_allocated_df['Max'].plot(color="red",label='Vcores Allocated (Max)',linestyle="--",linewidth=1)
        yarn_vcore_usage_plot.legend()
        yarn_vcore_usage_plot.set_ylabel('Total Vcore Usage')
        plt.title("Yarn Vcore Usage")
        plt.savefig('yarn_vcore_usage_plot.png')
        pdf.image('yarn_vcore_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_memory_usage_plot = yarn_memory_available_df['Mean'].plot(color="steelblue",label='Memory Available')
        yarn_memory_usage_plot = yarn_memory_allocated_df['Mean'].plot(color="darkorange",label='Memory Allocated (Mean)')
        yarn_memory_usage_plot = yarn_memory_allocated_df['Max'].plot(color="red",label='Memory Allocated (Max)',linestyle="--",linewidth=1)
        yarn_memory_usage_plot.legend()
        yarn_memory_usage_plot.set_ylabel('Total Yarn Memory(MB)')
        plt.title("Yarn Memory Usage")
        plt.savefig('yarn_memory_usage_plot.png')
        pdf.image('yarn_memory_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_vcore_usage_heatmap = sns.heatmap(yarn_vcore_allocated_pivot_df,cmap="OrRd")
        plt.title('Yarn Vcore Usage')
        plt.savefig('yarn_vcore_usage_heatmap.png')
        pdf.image('yarn_vcore_usage_heatmap.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_memory_usage_heatmap = sns.heatmap(yarn_memory_allocated_pivot_df,cmap="OrRd")
        plt.title('Yarn Memory Usage')
        plt.savefig('yarn_memory_usage_heatmap.png')
        pdf.image('yarn_memory_usage_heatmap.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        yarn_queues_list, [queue_app_count_df, queue_elapsed_time_df], [app_queue_df, app_queue_usage_df], [queue_vcore_df, queue_vcore_usage_df, queue_memory_df, queue_memory_usage_df] = obj_app.getQueueDetails(yarn_rm), obj_app.getQueueApplication(yarn_application_df), obj_app.getQueuePendingApplication(yarn_application_df), obj_app.getQueueVcoreMemory(yarn_application_df)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Yarn Queues",0,ln=1)
        pdf.cell(230, 5, "",0,ln=1)
        pdf.set_font('Arial','', 12)
        def yarn_queue(yarn_queues_list,count):
            for queue in yarn_queues_list:
                if('queues' in queue):
                    pdf.cell(10*count, 5, "", 0, 0)
                    pdf.cell(30, 5, "|-- {} - (Absolute Capacity - {}, Max Capacity - {})".format(queue['queueName'],queue['absoluteCapacity'],queue['absoluteMaxCapacity']), 0, ln=1)
                    yarn_queue(queue['queues']['queue'],count+1)
                else:
                    pdf.cell(10*count, 5, "", 0, 0)
                    pdf.cell(30, 5, "|-- {} - (Absolute Capacity - {}, Max Capacity - {})".format(queue['queueName'],queue['absoluteCapacity'],queue['absoluteMaxCapacity']), 0, ln=1)
        pdf.cell(230, 5, "Queue Structure : ",0,ln=1)
        pdf.cell(230, 5, "Root - (Absolute Capacity - 100, Max Capacity - 100)",0,ln=1)
        yarn_queue(yarn_queues_list,1)
        pdf.cell(230, 10, "",0,ln=1)
        def make_autopct(values):
            def my_autopct(pct):
                total = sum(values)
                val = int(round(pct*total/100.0))
                return '{p:.2f}%  ({v:d})'.format(p=pct,v=val)
            return my_autopct
        plt.figure()
        x = pdf.get_x()
        y = pdf.get_y()
        queue_app_count_plot = queue_app_count_df.plot.pie(y='Application Count', figsize=(6, 6),autopct=make_autopct(queue_app_count_df['Application Count']),title="Queue Application Count (Weekly)")
        plt.savefig('queue_app_count_plot.png')
        pdf.image('queue_app_count_plot.png', x = 15, y = None, w = 95, h = 95, type = '', link = '')
        pdf.set_xy(x,y)
        plt.figure()
        queue_elapsed_time_plot = queue_elapsed_time_df.plot.pie(y='Elapsed Time', figsize=(6, 6),autopct='%.1f%%',title="Queue Elapsed Time (Weekly)")
        plt.savefig('queue_elapsed_time_plot.png')
        pdf.image('queue_elapsed_time_plot.png', x = 130, y = None, w = 95, h = 95, type = '', link = '')
        plt.figure()
        for i in app_queue_df['Queue'].unique():
            app_queue_df_temp = pd.DataFrame(None)
            app_queue_df_temp = app_queue_df[(app_queue_df['Queue']==i) & (app_queue_df['Wait Time'] > timedelta(minutes=5))]
            app_queue_usage_df[i] = 0
            for index, row in app_queue_df_temp.iterrows():
                val = (row['Start Time'],0)
                if(val not in app_queue_usage_df['Date']):
                    app_queue_usage_df.loc[len(app_queue_usage_df)] = val
                val = (row['Launch Time'],0)
                if(val not in app_queue_usage_df['Date']):
                    app_queue_usage_df.loc[len(app_queue_usage_df)] = val
                app_queue_usage_df.loc[(app_queue_usage_df['Date']>= row['Start Time']) & (app_queue_usage_df['Date']< row['Launch Time']),i] = app_queue_usage_df.loc[(app_queue_usage_df['Date']>= row['Start Time']) & (app_queue_usage_df['Date']< row['Launch Time'])][i] + 1
            app_queue_usage_plot = app_queue_usage_df.set_index('Date')[i].plot(label=i)
            app_queue_usage_df = app_queue_usage_df.drop([i], axis = 1)
        app_queue_usage_plot.legend()
        app_queue_usage_plot.set_ylabel('Application Count')
        plt.title("Application Pending by Queue")
        plt.savefig('app_queue_usage_plot.png')
        pdf.image('app_queue_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        pdf.cell(230, 10, "",0,ln=1)
        plt.figure()
        for i in queue_vcore_df['Queue'].unique():
            queue_vcore_df_temp = pd.DataFrame(None)
            queue_vcore_df_temp = queue_vcore_df[queue_vcore_df['Queue']==i]
            queue_vcore_usage_df[i] = 0
            for index, row in queue_vcore_df_temp.iterrows():
                val = (row['Launch Time'],0)
                if(val not in queue_vcore_usage_df['Date']):
                    queue_vcore_usage_df.loc[len(queue_vcore_usage_df)] = val
                val = (row['Finished Time'],0)
                if(val not in queue_vcore_usage_df['Date']):
                    queue_vcore_usage_df.loc[len(queue_vcore_usage_df)] = val
                queue_vcore_usage_df.loc[(queue_vcore_usage_df['Date']>= row['Launch Time']) & (queue_vcore_usage_df['Date']< row['Finished Time']),i] = queue_vcore_usage_df.loc[(queue_vcore_usage_df['Date']>= row['Launch Time']) & (queue_vcore_usage_df['Date']< row['Finished Time'])][i] + row['Vcore']
            queue_vcore_usage_plot = queue_vcore_usage_df.set_index('Date')[i].plot(label=i)
            queue_vcore_usage_df = queue_vcore_usage_df.drop([i], axis = 1)
        queue_vcore_usage_plot.legend()
        queue_vcore_usage_plot.set_ylabel('Application Vcores')
        plt.title("Vcore Breakdown By Queue")
        plt.savefig('queue_vcore_usage_plot.png')
        pdf.image('queue_vcore_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        for i in queue_memory_df['Queue'].unique():
            queue_memory_df_temp = pd.DataFrame(None)
            queue_memory_df_temp = queue_memory_df[queue_memory_df['Queue']==i]
            queue_memory_usage_df[i] = 0
            for index, row in queue_memory_df_temp.iterrows():
                val = (row['Launch Time'],0)
                if(val not in queue_memory_usage_df['Date']):
                    queue_memory_usage_df.loc[len(queue_memory_usage_df)] = val
                val = (row['Finished Time'],0)
                if(val not in queue_memory_usage_df['Date']):
                    queue_memory_usage_df.loc[len(queue_memory_usage_df)] = val
                queue_memory_usage_df.loc[(queue_memory_usage_df['Date']>= row['Launch Time']) & (queue_memory_usage_df['Date']< row['Finished Time']),i] = queue_memory_usage_df.loc[(queue_memory_usage_df['Date']>= row['Launch Time']) & (queue_memory_usage_df['Date']< row['Finished Time'])][i] + row['Memory']
            queue_memory_usage_plot = queue_memory_usage_df.set_index('Date')[i].plot(label=i)
            queue_memory_usage_df = queue_memory_usage_df.drop([i], axis = 1)
        queue_memory_usage_plot.legend()
        queue_memory_usage_plot.set_ylabel('Application Memory')
        plt.title("Memory Breakdown By Queue")
        plt.savefig('queue_memory_usage_plot.png')
        pdf.image('queue_memory_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        pdf.output('Discovery_Report/{}.pdf'.format(cluster_name), 'F')

    def run_7():
        obj1=HardwareOSAPI
        obj2=DataAPI
        obj3=FrameworkDetailsAPI
        obj_app=ApplicationAPI
        yarn_rm = ""
        hive_server2 = ""
        if os.path.exists("Discovery_Report"):
            shutil.rmtree("Discovery_Report")
        os.makedirs("Discovery_Report")
        yarn_rm = ""
        hive_server2 = ""

        pdf = FPDF(format=(250,350))
        pdf.add_page()
        pdf.set_font('Arial', 'B', 16)
        pdf.cell(230, 10, "Hadoop Discovery Report",0,ln=1,align = 'C')
        pdf.set_font('Arial','', 12)
        pdf.cell(230, 8, "Report Date Range : Start  {} ".format(date_range_start),0,ln=1,align='R')
        pdf.cell(0, 8, "End  {} ".format(date_range_end),0,ln=1,align='R')
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Cluster Information",0,ln=1)
        pdf.set_font('Arial','', 12)
        cluster_items=obj1.clusterItems()
        
        #Number of cluster configured
        pdf.cell(230, 8, "Number of Cluster Configured : {}".format(len(cluster_items)),0,ln=1)
        pdf.cell(230, 8, "Cluster Details : ",0,ln=1)
        pdf.set_font('Arial','B', 11)
        cluster_df = pd.DataFrame(cluster_items,columns =['name','fullVersion','clusterType','entityStatus'])
        cluster_df.index = cluster_df.index + 1
        cluster_df = cluster_df.rename(columns={"name": "Cluster Name", "fullVersion": "Cloudera Version", "clusterType": "Cluster Type","entityStatus": "Health Status"})
        pdf.set_fill_color(r = 66, g = 133, b = 244)
        
        #Cluster Details
        pdf.set_text_color(r = 255, g = 255, b = 255)
        pdf.cell(40, 5, 'Cluster Name', 1, 0, 'C',True)
        pdf.cell(40, 5, 'Cloudera Version', 1, 0, 'C',True)
        pdf.cell(50, 5, 'Cluster Type', 1, 0,'C',True)
        pdf.cell(60, 5, 'Health Status', 1, 1, 'C',True)
        pdf.set_text_color(r = 1, g = 1, b = 1)
        pdf.set_fill_color(r = 244, g = 244, b = 244)
        pdf.set_font('Arial','', 11)
        for pos in range(0, len(cluster_df)):
            x = pdf.get_x()
            y = pdf.get_y()
            if(y > 300):
                pdf.add_page()
                x = pdf.get_x()
                y = pdf.get_y()
            line_width = 0
            line_width = max(line_width,pdf.get_string_width(cluster_df['Cluster Name'].iloc[pos]))
            cell_y = line_width/39.0
            line_width = max(line_width,pdf.get_string_width(cluster_df['Cloudera Version'].iloc[pos]))
            cell_y = max(cell_y,line_width/39.0)
            line_width = max(line_width,pdf.get_string_width(cluster_df['Cluster Type'].iloc[pos]))
            cell_y = max(cell_y,line_width/49.0)
            line_width = max(line_width,pdf.get_string_width(cluster_df['Health Status'].iloc[pos]))
            cell_y = max(cell_y,line_width/59.0)
            cell_y = line_width/39.0
            cell_y = math.ceil(cell_y)
            cell_y = max(cell_y,1)
            cell_y = cell_y*5
            line_width = pdf.get_string_width(cluster_df['Cluster Name'].iloc[pos])
            y_pos = line_width/39.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos,1)
            y_pos = cell_y/y_pos
            pdf.multi_cell(40, y_pos, "{}".format(cluster_df['Cluster Name'].iloc[pos]), 1, 'C',fill = True)
            pdf.set_xy(x+40,y)
            line_width = pdf.get_string_width(cluster_df['Cloudera Version'].iloc[pos])
            y_pos = line_width/39.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos,1)
            y_pos = cell_y/y_pos
            pdf.multi_cell(40, y_pos, "{}".format(cluster_df['Cloudera Version'].iloc[pos]), 1, 'C',fill = True)
            pdf.set_xy(x+80,y)
            line_width = pdf.get_string_width(cluster_df['Cluster Type'].iloc[pos])
            y_pos = line_width/49.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos,1)
            y_pos = cell_y/y_pos
            pdf.multi_cell(50, y_pos, "{}".format(cluster_df['Cluster Type'].iloc[pos]), 1, 'C',fill = True)
            pdf.set_xy(x+130,y)
            line_width = pdf.get_string_width(cluster_df['Health Status'].iloc[pos])
            y_pos = line_width/59.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos,1)
            y_pos = cell_y/y_pos
            pdf.multi_cell(60, y_pos, "{}".format(cluster_df['Health Status'].iloc[pos]), 1, 'C',fill = True)
        cluster_count = 0
        host_counts = 0
        service_count = 0
        pdf.cell(190, 5, "",0,ln=1)
        pdf.set_font('Arial','', 12)
        clusterName=""
        for i in cluster_items:
            cluster_name = i['name']
            clusterName=cluster_name
            if os.path.exists("Discovery_Report/{}".format(cluster_name)):
                shutil.rmtree("Discovery_Report/{}".format(cluster_name))
            os.makedirs("Discovery_Report/{}".format(cluster_name))
            cluster_count = cluster_count+1
            
            #Listing details for Cluster
            pdf.cell(230, 8,"Listing Details for Cluster {} - {}".format(cluster_count,cluster_name),0,ln=1)
            cluster_host_items,clusterHostLen=obj1.clusterHostItems(cluster_name)
            if os.path.exists('Discovery_Report/{}/host_details.json'.format(cluster_name)):
                os.remove('Discovery_Report/{}/host_details.json'.format(cluster_name))
            
            
            #Number of host in the cluster 
            pdf.cell(230, 8, "Number of Host               : {}".format(clusterHostLen),0,ln=1)
            host_df = pd.DataFrame(columns=['Hostname', 'Host ip','Number of cores','Physical Memory','Health Status','Distribution'])
            for j in cluster_host_items:
                host_counts = host_counts+1
                host_count = obj1.hostData(j['hostId'])
                for role in host_count['roleRefs']:
                    if "RESOURCEMANAGER" in role['roleName'].upper():
                        yarn_rm = host_count['ipAddress']
                    if "HIVESERVER2" in role['roleName'].upper():
                        hive_server2 = host_count['ipAddress']
                with open('Discovery_Report/{}/host_details.json'.format(cluster_name), 'a') as fp:
                    json.dump(host_count, fp,  indent=4)
                host_tmp_df = pd.DataFrame({'Hostname': host_count['hostname'], 'Host IP': host_count['ipAddress'],'Cores' : host_count['numCores'],'Memory' : "{: .2f} GB".format(float(host_count['totalPhysMemBytes'])/1024/1024/1024),'Health Status' : host_count['entityStatus'],'Distribution' : host_count['distribution']['name'] + " " + host_count['distribution']['version']}, index=[host_count])
                host_df = host_df.append(host_tmp_df)
            
            #Total Data Nodes in the cluster
            tot_datanode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                data_count =obj1.hostData(j['hostId'])
                for role in data_count['roleRefs']:
                    if re.search(r'\bDATANODE\b', role['roleName']):
                        host_tmp_df_1 = pd.DataFrame({'Cores' : data_count['numCores'],'Memory' : "{: .2f}".format(float(data_count['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        tot_datanode_df = tot_datanode_df.append(host_tmp_df_1)
            var_total_datanode=tot_datanode_df['Memory'].tolist()
            len_datanodes=len(var_total_datanode)
            pdf.cell(230, 8, "Number of DataNodes    : {}".format(len_datanodes),0,ln=1)

            #Total Name Nodes in the cluster
            tot_namenode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                    node_count =obj1.hostData(j['hostId'])
                    for role in node_count['roleRefs']:
                        if re.search(r'\bNAMENODE\b', role['roleName']):
                            host_tmp_df_1 = pd.DataFrame({'Cores' : node_count['numCores'],'Memory' : "{: .2f}".format(float(node_count['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                            tot_namenode_df = tot_namenode_df.append(host_tmp_df_1)
            var_total_namemode=tot_namenode_df['Memory'].tolist()
            len_namenodes=len(var_total_namemode)
            pdf.cell(230, 8, "Number of NameNodes  : {}".format(len_namenodes),0,ln=1)
            
            #Number of Edge Nodes
            num_edgenode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                redgenode=obj1.hostData(j['hostId'])
                for role in redgenode['roleRefs']:
                    if re.search(r'\bGATEWAY\b', role['roleName']) and "hdfs" in role['serviceName']:
                        host_tmp_df_1 = pd.DataFrame({'Host IP': redgenode['ipAddress']}, index=[host_count])
                        num_edgenode_df = num_edgenode_df.append(host_tmp_df_1)
            #pdf.cell(230, 8,"Number of Edge Nodes : {} ".format(len(num_edgenode_df)),0,ln=1)
            pdf.cell(230, 8,"Number of Edge Nodes  : {} ".format(len(num_edgenode_df)),0,ln=1)
            
            #Host Details
            pdf.cell(230, 8, "Host Details : ",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(90, 5, 'Hostname', 1, 0, 'C',True)
            pdf.cell(20, 5, 'Host IP', 1, 0, 'C',True)
            pdf.cell(15, 5, 'Cores', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Memory', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Health Status', 1, 0, 'C',True)
            pdf.cell(40, 5, 'Distribution', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(host_df)):
                pdf.cell(90, 5, "{}".format(host_df['Hostname'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(20, 5, "{}".format(host_df['Host IP'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(15, 5, "{}".format(host_df['Cores'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{}".format(host_df['Memory'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{}".format(host_df['Health Status'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(40, 5, "{}".format(host_df['Distribution'].iloc[pos]), 1, 1, 'C',True)    
            cluster_service_item=obj1.clusterServiceItem(cluster_name)
            service_df = pd.DataFrame(columns=['Service Name','Health Status','Health Concerns'])
            for k in cluster_service_item:
                if(k['serviceState']!='STARTED'):
                    continue
                service_count = service_count + 1
                concerns = ""
                if(k['entityStatus']!='GOOD_HEALTH'):
                    for l in k['healthChecks']:
                        if(l['summary']!='GOOD'):
                            if(concerns == ""):
                                concerns = l['name']
                            else:
                                concerns = concerns + "\n" + l['name']
                service_tmp_df = pd.DataFrame({'Service Name': k['name'], 'Health Status': k['entityStatus'],'Health Concerns' : concerns}, index=[service_count])
                service_df = service_df.append(service_tmp_df)
            
            #Services running in the cluster
            pdf.set_font('Arial','', 12)
            pdf.cell(230, 3, "",0,ln=1)
            pdf.cell(230, 8, "Services Running in the Cluster : ",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(60, 5, 'Service Name', 1, 0, 'C',True)
            pdf.cell(60, 5, 'Health Status', 1, 0, 'C',True)
            pdf.cell(90, 5, 'Health Concerns', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(service_df)):
                x = pdf.get_x()
                y = pdf.get_y()
                if(y > 300):
                    pdf.add_page()
                    x = pdf.get_x()
                    y = pdf.get_y()
                line_width = 0
                line_width = max(line_width,pdf.get_string_width(service_df['Service Name'].iloc[pos]))
                cell_y = line_width/59.0
                line_width = max(line_width,pdf.get_string_width(service_df['Health Status'].iloc[pos]))
                cell_y = max(cell_y,line_width/59.0)
                line_width = max(line_width,pdf.get_string_width(service_df['Health Concerns'].iloc[pos]))
                cell_y = max(cell_y,line_width/89.0)
                #cell_y = line_width/59.0
                cell_y = math.ceil(cell_y)
                cell_y = max(cell_y,1)
                cell_y = cell_y*5
                line_width = pdf.get_string_width(service_df['Service Name'].iloc[pos])
                y_pos = line_width/59.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(60, y_pos, "{}".format(service_df['Service Name'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+60,y)
                line_width = pdf.get_string_width(service_df['Health Status'].iloc[pos])
                y_pos = line_width/59.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(60, y_pos, "{}".format(service_df['Health Status'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+120,y)
                line_width = pdf.get_string_width(service_df['Health Concerns'].iloc[pos])
                y_pos = line_width/89.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(90, y_pos, "{}".format(service_df['Health Concerns'].iloc[pos]), 1, 'C',fill = True)
            
        for data in cluster_items: 
        #Memory Allocated per node  
            memory_edgenode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                edge_memory =obj1.hostData(j['hostId'])
                for role in edge_memory['roleRefs']:
                    #re.search(r'\b' NAMENODE r'\b', role['roleName'].upper()) re.search(r'\bNAMENODE\b', role['roleName'])
                    if re.search(r'\bGATEWAY\b', role['roleName']) and "hdfs" in role['serviceName']:
                        #print(role['roleName'])
                        host_tmp_df_1 = pd.DataFrame({'Nodename':role['roleName'],'Memory' : "{: .2f}".format(float(edge_memory['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        memory_edgenode_df = memory_edgenode_df.append(host_tmp_df_1)

            #print(memory_edgenode_df)
            if memory_edgenode_df.empty:
                #print("Zero edgenode detected")
                pdf.cell(230, 8,"Edgenode Detected : Zero",0,ln=1)
            else:
                pdf.cell(230, 8, "Memory Allocated to edge nodes : ",0,ln=1)
                pdf.set_font('Arial','B', 11)
                pdf.set_fill_color(r = 66, g = 133, b = 244)
                pdf.set_text_color(r = 255, g = 255, b = 255)
                pdf.cell(100, 5, 'Nodename', 1, 0, 'C',True)
                pdf.cell(30, 5, 'Memory', 1, 1, 'C',True)
                pdf.set_text_color(r = 1, g = 1, b = 1)
                pdf.set_fill_color(r = 244, g = 244, b = 244)
                pdf.set_font('Arial','', 11)
                for pos in range(0, len(memory_edgenode_df)):
                    # print(memory_edgenode_df['Nodename'])
                    # print(memory_edgenode_df['Memory'])
                    pdf.cell(100, 5, "{}".format(memory_edgenode_df['Nodename'].iloc[pos]), 1, 0, 'C',True)
                    pdf.cell(30, 5, "{}".format(memory_edgenode_df['Memory'].iloc[pos]), 1, 1, 'C',True)
            
            clints_onEdgenode=pd.DataFrame(columns=[])
            for j in cluster_host_items:
                edge_services =obj1.hostData(j['hostId'])
                for role in edge_services['roleRefs']:
                    #re.search(r'\b' NAMENODE r'\b', role['roleName'].upper()) re.search(r'\bNAMENODE\b', role['roleName'])
                    if "GATEWAY" in role['roleName']:
                        a=pd.DataFrame({'service':role['serviceName']}, index=[host_count])
                        clints_onEdgenode=clints_onEdgenode.append(a)
            clints_onEdgenode=clints_onEdgenode.drop_duplicates()
            pdf.cell(230, 8, "Clients Installed on Gateway : ",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(70, 5, 'Services', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(clints_onEdgenode)):
                pdf.cell(70, 5, "{}".format(clints_onEdgenode['service'].iloc[pos]), 1, 1, 'C',True)
            
            #Kerberos Details
            pdf.set_font('Arial','', 12)
            pdf.cell(230, 3, "",0,ln=1)
            pdf.cell(230, 8, "Kerberos Details :",0,ln=1)
            cluster_kerberos_info=obj1.clusterKerberosInfo(cluster_name)
            with open('Discovery_Report/{}/cluster_kerberos_info.json'.format(cluster_name), 'w') as fp:
                json.dump(cluster_kerberos_info, fp,  indent=4)
            kerberized_status = str(cluster_kerberos_info['kerberized'])
            if(kerberized_status=="True"):
                #print("Cluster is kerberized")
                pdf.cell(230, 5, "Cluster is kerberized",0,ln=1)                 
            else:
                #print("Cluster is not kerberized")
                pdf.cell(230, 5, "Cluster is not kerberized",0,ln=1)                
            
            pdf.cell(230, 8, "Cluster Details :",0,ln=1)
            
            
        #Total Memory Assigned to all the Master Nodes
        tot_memory_master_df = pd.DataFrame(columns=[])
        for j in cluster_host_items:
            rmasternode=obj1.hostData(j['hostId'])
            for role in rmasternode['roleRefs']:                
                if re.search(r'\bNAMENODE\b', role['roleName']) and "hdfs" in role['serviceName']:                   
                    host_tmp_df_1 = pd.DataFrame({'Cores' : rmasternode['numCores'],'Memory' : "{: .2f}".format(float(rmasternode['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                    tot_memory_master_df = tot_memory_master_df.append(host_tmp_df_1)
        var_memory_master=tot_memory_master_df['Memory'].tolist()
        tot_memory_master = 0 
        itr=0
        for itr in var_memory_master:  
            tot_memory_master += float(itr)      
        pdf.cell(230, 8,"Total Memory Assigned to All the MasterNodes : {: .2f} GB  ".format(tot_memory_master),0,ln=1)
        length_memory_master=len(var_memory_master)
        itr=0
        while itr < length_memory_master:
            pdf.cell(230, 8,"    Memory Available in MasterNode {} : {} GB ".format(itr+1,var_memory_master[itr]),0,ln=1)
            itr=itr+1
        
        #Total Memory Assigned to all the Data Nodes and per node
        tot_memory_datanode_df = pd.DataFrame(columns=[])
        for j in cluster_host_items:
            rdatanode=obj1.hostData(j['hostId'])
            for role in rdatanode['roleRefs']:
                if re.search(r'\bDATANODE\b', role['roleName']) and "hdfs" in role['serviceName']:
                    host_tmp_df_1 = pd.DataFrame({'Cores' : rdatanode['numCores'],'Memory' : "{: .2f}".format(float(rdatanode['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                    tot_memory_datanode_df = tot_memory_datanode_df.append(host_tmp_df_1)
        var_memory_datanode=tot_memory_datanode_df['Memory'].tolist()
        tot_memory_datanode = 0 
        itr=0
        for itr in var_memory_datanode:  
            tot_memory_datanode += float(itr)      
        pdf.cell(230, 8,"Total Memory Assigned to All the DataNodes : {: .2f} GB  ".format(tot_memory_datanode),0,ln=1)
        length_memory_datanode=len(var_memory_datanode)
        itr=0
        while itr < length_memory_datanode:
            pdf.cell(230, 8,"    Memory Available in DataNode {}     : {} GB ".format(itr+1,var_memory_datanode[itr]),0,ln=1)
            itr=itr+1
        
            
        #Using cloudera apis fetching cluster resource avalilability over a period of time
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Cluster Metrics",0,ln=1)
        pdf.set_font('Arial','', 12)
        cluster_total_cores_df=obj1.clusterTotalCores(cluster_name)
        cluster_cpu_usage_df,cluster_cpu_usage_avg=obj1.clusterCpuUsage(cluster_name)
        cluster_total_memory_df=obj1.clusterTotalMemory(cluster_name)
        cluster_memory_usage_df,cluster_memory_usage_avg=obj1.clusterMemoryUsage(cluster_name)

        #Total Cores Assigned to all the master nodes
        tot_core_masternode = 0 
        for i in cluster_items:            
            tot_cores_masternode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                rmasternode=obj1.hostData(j['hostId'])
                for role in rmasternode['roleRefs']:
                    if re.search(r'\bNAMENODE\b', role['roleName']) and "hdfs" in role['serviceName']:
                        host_tmp_df_1 = pd.DataFrame({'Cores' : rmasternode['numCores'],'Memory' : "{: .2f}".format(float(rmasternode['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        tot_cores_masternode_df = tot_cores_masternode_df.append(host_tmp_df_1)
            var_core_master=tot_cores_masternode_df['Cores'].tolist()
            for ele in var_core_master:  
                tot_core_masternode += int(ele)         
        
            
        #Total Cores Assigned to all the Data Nodes
        tot_core_datanode = 0
        for i in cluster_items:            
            tot_cores_datanode_df = pd.DataFrame(columns=[])
            for j in cluster_host_items:
                rdatanode=obj1.hostData(j['hostId'])
                for role in rdatanode['roleRefs']:
                    if re.search(r'\bDATANODE\b', role['roleName']) and "hdfs" in role['serviceName']:
                        host_tmp_df_1 = pd.DataFrame({'Cores' : rdatanode['numCores'],'Memory' : "{: .2f}".format(float(rdatanode['totalPhysMemBytes'])/1024/1024/1024)}, index=[host_count])
                        tot_cores_datanode_df = tot_cores_datanode_df.append(host_tmp_df_1)
            var_core_data=tot_cores_datanode_df['Cores'].tolist() 
            for ele in var_core_data:  
                tot_core_datanode += int(ele)
            
                
        total_cores_cluster=tot_core_datanode+tot_core_masternode    
        pdf.cell(230, 3, "",0,ln=1)
        pdf.cell(230, 5, "Total CPU Core in the cluster                            : {: .0f}".format(total_cores_cluster),0,ln=1)
        pdf.cell(230, 8,"Total Cores Assigned to the MasterNode        : {} ".format(tot_core_masternode),0,ln=1)
        length_core_master=len(var_core_master)
        itr=0
        while itr < length_core_master:
            pdf.cell(230, 8,"    Cores Assigned to MasterNode {} : {} ".format(itr+1,var_core_master[itr]),0,ln=1)
            itr=itr+1     
        pdf.cell(230, 8,"Total Cores Assigned to All the DataNodes    : {} ".format(tot_core_datanode),0,ln=1)
        length_core_data=len(var_core_data)
        itr=0
        while itr < length_core_data:
            pdf.cell(230, 8,"    Cores Assigned to DataNode {}     : {} ".format(itr+1,var_core_data[itr]),0,ln=1)
            itr=itr+1       
        #print("Total CPU core in the cluster : {: .0f}".format(cluster_total_cores_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']))
        pdf.cell(230, 5, "Total Memory in the cluster                              : {: .2f} GB".format(cluster_total_memory_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        pdf.cell(230, 3, "",0,ln=1)
        #pdf.cell(230, 5, "Time Duration in consideration : {} to {} ".format(cluster_total_cores_df['DateTime'].min(),cluster_total_cores_df['DateTime'].max()),0,ln=1)
        pdf.cell(230, 5, "Time Duration in Consideration : {} - {} ".format(datetime.strptime(str(cluster_total_cores_df['DateTime'].min()), '%Y-%m-%d %H:%M:%S').strftime('%b %d %Y'),datetime.strptime(str(cluster_total_cores_df['DateTime'].max()), '%Y-%m-%d %H:%M:%S').strftime('%b %d %Y')),0,ln=1)
        pdf.cell(230, 3, "",0,ln=1)
        pdf.cell(230, 5, "Average Cluster CPU Utilization is {: .2f}%".format(cluster_cpu_usage_avg),0,ln=1)
        pdf.cell(230, 5, "Average Cluster Memory Utilization is {: .2f}%".format(cluster_memory_usage_avg),0,ln=1)

        plt.figure(1)
        cluster_total_cores_plot = cluster_total_cores_df['Mean'].plot(color="steelblue",label='Available Cores')
        cluster_total_cores_plot.set_ylabel('Total CPU Cores')
        cluster_total_cores_plot.legend()
        plt.title("Cluster Vcore Availability")
        plt.savefig('cluster_total_cores_plot.png')
        pdf.image('cluster_total_cores_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')

        plt.figure(2)
        cluster_cpu_usage_plot = cluster_cpu_usage_df['Max'].plot(color="red",linestyle="--",label='Max Core Allocated',linewidth=1)
        cluster_cpu_usage_plot = cluster_cpu_usage_df['Mean'].plot(color="steelblue",label='Mean Cores Allocated')
        cluster_cpu_usage_plot = cluster_cpu_usage_df['Min'].plot(color="lime",linestyle="--",label='Min Cores Allocated',linewidth=1)
        cluster_cpu_usage_plot.legend()
        cluster_cpu_usage_plot.set_ylabel('CPU Utilization %')
        plt.title("Cluster Vcore Usage")
        plt.savefig('cluster_cpu_usage_plot.png')
        pdf.image('cluster_cpu_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')

        plt.figure(3)
        cluster_total_memory_plot = cluster_total_memory_df['Mean'].plot(color="steelblue",label='Avaliable Memory')
        cluster_total_memory_plot.set_ylabel('Total Memory(GB)')
        cluster_total_memory_plot.legend()
        plt.title("Cluster Memory Availability")
        plt.savefig('cluster_total_memory_plot.png')
        pdf.image('cluster_total_memory_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')

        plt.figure(4)
        cluster_memory_usage_plot = cluster_memory_usage_df['Mean'].plot(color="steelblue",label='Memory Allocated')
        cluster_memory_usage_plot.legend()
        cluster_memory_usage_plot.set_ylabel('Memory Utilization %')
        plt.title("Cluster Memory Usage")
        plt.savefig('cluster_memory_usage_plot.png')
        pdf.image('cluster_memory_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        
        
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Frameworks and Software Details",0,ln=1)
        pdf.set_font('Arial','', 12)
        # clouderaEnabled=obj3.checkCloudera()
        # if(clouderaEnabled==1):
            # pdf.cell(230, 8,"Cloudera Enabled ",0,ln=1)    
        # else:
            # pdf.cell(230, 8,"",0,ln=1)  
        
        hadoopVersionMajor,hadoopVersionMinor,distribution=obj3.hadoopVersion()
        pdf.cell(230, 8,"Hadoop Major Version Is     : {} ".format(hadoopVersionMajor),0,ln=1)
        pdf.cell(230, 8,"Hadoop Minor Version Is     : {} ".format(hadoopVersionMinor),0,ln=1)
        pdf.cell(230, 8,"Hadoop Distribution Is        : {} ".format(distribution),0,ln=1)
        
        # List of installed Apache Services (Eg. Hadoop, Spark, Kafka, Yarn, Hive, etc.)
        list_services_installed_df,new_ref_df=obj3.versionMapping(cluster_name)
        pdf.cell(230, 8, "List of Services Installed  : ",0,ln=1)
        pdf.set_font('Arial','B', 11)
        pdf.set_fill_color(r = 66, g = 133, b = 244)
        pdf.set_text_color(r = 255, g = 255, b = 255)
        pdf.cell(70, 5, 'Name', 1, 0, 'C',True)
        pdf.cell(70, 5, 'Vesrion', 1, 1, 'C',True)
        pdf.set_text_color(r = 1, g = 1, b = 1)
        pdf.set_fill_color(r = 244, g = 244, b = 244)
        pdf.set_font('Arial','', 11)
        for pos in range(0, len(new_ref_df)):
            pdf.cell(70, 5, "{}".format(new_ref_df['name'].iloc[pos]), 1, 0, 'C',True)
            pdf.cell(70, 5, "{}".format(new_ref_df['sub_version'].iloc[pos]), 1, 1, 'C',True)
        
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Data Section",0,ln=1)
        pdf.set_font('Arial','', 12)

        individual_node_size,total_storage=obj2.totalSizeConfigured()
        pdf.cell(230, 8,"Total Size Configured in the Cluster: {: .2f} GB ".format(total_storage),0,ln=1)
        #pdf.cell(230, 8,"Size Configured for individual nodes: {: .2f} GB ".format(total_storage),0,ln=1)
        # itr=0
        # while itr < individual_node_size:
            # pdf.cell(230, 8,"    Size Configured for node {} : {} GB ".format(itr+1,individual_node_size[itr]),0,ln=1)
            # itr=itr+1
        
        #Replication Factor
        replication_factor=obj2.replicationFactor()
        pdf.cell(230, 8,"Replication Factor                             : {} ".format(replication_factor),0,ln=1)    

        #check trash interval setup in the clsuter e.g Filesystem Trash Interval property to 1440
        trash_flag=obj2.getTrashStatus()
        pdf.cell(230, 8,"Trash Interval Setup in the Cluster  : {} ".format(trash_flag),0,ln=1)


        #HDFS Storage Available/Usage
        hdfs_capacity_df=obj2.getHdfsCapacity(clusterName)

        hdfs_capacity_used_df=obj2.getHdfsCapacityUsed(clusterName)

        hdfs_storage_config = hdfs_capacity_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']
        hdfs_storage_used = hdfs_capacity_used_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']

        pdf.cell(230, 3, "",0,ln=1)
        #pdf.cell(230, 5, "HDFS Storage Available : {: .0f} GB".format(hdfs_capacity_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        #pdf.cell(230, 5, "HDFS Storage Used : {: .0f} GB".format(hdfs_capacity_used_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        pdf.cell(230, 5, "HDFS Storage Available : {: .0f} GB".format(hdfs_capacity_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        pdf.cell(230, 5, "HDFS Storage Used       : {: .0f} GB".format(hdfs_capacity_used_df.sort_values(by='DateTime', ascending=False).iloc[0]['Mean']),0,ln=1)
        pdf.cell(230, 3, "",0,ln=1)

        plt.figure(19)
        pdf.cell(230, 3, "",0,ln=1)
        hdfs_usage_plot = hdfs_capacity_df['Mean'].plot(color="steelblue",label='Storage Available')
        hdfs_usage_plot = hdfs_capacity_used_df['Mean'].plot(color="darkorange",label='Storage Used')
        hdfs_usage_plot.legend()
        hdfs_usage_plot.set_ylabel('HDFS Capacity(GB)')
        plt.title("HDFS Usage")
        plt.savefig('hdfs_usage_plot.png')
        pdf.image('hdfs_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')

        
        #HDFS Size Breakdow
        pdf.set_font('Arial','B', 12)
        pdf.cell(230, 10, "HDFS Size Breakdown",0,ln=1)
        pdf.set_font('Arial','', 12)

        hdfs_root_dir=obj2.getCliresult("/")

        for i in hdfs_root_dir.splitlines():
            hdfs_dir = i.split()
            if(len(hdfs_dir) == 5):
                hdfs_dir[0] = hdfs_dir[0] + b' ' + hdfs_dir[1]
                hdfs_dir[1] = hdfs_dir[2] + b' ' + hdfs_dir[3]
                hdfs_dir[2] = hdfs_dir[4]
            pdf.cell(230, 5, "{} - (Size = {} , Disk Space = {})".format(str(hdfs_dir[2], 'utf-8'),str(hdfs_dir[0], 'utf-8'),str(hdfs_dir[1], 'utf-8')),0,ln=1)
            hdfs_inner_dir=obj2.getCliresult(hdfs_dir[2])
            for j in hdfs_inner_dir.splitlines():
                hdfs_inner_dir = j.split()
                if(len(hdfs_inner_dir) == 5):
                    hdfs_inner_dir[0] = hdfs_inner_dir[0] + b' ' + hdfs_inner_dir[1]
                    hdfs_inner_dir[1] = hdfs_inner_dir[2] + b' ' + hdfs_inner_dir[3]
                    hdfs_inner_dir[2] = hdfs_inner_dir[4]
                pdf.cell(230, 5, "    |-- {} - (Size = {} , Disk Space = {})".format(str(hdfs_inner_dir[2], 'utf-8'),str(hdfs_inner_dir[0], 'utf-8'),str(hdfs_inner_dir[1], 'utf-8')),0,ln=1)
            pdf.cell(230, 3, "",0,ln=1)
        
        yarn_application_df = obj_app.getApplicationDetails(yarn_rm)
        app_count_df, app_type_count_df, app_status_count_df = obj_app.getApplicationTypeStatusCount(yarn_application_df)
        app_vcore_df, app_memory_df = obj_app.getApplicationVcoreMemoryUsage(yarn_application_df)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Yarn Application Metrics",0,ln=1)
        pdf.cell(230, 10, "",0,ln=1)
        pdf.set_font('Arial','', 12)
        pdf.set_font('Arial','B', 11)
        pdf.set_fill_color(r = 66, g = 133, b = 244)
        pdf.set_text_color(r = 255, g = 255, b = 255)
        pdf.cell(40, 5, 'Application Type', 1, 0, 'C',True)
        pdf.cell(40, 5, 'Status', 1, 0, 'C',True)
        pdf.cell(30, 5, 'Count', 1, 1, 'C',True)
        pdf.set_text_color(r = 1, g = 1, b = 1)
        pdf.set_fill_color(r = 244, g = 244, b = 244)
        pdf.set_font('Arial','', 11)
        for pos in range(0, len(app_count_df)):
            pdf.cell(40, 5, "{}".format(app_count_df['Application Type'].iloc[pos]), 1, 0, 'C',True)
            pdf.cell(40, 5, "{}".format(app_count_df['Status'].iloc[pos]), 1, 0, 'C',True)
            pdf.cell(30, 5, "{}".format(app_count_df['Count'].iloc[pos]), 1, 1, 'C',True)
        pdf.set_font('Arial','', 12)
        pdf.cell(230, 15, "",0,ln=1)
        x = pdf.get_x()
        y = pdf.get_y()
        plt.figure()
        app_type_count_pie_plot = app_type_count_df.plot.pie(y='Count', figsize=(6, 6),autopct='%.1f%%',title="Yarn Application by Type")
        plt.savefig('app_type_count_pie_plot.png')
        pdf.image('app_type_count_pie_plot.png', x = 15, y = None, w = 95, h = 95, type = '', link = '')
        pdf.set_xy(x,y)
        plt.figure()
        app_status_count_pie_plot = app_status_count_df.plot.pie(y='Count', figsize=(6, 6),autopct='%.1f%%',title="Yarn Application by Status")
        plt.savefig('app_status_count_pie_plot.png')
        pdf.image('app_status_count_pie_plot.png', x = 130, y = None, w = 95, h = 95, type = '', link = '')
        pdf.cell(230, 30, "",0,ln=1)
        plt.figure()
        x = pdf.get_x()
        y = pdf.get_y()
        app_vcore_plot = app_vcore_df.plot.pie(y='Vcore', figsize=(6, 6),autopct='%.1f%%',title="Yarn Application Vcore Usage")
        plt.savefig('app_vcore_plot.png')
        pdf.image('app_vcore_plot.png', x = 15, y = None, w = 95, h = 95, type = '', link = '')
        pdf.set_xy(x,y)
        plt.figure()
        app_memory_plot = app_memory_df.plot.pie(y='Memory', figsize=(6, 6),autopct='%.1f%%',title="Yarn Application Memory Usage")
        plt.savefig('app_memory_plot.png')
        pdf.image('app_memory_plot.png', x = 130, y = None, w = 95, h = 95, type = '', link = '')
        app_vcore_df, app_vcore_usage_df, app_memory_df, app_memory_usage_df = obj_app.getVcoreMemoryByApplication(yarn_application_df)
        pdf.add_page()
        plt.figure()
        for i in app_vcore_df['Application Type'].unique():
            app_vcore_df_temp = pd.DataFrame(None)
            app_vcore_df_temp = app_vcore_df[app_vcore_df['Application Type']==i]
            app_vcore_usage_df[i] = 0
            for index, row in app_vcore_df_temp.iterrows():
                val = (row['Launch Time'],0)
                if(val not in app_vcore_usage_df['Date']):
                    app_vcore_usage_df.loc[len(app_vcore_usage_df)] = val
                val = (row['Finished Time'],0)
                if(val not in app_vcore_usage_df['Date']):
                    app_vcore_usage_df.loc[len(app_vcore_usage_df)] = val
                app_vcore_usage_df.loc[(app_vcore_usage_df['Date']>= row['Launch Time']) & (app_vcore_usage_df['Date']< row['Finished Time']),i] = app_vcore_usage_df.loc[(app_vcore_usage_df['Date']>= row['Launch Time']) & (app_vcore_usage_df['Date']< row['Finished Time'])][i] + row['Vcore']
            app_vcore_usage_plot = app_vcore_usage_df.set_index('Date')[i].plot(label=i)
            app_vcore_usage_df = app_vcore_usage_df.drop([i], axis = 1)
        app_vcore_usage_plot.legend()
        app_vcore_usage_plot.set_ylabel('Application Vcores')
        plt.title("Vcore Breakdown By Application Type")
        plt.savefig('app_vcore_usage_plot.png')
        pdf.image('app_vcore_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        for i in app_memory_df['Application Type'].unique():
            app_memory_df_temp = pd.DataFrame(None)
            app_memory_df_temp = app_memory_df[app_memory_df['Application Type']==i]
            app_memory_usage_df[i] = 0
            for index, row in app_memory_df_temp.iterrows():
                val = (row['Launch Time'],0)
                if(val not in app_memory_usage_df['Date']):
                    app_memory_usage_df.loc[len(app_memory_usage_df)] = val
                val = (row['Finished Time'],0)
                if(val not in app_memory_usage_df['Date']):
                    app_memory_usage_df.loc[len(app_memory_usage_df)] = val
                app_memory_usage_df.loc[(app_memory_usage_df['Date']>= row['Launch Time']) & (app_memory_usage_df['Date']< row['Finished Time']),i] = app_memory_usage_df.loc[(app_memory_usage_df['Date']>= row['Launch Time']) & (app_memory_usage_df['Date']< row['Finished Time'])][i] + row['Memory']
            app_memory_usage_plot = app_memory_usage_df.set_index('Date')[i].plot(label=i)
            app_memory_usage_df = app_memory_usage_df.drop([i], axis = 1)
        app_memory_usage_plot.legend()
        app_memory_usage_plot.set_ylabel('Application Memory')
        plt.title("Memory Breakdown By Application Type")
        plt.savefig('app_memory_usage_plot.png')
        pdf.image('app_memory_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        yarn_pending_apps_df, yarn_pending_vcore_df, yarn_pending_memory_df = obj_app.getPendingApplication(cluster_name), obj_app.getPendingVcore(cluster_name), obj_app.getPendingMemory(cluster_name)
        pdf.add_page()
        plt.figure()
        yarn_pending_apps_plot = yarn_pending_apps_df['Max'].plot(color="steelblue",label='Pending Applications')
        yarn_pending_apps_plot.legend()
        yarn_pending_apps_plot.set_ylabel('Application Count')
        plt.title("Total Pending Applications Across YARN Pools")
        plt.savefig('yarn_pending_apps_plot.png')
        pdf.image('yarn_pending_apps_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_pending_vcore_plot = yarn_pending_vcore_df['Mean'].plot(color="steelblue",label='Pending Vcores')
        yarn_pending_vcore_plot.legend()
        yarn_pending_vcore_plot.set_ylabel('Vcores')
        plt.title("Total Pending VCores Across YARN Pools")
        plt.savefig('yarn_pending_vcore_plot.png')
        pdf.image('yarn_pending_vcore_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_pending_memory_plot = yarn_pending_memory_df['Mean'].plot(color="steelblue",label='Pending Memory')
        yarn_pending_memory_plot.legend()
        yarn_pending_memory_plot.set_ylabel('Memory (MB)')
        plt.title("Total Pending Memory Across YARN Pools")
        plt.savefig('yarn_pending_memory_plot.png')
        pdf.image('yarn_pending_memory_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        bursty_app_time_df, bursty_app_vcore_df, bursty_app_mem_df = obj_app.getBurstyApplicationDetails(yarn_application_df)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Bursty Applications",0,ln=1)
        if(bursty_app_time_df.size != 0):
            pdf.set_font('Arial', '', 12)
            pdf.cell(230, 5, "Bursty Applications - Elapsed Time",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(110, 5, 'Application Name', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Min Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Mean Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Max Time', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(bursty_app_time_df)):
                pdf.cell(110, 5, "{}".format(bursty_app_time_df['Application Name'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_time_df['Min'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_time_df['Mean'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_time_df['Max'].iloc[pos]), 1, 1, 'C',True)
            pdf.cell(230, 5, "",0,ln=1)
            plt.figure()
            bursty_app_time_df = bursty_app_time_df.set_index('Application Name')
            bursty_app_time_plot = bursty_app_time_df.plot.barh( stacked=True).legend(loc='upper center', ncol=3)
            plt.title("Bursty Applications - Elapsed Time")
            plt.xlabel("Time(secs)")
            plt.ylabel("Applications")
            plt.tight_layout()
            plt.savefig('bursty_app_time_plot.png')
            pdf.image('bursty_app_time_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
            pdf.cell(230, 5, "",0,ln=1)
            pdf.set_font('Arial', '', 12)
            pdf.cell(230, 5, "Bursty Applications - Vcore Seconds",0,ln=1)   
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(110, 5, 'Application Name', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Min Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Mean Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Max Time', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(bursty_app_vcore_df)):
                pdf.cell(110, 5, "{}".format(bursty_app_vcore_df['Application Name'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_vcore_df['Min'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_vcore_df['Mean'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_vcore_df['Max'].iloc[pos]), 1, 1, 'C',True)
            pdf.cell(230, 5, "",0,ln=1)
            plt.figure()
            bursty_app_vcore_df = bursty_app_vcore_df.set_index('Application Name')
            bursty_app_vcore_plot = bursty_app_vcore_df.plot.barh( stacked=True).legend(loc='upper center', ncol=3)
            plt.title("Bursty Applications - Vcore Seconds")
            plt.xlabel("Time(secs)")
            plt.ylabel("Applications")
            plt.tight_layout()
            plt.savefig('bursty_app_vcore_plot.png')
            pdf.image('bursty_app_vcore_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
            pdf.cell(230, 5, "",0,ln=1)
            pdf.set_font('Arial', '', 12)
            pdf.cell(230, 5, "Bursty Applications - Memory Seconds",0,ln=1)   
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(110, 5, 'Application Name', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Min Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Mean Time', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Max Time', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(bursty_app_mem_df)):
                pdf.cell(110, 5, "{}".format(bursty_app_mem_df['Application Name'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_mem_df['Min'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_mem_df['Mean'].iloc[pos]), 1, 0, 'C',True)
                pdf.cell(30, 5, "{: .0f} sec".format(bursty_app_mem_df['Max'].iloc[pos]), 1, 1, 'C',True)
            pdf.cell(230, 5, "",0,ln=1)
            plt.figure()
            bursty_app_mem_df = bursty_app_mem_df.set_index('Application Name')
            bursty_app_mem_plot = bursty_app_mem_df.plot.barh( stacked=True).legend(loc='upper center', ncol=3)
            plt.title("Bursty Applications - Memory Seconds")
            plt.xlabel("Memory Seconds")
            plt.ylabel("Applications")
            plt.tight_layout()
            plt.savefig('bursty_app_mem_plot.png')
            pdf.image('bursty_app_mem_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        yarn_failed_app = obj_app.getFailedApplicationDetails(yarn_application_df)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Failed Applications",0,ln=1)
        pdf.set_font('Arial', '', 12)
        pdf.cell(230, 5, "Run time of Failed/Killed Applications = {: .2f} seconds".format(yarn_failed_app['ElapsedTime'].sum()),0,ln=1)
        pdf.cell(230, 5, "Vcores Seconds Used by Failed/Killed Applications = {} seconds".format(yarn_failed_app['MemorySeconds'].sum()),0,ln=1)
        pdf.cell(230, 5, "Memory Seconds Used Failed/Killed Applications = {} seconds".format(yarn_failed_app['VcoreSeconds'].sum()),0,ln=1)
        if(yarn_failed_app.size != 0):
            yarn_failed_app = yarn_failed_app.head(10)
            pdf.set_font('Arial','', 11)
            pdf.cell(230, 3, "",0,ln=1)
            pdf.cell(230, 8, "Top long running failed application diagnostics : ",0,ln=1)
            pdf.set_font('Arial','B', 11)
            pdf.set_fill_color(r = 66, g = 133, b = 244)
            pdf.set_text_color(r = 255, g = 255, b = 255)
            pdf.cell(40, 5, 'App Id', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Final Status', 1, 0, 'C',True)
            pdf.cell(30, 5, 'Elapsed Time', 1, 0, 'C',True)
            pdf.cell(130, 5, 'Diagnostics', 1, 1, 'C',True)
            pdf.set_text_color(r = 1, g = 1, b = 1)
            pdf.set_fill_color(r = 244, g = 244, b = 244)
            pdf.set_font('Arial','', 11)
            for pos in range(0, len(yarn_failed_app)):
                x = pdf.get_x()
                y = pdf.get_y()
                if(y > 300):
                    pdf.add_page()
                    x = pdf.get_x()
                    y = pdf.get_y()
                diag = yarn_failed_app['Diagnostics'].iloc[pos][:300]
                line_width = 0
                line_width = max(line_width,pdf.get_string_width(yarn_failed_app['ApplicationId'].iloc[pos]))
                cell_y = line_width/39.0
                line_width = max(line_width,pdf.get_string_width(yarn_failed_app['FinalStatus'].iloc[pos]))
                cell_y = max(cell_y,line_width/29.0)
                line_width = max(line_width,pdf.get_string_width(str(yarn_failed_app['ElapsedTime'].iloc[pos])))
                cell_y = max(cell_y,line_width/29.0)
                line_width = max(line_width,pdf.get_string_width(diag))
                cell_y = max(cell_y,line_width/129.0)
                cell_y = math.ceil(cell_y)
                cell_y = max(cell_y,1)
                cell_y = cell_y*5
                line_width = pdf.get_string_width(yarn_failed_app['ApplicationId'].iloc[pos])
                y_pos = line_width/39.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(40, y_pos, "{}".format(yarn_failed_app['ApplicationId'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+40,y)
                line_width = pdf.get_string_width(yarn_failed_app['FinalStatus'].iloc[pos])
                y_pos = line_width/29.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(30, y_pos, "{}".format(yarn_failed_app['FinalStatus'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+70,y)
                line_width = pdf.get_string_width(str(yarn_failed_app['ElapsedTime'].iloc[pos]))
                y_pos = line_width/29.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(30, y_pos, "{}".format(yarn_failed_app['ElapsedTime'].iloc[pos]), 1, 'C',fill = True)
                pdf.set_xy(x+100,y)
                line_width = pdf.get_string_width(diag)
                y_pos = line_width/129.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos,1)
                y_pos = cell_y/y_pos
                pdf.multi_cell(130, y_pos, "{}".format(diag), 1, 'C',fill = True)
        yarn_total_vcores_count, yarn_vcore_available_df, [yarn_vcore_allocated_avg, yarn_vcore_allocated_df, yarn_vcore_allocated_pivot_df] = obj_app.getYarnTotalVcore(yarn_rm), obj_app.getYarnVcoreAvailable(cluster_name), obj_app.getYarnVcoreAllocated(cluster_name)
        yarn_total_memory_count, yarn_memory_available_df, [yarn_memory_allocated_avg, yarn_memory_allocated_df, yarn_memory_allocated_pivot_df] = obj_app.getYarnTotalMemory(yarn_rm), obj_app.getYarnMemoryAvailable(cluster_name), obj_app.getYarnMemoryAllocated(cluster_name)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Yarn Metrics",0,ln=1)
        pdf.set_font('Arial','', 12)
        pdf.cell(230, 3, "",0,ln=1)
        pdf.cell(230, 5, "Total Yarn Vcore                    : {:.0f}".format(yarn_total_vcores_count),0,ln=1)
        pdf.cell(230, 5, "Total Yarn Memory                : {:.0f} GB".format(yarn_total_memory_count),0,ln=1)
        pdf.cell(230, 3, "",0,ln=1)
        pdf.cell(230, 5, "Average No. of Vcores Used : {: .2f}".format(yarn_vcore_allocated_avg),0,ln=1)
        pdf.cell(230, 5, "Average Yarn Memory Used : {:.0f} MB".format(yarn_memory_allocated_avg),0,ln=1)
        plt.figure()
        yarn_vcore_usage_plot = yarn_vcore_available_df['Mean'].plot(color="steelblue",label='Vcores Available')
        yarn_vcore_usage_plot = yarn_vcore_allocated_df['Mean'].plot(color="darkorange",label='Vcores Allocated (Mean)')
        yarn_vcore_usage_plot = yarn_vcore_allocated_df['Max'].plot(color="red",label='Vcores Allocated (Max)',linestyle="--",linewidth=1)
        yarn_vcore_usage_plot.legend()
        yarn_vcore_usage_plot.set_ylabel('Total Vcore Usage')
        plt.title("Yarn Vcore Usage")
        plt.savefig('yarn_vcore_usage_plot.png')
        pdf.image('yarn_vcore_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_memory_usage_plot = yarn_memory_available_df['Mean'].plot(color="steelblue",label='Memory Available')
        yarn_memory_usage_plot = yarn_memory_allocated_df['Mean'].plot(color="darkorange",label='Memory Allocated (Mean)')
        yarn_memory_usage_plot = yarn_memory_allocated_df['Max'].plot(color="red",label='Memory Allocated (Max)',linestyle="--",linewidth=1)
        yarn_memory_usage_plot.legend()
        yarn_memory_usage_plot.set_ylabel('Total Yarn Memory(MB)')
        plt.title("Yarn Memory Usage")
        plt.savefig('yarn_memory_usage_plot.png')
        pdf.image('yarn_memory_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_vcore_usage_heatmap = sns.heatmap(yarn_vcore_allocated_pivot_df,cmap="OrRd")
        plt.title('Yarn Vcore Usage')
        plt.savefig('yarn_vcore_usage_heatmap.png')
        pdf.image('yarn_vcore_usage_heatmap.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        yarn_memory_usage_heatmap = sns.heatmap(yarn_memory_allocated_pivot_df,cmap="OrRd")
        plt.title('Yarn Memory Usage')
        plt.savefig('yarn_memory_usage_heatmap.png')
        pdf.image('yarn_memory_usage_heatmap.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        yarn_queues_list, [queue_app_count_df, queue_elapsed_time_df], [app_queue_df, app_queue_usage_df], [queue_vcore_df, queue_vcore_usage_df, queue_memory_df, queue_memory_usage_df] = obj_app.getQueueDetails(yarn_rm), obj_app.getQueueApplication(yarn_application_df), obj_app.getQueuePendingApplication(yarn_application_df), obj_app.getQueueVcoreMemory(yarn_application_df)
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(230, 10, "Yarn Queues",0,ln=1)
        pdf.cell(230, 5, "",0,ln=1)
        pdf.set_font('Arial','', 12)
        def yarn_queue(yarn_queues_list,count):
            for queue in yarn_queues_list:
                if('queues' in queue):
                    pdf.cell(10*count, 5, "", 0, 0)
                    pdf.cell(30, 5, "|-- {} - (Absolute Capacity - {}, Max Capacity - {})".format(queue['queueName'],queue['absoluteCapacity'],queue['absoluteMaxCapacity']), 0, ln=1)
                    yarn_queue(queue['queues']['queue'],count+1)
                else:
                    pdf.cell(10*count, 5, "", 0, 0)
                    pdf.cell(30, 5, "|-- {} - (Absolute Capacity - {}, Max Capacity - {})".format(queue['queueName'],queue['absoluteCapacity'],queue['absoluteMaxCapacity']), 0, ln=1)
        pdf.cell(230, 5, "Queue Structure : ",0,ln=1)
        pdf.cell(230, 5, "Root - (Absolute Capacity - 100, Max Capacity - 100)",0,ln=1)
        yarn_queue(yarn_queues_list,1)
        pdf.cell(230, 10, "",0,ln=1)
        def make_autopct(values):
            def my_autopct(pct):
                total = sum(values)
                val = int(round(pct*total/100.0))
                return '{p:.2f}%  ({v:d})'.format(p=pct,v=val)
            return my_autopct
        plt.figure()
        x = pdf.get_x()
        y = pdf.get_y()
        queue_app_count_plot = queue_app_count_df.plot.pie(y='Application Count', figsize=(6, 6),autopct=make_autopct(queue_app_count_df['Application Count']),title="Queue Application Count (Weekly)")
        plt.savefig('queue_app_count_plot.png')
        pdf.image('queue_app_count_plot.png', x = 15, y = None, w = 95, h = 95, type = '', link = '')
        pdf.set_xy(x,y)
        plt.figure()
        queue_elapsed_time_plot = queue_elapsed_time_df.plot.pie(y='Elapsed Time', figsize=(6, 6),autopct='%.1f%%',title="Queue Elapsed Time (Weekly)")
        plt.savefig('queue_elapsed_time_plot.png')
        pdf.image('queue_elapsed_time_plot.png', x = 130, y = None, w = 95, h = 95, type = '', link = '')
        plt.figure()
        for i in app_queue_df['Queue'].unique():
            app_queue_df_temp = pd.DataFrame(None)
            app_queue_df_temp = app_queue_df[(app_queue_df['Queue']==i) & (app_queue_df['Wait Time'] > timedelta(minutes=5))]
            app_queue_usage_df[i] = 0
            for index, row in app_queue_df_temp.iterrows():
                val = (row['Start Time'],0)
                if(val not in app_queue_usage_df['Date']):
                    app_queue_usage_df.loc[len(app_queue_usage_df)] = val
                val = (row['Launch Time'],0)
                if(val not in app_queue_usage_df['Date']):
                    app_queue_usage_df.loc[len(app_queue_usage_df)] = val
                app_queue_usage_df.loc[(app_queue_usage_df['Date']>= row['Start Time']) & (app_queue_usage_df['Date']< row['Launch Time']),i] = app_queue_usage_df.loc[(app_queue_usage_df['Date']>= row['Start Time']) & (app_queue_usage_df['Date']< row['Launch Time'])][i] + 1
            app_queue_usage_plot = app_queue_usage_df.set_index('Date')[i].plot(label=i)
            app_queue_usage_df = app_queue_usage_df.drop([i], axis = 1)
        app_queue_usage_plot.legend()
        app_queue_usage_plot.set_ylabel('Application Count')
        plt.title("Application Pending by Queue")
        plt.savefig('app_queue_usage_plot.png')
        pdf.image('app_queue_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        pdf.cell(230, 10, "",0,ln=1)
        plt.figure()
        for i in queue_vcore_df['Queue'].unique():
            queue_vcore_df_temp = pd.DataFrame(None)
            queue_vcore_df_temp = queue_vcore_df[queue_vcore_df['Queue']==i]
            queue_vcore_usage_df[i] = 0
            for index, row in queue_vcore_df_temp.iterrows():
                val = (row['Launch Time'],0)
                if(val not in queue_vcore_usage_df['Date']):
                    queue_vcore_usage_df.loc[len(queue_vcore_usage_df)] = val
                val = (row['Finished Time'],0)
                if(val not in queue_vcore_usage_df['Date']):
                    queue_vcore_usage_df.loc[len(queue_vcore_usage_df)] = val
                queue_vcore_usage_df.loc[(queue_vcore_usage_df['Date']>= row['Launch Time']) & (queue_vcore_usage_df['Date']< row['Finished Time']),i] = queue_vcore_usage_df.loc[(queue_vcore_usage_df['Date']>= row['Launch Time']) & (queue_vcore_usage_df['Date']< row['Finished Time'])][i] + row['Vcore']
            queue_vcore_usage_plot = queue_vcore_usage_df.set_index('Date')[i].plot(label=i)
            queue_vcore_usage_df = queue_vcore_usage_df.drop([i], axis = 1)
        queue_vcore_usage_plot.legend()
        queue_vcore_usage_plot.set_ylabel('Application Vcores')
        plt.title("Vcore Breakdown By Queue")
        plt.savefig('queue_vcore_usage_plot.png')
        pdf.image('queue_vcore_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        plt.figure()
        for i in queue_memory_df['Queue'].unique():
            queue_memory_df_temp = pd.DataFrame(None)
            queue_memory_df_temp = queue_memory_df[queue_memory_df['Queue']==i]
            queue_memory_usage_df[i] = 0
            for index, row in queue_memory_df_temp.iterrows():
                val = (row['Launch Time'],0)
                if(val not in queue_memory_usage_df['Date']):
                    queue_memory_usage_df.loc[len(queue_memory_usage_df)] = val
                val = (row['Finished Time'],0)
                if(val not in queue_memory_usage_df['Date']):
                    queue_memory_usage_df.loc[len(queue_memory_usage_df)] = val
                queue_memory_usage_df.loc[(queue_memory_usage_df['Date']>= row['Launch Time']) & (queue_memory_usage_df['Date']< row['Finished Time']),i] = queue_memory_usage_df.loc[(queue_memory_usage_df['Date']>= row['Launch Time']) & (queue_memory_usage_df['Date']< row['Finished Time'])][i] + row['Memory']
            queue_memory_usage_plot = queue_memory_usage_df.set_index('Date')[i].plot(label=i)
            queue_memory_usage_df = queue_memory_usage_df.drop([i], axis = 1)
        queue_memory_usage_plot.legend()
        queue_memory_usage_plot.set_ylabel('Application Memory')
        plt.title("Memory Breakdown By Queue")
        plt.savefig('queue_memory_usage_plot.png')
        pdf.image('queue_memory_usage_plot.png', x = 0, y = None, w = 250, h = 85, type = '', link = '')
        pdf.output('Discovery_Report/{}.pdf'.format(cluster_name), 'F')
