# ------------------------------------------------------------------------------
# pdfgeneraor module contains the logic which will generate the pdf report with
# the metrics related to the Hardware and Operating system, Framework and
# software details, Security, Application, Data, Network and traffic.
#
# pdfgenerator will take inputs from the other modules as an argument and based
# on the input it will generate tabular and labelled reports.
# ------------------------------------------------------------------------------

# Importing required libraries
from imports import *
from HardwareOSAPI import *
from FrameworkDetailsAPI import *
from DataAPI import *
from SecurityAPI import *
from ApplicationAPI import *
from PdfFunctions import *


class PdfGenerator:
    """This Class has functions for PDF generation based on different 
    Cloudera versions.

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
        self.hive_username = inputs["hive_username"]
        self.hive_password = inputs["hive_password"]
        self.start_date = inputs["start_date"]
        self.end_date = inputs["end_date"]

    def run(self):
        """Generate PDF for CDH-5, CDH-6 and CDP-7"""

        pdf = FPDF(format=(250, 350))
        p_bar = tqdm(total=6, desc="Hadoop Discovery Tool")
        obj1 = HardwareOSAPI(self.inputs)
        obj2 = DataAPI(self.inputs)
        obj3 = FrameworkDetailsAPI(self.inputs)
        obj4 = SecurityAPI(self.inputs)
        obj_app = ApplicationAPI(self.inputs)
        obj_pdf = PdfFunctions(self.inputs, pdf)
        yarn_rm = ""
        yarn_port = ""
        cluster_name = self.cluster_name
        if os.path.exists("Discovery_Report"):
            shutil.rmtree("Discovery_Report")
        os.makedirs("Discovery_Report")
        if os.path.exists("Discovery_Report/{}".format(cluster_name)):
            shutil.rmtree("Discovery_Report/{}".format(cluster_name))
        os.makedirs("Discovery_Report/{}".format(cluster_name))

        pdf.add_page()
        pdf.set_font("Arial", "B", 26)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Hadoop Discovery Report", 0, ln=1, align="C")
        pdf.set_font("Arial", "B", 12)
        pdf.set_fill_color(r=66, g=133, b=244)
        pdf.set_text_color(r=255, g=255, b=255)
        pdf.cell(0, 10, "", 0, 1)
        pdf.cell(60, 16, "Report Date Range", 1, 0)
        pdf.cell(40, 8, "Start Date", 1, 0)
        pdf.cell(40, 8, "End Date", 1, 0)
        pdf.cell(0, 8, "", 0, 1)
        pdf.cell(60, 8, "", 0, 0)
        pdf.set_text_color(r=1, g=1, b=1)
        pdf.set_fill_color(r=244, g=244, b=244)
        pdf.set_font("Arial", "", 12)
        pdf.cell(
            40,
            8,
            datetime.strptime(self.start_date, "%Y-%m-%dT%H:%M:%S").strftime(
                "%d-%b-%Y"
            ),
            1,
            0,
        )
        pdf.cell(
            40,
            8,
            datetime.strptime(self.end_date, "%Y-%m-%dT%H:%M:%S").strftime("%d-%b-%Y"),
            1,
            1,
        )
        pdf.cell(0, 8, "", 0, 1)
        pdf.cell(0, 8, "", 0, 1)

        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Key Metrics", 0, ln=1)

        cluster_host_items, clusterHostLen = None, None
        all_host_data = None
        temp = obj1.clusterHostItems(cluster_name)
        if type(temp) != type(None):
            cluster_host_items, clusterHostLen = temp
            all_host_data = []
            for i in cluster_host_items:
                host_data = obj1.hostData(i["hostId"])
                if host_data != None:
                    all_host_data.append(host_data)

        cluster_cpu_usage_df, cluster_cpu_usage_avg = None, None
        temp2 = obj1.clusterCpuUsage(cluster_name)
        if type(temp2) != type(None):
            cluster_cpu_usage_df, cluster_cpu_usage_avg = temp2

        cluster_memory_usage_df, cluster_memory_usage_avg = None, None
        temp2 = obj1.clusterMemoryUsage(cluster_name)
        if type(temp2) != type(None):
            cluster_memory_usage_df, cluster_memory_usage_avg = temp2

        hadoopVersionMajor, hadoopVersionMinor, distribution = None, None, None
        temp = obj3.hadoopVersion()
        if type(temp) != type(None):
            hadoopVersionMajor, hadoopVersionMinor, distribution = temp

        mapped_df, total_storage = None, None
        temp = obj2.totalSizeConfigured()
        if type(temp) != type(None):
            mapped_df, total_storage = temp

        hdfs_capacity_df, hdfs_storage_config = None, None
        hdfs_capacity_used_df, hdfs_storage_used = None, None
        temp1 = obj2.getHdfsCapacity(cluster_name)
        temp2 = obj2.getHdfsCapacityUsed(cluster_name)
        if (type(temp1) != type(None)) and (type(temp2) != type(None)):
            hdfs_capacity_df, hdfs_storage_config = temp1
            hdfs_capacity_used_df, hdfs_storage_used = temp2

        (
            yarn_vcore_allocated_avg,
            yarn_vcore_allocated_df,
            yarn_vcore_allocated_pivot_df,
        ) = (None, None, None)
        temp2 = obj_app.getYarnVcoreAllocated(cluster_name)
        if type(temp2) != type(None):
            (
                yarn_vcore_allocated_avg,
                yarn_vcore_allocated_df,
                yarn_vcore_allocated_pivot_df,
            ) = temp2

        (
            yarn_memory_allocated_avg,
            yarn_memory_allocated_df,
            yarn_memory_allocated_pivot_df,
        ) = (None, None, None)
        temp1 = obj_app.getYarnMemoryAvailable(cluster_name)
        temp2 = obj_app.getYarnMemoryAllocated(cluster_name)
        if (type(temp1) != type(None)) and (type(temp2) != type(None)):
            (
                yarn_memory_allocated_avg,
                yarn_memory_allocated_df,
                yarn_memory_allocated_pivot_df,
            ) = temp2

        obj_pdf.summaryTable(
            all_host_data,
            cluster_cpu_usage_avg,
            cluster_memory_usage_avg,
            hadoopVersionMajor,
            hadoopVersionMinor,
            distribution,
            total_storage,
            hdfs_storage_config,
            hdfs_storage_used,
            yarn_vcore_allocated_avg,
            yarn_memory_allocated_avg,
        )

        p_bar.update(1)
        p_bar.set_description(desc="Key Metrics Added in PDF")

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Cluster Information", 0, ln=1)

        cluster_items = None
        temp = obj1.clusterItems()
        if type(temp) != type(None):
            cluster_items = temp
            obj_pdf.clusterInfo(cluster_items)

        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(
            230, 10, "Listing Details for Cluster: {}".format(cluster_name), 0, ln=1
        )

        cluster_host_items, clusterHostLen = None, None
        all_host_data = None
        os_version = obj1.osVersion()
        temp = obj1.clusterHostItems(cluster_name)
        if type(temp) != type(None):
            cluster_host_items, clusterHostLen = temp
            all_host_data = []
            for i in cluster_host_items:
                host_data = obj1.hostData(i["hostId"])
                if host_data != None:
                    all_host_data.append(host_data)
            if (len(all_host_data) != 0) and (os_version != None):
                obj_pdf.clusterHostInfo(cluster_host_items, all_host_data, os_version)

        if all_host_data != None:
            for host in all_host_data:
                for role in host["roleRefs"]:
                    if "RESOURCEMANAGER" in role["roleName"].upper():
                        yarn_rm = host["ipAddress"]
        yarn_port = "8088"

        cluster_service_item = None
        temp = obj1.clusterServiceItem(cluster_name)
        if type(temp) != type(None):
            cluster_service_item = temp
            obj_pdf.clusterServiceInfo(cluster_service_item)

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Cluster Metrics", 0, ln=1)

        cluster_total_cores_df, cluster_cpu_usage_df, cluster_cpu_usage_avg = (
            None,
            None,
            None,
        )
        temp1, temp2 = (
            obj1.clusterTotalCores(cluster_name),
            obj1.clusterCpuUsage(cluster_name),
        )
        if (type(temp1) != type(None)) and (type(temp2) != type(None)):
            cluster_total_cores_df = temp1
            cluster_cpu_usage_df, cluster_cpu_usage_avg = temp2
            obj_pdf.clusterVcoreAvg(cluster_cpu_usage_avg)
            obj_pdf.clusterVcorePlot(cluster_total_cores_df, cluster_cpu_usage_df)

        cluster_total_memory_df, cluster_memory_usage_df, cluster_memory_usage_avg = (
            None,
            None,
            None,
        )
        temp1, temp2 = (
            obj1.clusterTotalMemory(cluster_name),
            obj1.clusterMemoryUsage(cluster_name),
        )
        if (type(temp1) != type(None)) and (type(temp2) != type(None)):
            cluster_total_memory_df = temp1
            cluster_memory_usage_df, cluster_memory_usage_avg = temp2
            obj_pdf.clusterMemoryAvg(cluster_memory_usage_avg)
            obj_pdf.clusterMemoryPlot(cluster_total_memory_df, cluster_memory_usage_df)

        p_bar.update(1)
        p_bar.set_description(desc="Hardware and OS footprint Metrics Added in PDF")

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Frameworks and Software Details", 0, ln=1)

        hadoopVersionMajor, hadoopVersionMinor, distribution = None, None, None
        temp = obj3.hadoopVersion()
        if type(temp) != type(None):
            hadoopVersionMajor, hadoopVersionMinor, distribution = temp
            obj_pdf.hadoopVersion(hadoopVersionMajor, hadoopVersionMinor, distribution)

        list_services_installed_df, new_ref_df = None, None
        temp = obj3.versionMapping(cluster_name)
        if type(temp) != type(None):
            list_services_installed_df, new_ref_df = temp
            obj_pdf.serviceInstalled(new_ref_df)

        p_bar.update(1)
        p_bar.set_description(desc="Framework and software details Added in PDF")

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Data Section", 0, ln=1)

        mapped_df, total_storage = None, None
        temp = obj2.totalSizeConfigured()
        if type(temp) != type(None):
            mapped_df, total_storage = temp
            obj_pdf.totalHDFSSize(total_storage)
            obj_pdf.individualHDFSSize(mapped_df)

        replication_factor = None
        temp = obj2.replicationFactor()
        if type(temp) != type(None):
            replication_factor = temp
            obj_pdf.repFactor(replication_factor)

        trash_flag = None
        temp = obj2.getTrashStatus()
        if type(temp) != type(None):
            trash_flag = temp
            obj_pdf.trashInterval(trash_flag)

        hdfs_capacity_df, hdfs_storage_config = None, None
        hdfs_capacity_used_df, hdfs_storage_used = None, None
        temp1 = obj2.getHdfsCapacity(cluster_name)
        temp2 = obj2.getHdfsCapacityUsed(cluster_name)
        if (type(temp1) != type(None)) and (type(temp2) != type(None)):
            hdfs_capacity_df, hdfs_storage_config = temp1
            hdfs_capacity_used_df, hdfs_storage_used = temp2
            obj_pdf.availableHDFSStorage(hdfs_storage_config)
            obj_pdf.usedHDFSStorage(hdfs_storage_used)
            obj_pdf.HDFSStoragePlot(hdfs_capacity_df, hdfs_capacity_used_df)

        pdf.add_page()

        hdfs_root_dir = None
        temp = obj2.getCliresult("/")
        if type(temp) != type(None):
            hdfs_root_dir = temp
            pdf.set_font("Arial", "", 12)
            pdf.set_text_color(r=1, g=1, b=1)
            pdf.cell(230, 10, "HDFS Size Breakdown", 0, ln=1)
            for i in hdfs_root_dir.splitlines():
                hdfs_dir = i.split()
                if len(hdfs_dir) == 5:
                    hdfs_dir[0] = hdfs_dir[0] + b" " + hdfs_dir[1]
                    hdfs_dir[1] = hdfs_dir[2] + b" " + hdfs_dir[3]
                    hdfs_dir[2] = hdfs_dir[4]
                pdf.cell(
                    230,
                    5,
                    "{} - (Size = {} , Disk Space = {})".format(
                        str(hdfs_dir[2], "utf-8"),
                        str(hdfs_dir[0], "utf-8"),
                        str(hdfs_dir[1], "utf-8"),
                    ),
                    0,
                    ln=1,
                )
                hdfs_inner_dir = obj2.getCliresult(hdfs_dir[2])
                for j in hdfs_inner_dir.splitlines():
                    hdfs_inner_dir = j.split()
                    if len(hdfs_inner_dir) == 5:
                        hdfs_inner_dir[0] = hdfs_inner_dir[0] + b" " + hdfs_inner_dir[1]
                        hdfs_inner_dir[1] = hdfs_inner_dir[2] + b" " + hdfs_inner_dir[3]
                        hdfs_inner_dir[2] = hdfs_inner_dir[4]
                    pdf.cell(
                        230,
                        5,
                        "    |-- {} - (Size = {} , Disk Space = {})".format(
                            str(hdfs_inner_dir[2], "utf-8"),
                            str(hdfs_inner_dir[0], "utf-8"),
                            str(hdfs_inner_dir[1], "utf-8"),
                        ),
                        0,
                        ln=1,
                    )
                pdf.cell(230, 3, "", 0, ln=1)

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Hive Metrics", 0, ln=1)

        mt_db_host, mt_db_name, mt_db_type, mt_db_port = None, None, None, None
        temp = obj2.getHiveConfigItems(cluster_name)
        if type(temp) != type(None):
            mt_db_host, mt_db_name, mt_db_type, mt_db_port = temp
            obj_pdf.hiveMetaStoreDetails(mt_db_host, mt_db_name, mt_db_type, mt_db_port)

            if mt_db_type == "postgresql":
                database_uri = "postgres+psycopg2://{}:{}@{}:{}/{}".format(
                    self.hive_username,
                    self.hive_password,
                    mt_db_host,
                    mt_db_port,
                    mt_db_name,
                )
            if mt_db_type == "mysql":
                database_uri = "mysql+pymysql://{}:{}@{}:{}/{}".format(
                    self.hive_username,
                    self.hive_password,
                    mt_db_host,
                    mt_db_port,
                    mt_db_name,
                )

            (
                database_count,
                [tables_with_partition, tables_without_partition],
                [internal_tables, external_tables],
                hive_execution_engine,
            ) = (None, [None, None], [None, None], None)
            t1 = obj2.getHiveDatabaseCount(database_uri, mt_db_type)
            t2 = obj2.getHivePartitionedTableCount(database_uri, mt_db_type)
            t3 = obj2.getHiveInternalExternalTables(database_uri, mt_db_type)
            t4 = obj2.getHiveExecutionEngine()
            if (
                (type(t1) != type(None))
                and (type(t2) != type(None))
                and (type(t3) != type(None))
                and (type(t4) != type(None))
            ):
                (
                    database_count,
                    [tables_with_partition, tables_without_partition],
                    [internal_tables, external_tables],
                    hive_execution_engine,
                ) = (t1, t2, t3, t4)
                obj_pdf.hiveDetails(
                    database_count,
                    tables_with_partition,
                    tables_without_partition,
                    internal_tables,
                    external_tables,
                    hive_execution_engine,
                )

            database_df = None
            temp1 = obj2.getHiveDatabaseInfo(database_uri, mt_db_type)
            if type(temp1) != type(None):
                database_df = temp1
                obj_pdf.hiveDatabasesSize(database_df)

            table_df = None
            temp1 = obj2.gethiveMetaStore(database_uri, mt_db_type)
            if type(temp1) != type(None):
                table_df = temp1
                obj_pdf.hiveAccessFrequency(table_df)

        p_bar.update(1)
        p_bar.set_description(desc="Data Metrics Added in PDF")

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Security", 0, ln=1)

        kerberos = None
        temp = obj4.clusterKerberosInfo(cluster_name)
        if type(temp) != type(None):
            kerberos = temp
            obj_pdf.kerberosInfo(kerberos)

        ADServer = None
        temp = obj4.ADServerNameAndPort(cluster_name)
        if type(temp) != type(None):
            ADServer = temp
            obj_pdf.ADServerNameAndPort(ADServer)

        Server_dn = None
        temp = obj4.adServerBasedDN(cluster_name)
        if type(temp) != type(None):
            Server_dn = temp
            obj_pdf.adServerBasedDN(Server_dn)

        keytab_files = None
        temp = obj4.keytabFilesInfo()
        if type(temp) != type(None):
            keytab_files = temp
            obj_pdf.keytabFiles(keytab_files)

        p_bar.update(1)
        p_bar.set_description(desc="Security Metrics Added in PDF")

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Yarn Metrics", 0, ln=1)

        pdf.set_font("Arial", "", 12)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "VCore Details:", 0, ln=1)

        yarn_total_vcores_count = None
        temp = obj_app.getYarnTotalVcore(yarn_rm, yarn_port)
        if type(temp) != type(None):
            yarn_total_vcores_count = temp
            obj_pdf.yarnVcoreTotal(yarn_total_vcores_count)

        (
            yarn_vcore_available_df,
            yarn_vcore_allocated_avg,
            yarn_vcore_allocated_df,
            yarn_vcore_allocated_pivot_df,
        ) = (None, None, None, None)
        temp1 = obj_app.getYarnVcoreAvailable(cluster_name)
        temp2 = obj_app.getYarnVcoreAllocated(cluster_name)
        if (type(temp1) != type(None)) and (type(temp2) != type(None)):
            (
                yarn_vcore_available_df,
                [
                    yarn_vcore_allocated_avg,
                    yarn_vcore_allocated_df,
                    yarn_vcore_allocated_pivot_df,
                ],
            ) = (temp1, temp2)
            obj_pdf.yarnVcoreAvg(yarn_vcore_allocated_avg)
            obj_pdf.yarnVcoreUsage(yarn_vcore_available_df, yarn_vcore_allocated_df)
            obj_pdf.yarnVcoreSeasonality(yarn_vcore_allocated_pivot_df)

        pdf.add_page()
        pdf.set_font("Arial", "", 12)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Memory Details:", 0, ln=1)

        yarn_total_memory_count = None
        temp = obj_app.getYarnTotalMemory(yarn_rm, yarn_port)
        if type(temp) != type(None):
            yarn_total_memory_count = temp
            obj_pdf.yarnMemoryTotal(yarn_total_memory_count)

        (
            yarn_memory_available_df,
            yarn_memory_allocated_avg,
            yarn_memory_allocated_df,
            yarn_memory_allocated_pivot_df,
        ) = (None, None, None, None)
        temp1 = obj_app.getYarnMemoryAvailable(cluster_name)
        temp2 = obj_app.getYarnMemoryAllocated(cluster_name)
        if (type(temp1) != type(None)) and (type(temp2) != type(None)):
            (
                yarn_memory_available_df,
                [
                    yarn_memory_allocated_avg,
                    yarn_memory_allocated_df,
                    yarn_memory_allocated_pivot_df,
                ],
            ) = (temp1, temp2)
            obj_pdf.yarnMemoryAvg(yarn_memory_allocated_avg)
            obj_pdf.yarnMemoryUsage(yarn_memory_available_df, yarn_memory_allocated_df)
            obj_pdf.yarnMemorySeasonality(yarn_memory_allocated_pivot_df)

        yarn_application_df = None
        temp = obj_app.getApplicationDetails(yarn_rm, yarn_port)
        if type(temp) != type(None):
            yarn_application_df = temp

            pdf.add_page()
            pdf.set_font("Arial", "B", 18)
            pdf.set_text_color(r=66, g=133, b=244)
            pdf.cell(230, 10, "Yarn Application Metrics", 0, ln=1)

            app_count_df, app_type_count_df, app_status_count_df = None, None, None
            temp1 = obj_app.getApplicationTypeStatusCount(yarn_application_df)
            if type(temp1) != type(None):
                app_count_df, app_type_count_df, app_status_count_df = temp1
                obj_pdf.yarnAppCount(app_count_df)
                obj_pdf.yarnAppTypeStatus(app_type_count_df, app_status_count_df)

            app_vcore_df, app_memory_df = None, None
            temp1 = obj_app.getApplicationVcoreMemoryUsage(yarn_application_df)
            if type(temp1) != type(None):
                app_vcore_df, app_memory_df = temp1
                obj_pdf.yarnAppVcoreMemory(app_vcore_df, app_memory_df)

            app_vcore_df, app_vcore_usage_df, app_memory_df, app_memory_usage_df = (
                None,
                None,
                None,
                None,
            )
            temp1 = obj_app.getVcoreMemoryByApplication(yarn_application_df)
            if type(temp1) != type(None):
                (
                    app_vcore_df,
                    app_vcore_usage_df,
                    app_memory_df,
                    app_memory_usage_df,
                ) = temp1
                obj_pdf.yarnAppVcoreUsage(app_vcore_df, app_vcore_usage_df)
                obj_pdf.yarnAppMemoryUsage(app_memory_df, app_memory_usage_df)

            pdf.add_page()
            pdf.set_font("Arial", "B", 18)
            pdf.set_text_color(r=66, g=133, b=244)
            pdf.cell(230, 10, "Bursty Applications", 0, ln=1)

            bursty_app_time_df, bursty_app_vcore_df, bursty_app_mem_df = (
                None,
                None,
                None,
            )
            temp1 = obj_app.getBurstyApplicationDetails(yarn_application_df)
            if type(temp1) != type(None):
                bursty_app_time_df, bursty_app_vcore_df, bursty_app_mem_df = temp1
                if bursty_app_time_df.size != 0:
                    obj_pdf.yarnBurstyAppTime(bursty_app_time_df)
                    obj_pdf.yarnBurstyAppVcore(bursty_app_vcore_df)
                    pdf.add_page()
                    obj_pdf.yarBurstyAppMemory(bursty_app_mem_df)

            # pdf.add_page()
            # pdf.set_font("Arial", "B", 18)
            # pdf.set_text_color(r=66, g=133, b=244)
            # pdf.cell(230, 10, "Failed Applications", 0, ln=1)

            # yarn_failed_app = None
            # temp1 = obj_app.getFailedApplicationDetails(yarn_application_df)
            # if type(temp1) != type(None):
            #     yarn_failed_app = temp1
            #     obj_pdf.yarnFailedApp(yarn_failed_app)

            pdf.add_page()
            pdf.set_font("Arial", "B", 18)
            pdf.set_text_color(r=66, g=133, b=244)
            pdf.cell(230, 10, "Yarn Queues", 0, ln=1)

            yarn_queues_list = None
            temp1 = obj_app.getQueueDetails(yarn_rm, yarn_port)
            if type(temp1) != type(None):
                yarn_queues_list = temp1
                obj_pdf.yarnQueue(yarn_queues_list)

            queue_app_count_df, queue_elapsed_time_df = None, None
            temp1 = obj_app.getQueueApplication(yarn_application_df)
            if type(temp1) != type(None):
                queue_app_count_df, queue_elapsed_time_df = temp1
                obj_pdf.yarnQueueApp(queue_app_count_df, queue_elapsed_time_df)

            (
                queue_vcore_df,
                queue_vcore_usage_df,
                queue_memory_df,
                queue_memory_usage_df,
            ) = (None, None, None, None)
            temp1 = obj_app.getQueueVcoreMemory(yarn_application_df)
            if type(temp1) != type(None):
                (
                    queue_vcore_df,
                    queue_vcore_usage_df,
                    queue_memory_df,
                    queue_memory_usage_df,
                ) = temp1
                obj_pdf.yarnQueueVcore(queue_vcore_df, queue_vcore_usage_df)
                obj_pdf.yarnQueueMemory(queue_memory_df, queue_memory_usage_df)

            if (self.version == 6) or (self.version == 7):
                app_queue_df, app_queue_usage_df = None, None
                temp1 = obj_app.getQueuePendingApplication(yarn_application_df)
                if type(temp1) != type(None):
                    app_queue_df, app_queue_usage_df = temp1
                    obj_pdf.yarnQueuePendingApp(app_queue_df, app_queue_usage_df)

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Yarn Pending Applications", 0, ln=1)

        yarn_pending_apps_df = None
        temp = obj_app.getPendingApplication(cluster_name)
        if type(temp) != type(None):
            yarn_pending_apps_df = temp
            obj_pdf.yarnPendingApp(yarn_pending_apps_df)

        yarn_pending_vcore_df = None
        temp = obj_app.getPendingVcore(cluster_name)
        if type(temp) != type(None):
            yarn_pending_vcore_df = temp
            obj_pdf.yarnPendingVcore(yarn_pending_vcore_df)

        yarn_pending_memory_df = None
        temp = obj_app.getPendingMemory(cluster_name)
        if type(temp) != type(None):
            yarn_pending_memory_df = temp
            obj_pdf.yarnPendingMemory(yarn_pending_memory_df)

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "HBase Metrics", 0, ln=1)

        base_size, disk_space_consumed = None, None
        temp = obj_app.getHbaseDataSize()
        if type(temp) != type(None):
            base_size, disk_space_consumed = temp
            obj_pdf.hbaseStorage(base_size, disk_space_consumed)

        replication = None
        temp = obj_app.getHbaseReplication(cluster_name)
        if type(temp) != type(None):
            replication = temp
            obj_pdf.hbaseReplication(replication)

        indexing = None
        temp = obj_app.getHbaseSecondaryIndex(cluster_name)
        if type(temp) != type(None):
            indexing = temp
            obj_pdf.hbaseIndexing(indexing)

        pdf.cell(230, 10, "", 0, ln=1)
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Spark Metrics", 0, ln=1)

        spark_version = None
        temp = obj_app.getSparkVersion()
        if type(temp) != type(None):
            spark_version = temp
            obj_pdf.sparkVersion(spark_version)

        languages = None
        temp = obj_app.getSparkApiProgrammingLanguages()
        if type(temp) != type(None):
            languages = temp
            obj_pdf.sparkLanguages(languages)

        dynamic_allocation, spark_resource_manager = None, None
        temp = obj_app.getDynamicAllocationAndSparkResourceManager()
        if type(temp) != type(None):
            dynamic_allocation, spark_resource_manager = temp
            obj_pdf.sparkDynamicAllocationAndResourceManager(
                dynamic_allocation, spark_resource_manager
            )

        pdf.cell(230, 10, "", 0, ln=1)
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Kafka Metrics", 0, ln=1)

        zookeeper_host, zookeeper_port, broker_host, broker_port = (
            "localhost",
            "5181",
            "localhost",
            "9092",
        )

        retention_period = None
        temp = obj_app.retentionPeriodKafka()
        if type(temp) != type(None):
            retention_period = temp
            obj_pdf.retentionPeriod(retention_period)

        num_topics = None
        temp = obj_app.numTopicsKafka(zookeeper_host, zookeeper_port)
        if type(temp) != type(None):
            num_topics = temp
            obj_pdf.numTopics(num_topics)

        sum_size = None
        temp = obj_app.msgSizeKafka(
            zookeeper_host, zookeeper_port, broker_host, broker_port
        )
        if type(temp) != type(None):
            sum_size = temp
            obj_pdf.msgSize(sum_size)

        sum_count = None
        temp = obj_app.msgCountKafka(
            zookeeper_host, zookeeper_port, broker_host, broker_port
        )
        if type(temp) != type(None):
            sum_count = temp
            obj_pdf.msgCount(sum_count)

        total_size, brokersize = None, None
        temp = obj_app.clusterSizeAndBrokerSizeKafka()
        if type(temp) != type(None):
            total_size, brokersize = temp
            obj_pdf.clusterSizeAndBrokerSize(total_size, brokersize)

        pdf.cell(230, 10, "", 0, ln=1)
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Cloudera Services", 0, ln=1)

        impala = None
        temp = obj_app.useOfImpala()
        if type(temp) != type(None):
            impala = temp
            obj_pdf.impala(impala)

        sentry = None
        temp = obj_app.useOfSentry()
        if type(temp) != type(None):
            sentry = temp
            obj_pdf.sentry(sentry)

        kudu = None
        temp = obj_app.useOfKudu()
        if type(temp) != type(None):
            kudu = temp
            obj_pdf.kudu(kudu)

        p_bar.update(1)
        p_bar.set_description(desc="Application Metrics Added in PDF")
        p_bar.close()

        print("Completed !!")
        pdf.output("Discovery_Report/{}.pdf".format(cluster_name), "F")
