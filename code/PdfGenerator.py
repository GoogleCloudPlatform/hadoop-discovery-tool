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
from NetworkMonitoringAPI import *


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
        self.ssl = inputs["ssl"]
        self.hive_username = inputs["hive_username"]
        self.hive_password = inputs["hive_password"]
        self.start_date = inputs["start_date"]
        self.end_date = inputs["end_date"]

    def run(self):
        """Generate PDF for CDH-5, CDH-6 and CDP-7"""

        pdf = FPDF(format=(250, 350))
        p_bar = tqdm(total=6, desc="Hadoop Assessment Tool")
        obj1 = HardwareOSAPI(self.inputs)
        obj2 = DataAPI(self.inputs)
        obj3 = FrameworkDetailsAPI(self.inputs)
        obj4 = SecurityAPI(self.inputs)
        obj5 = NetworkMonitoringAPI(self.inputs)
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
        pdf.cell(230, 12, "Hadoop Assessment Report", 0, ln=1, align="C")
        pdf.set_font("Arial", "B", 12)
        pdf.set_fill_color(r=66, g=133, b=244)
        pdf.set_text_color(r=255, g=255, b=255)
        pdf.cell(0, 10, "", 0, 1)
        pdf.cell(60, 16, "Report Date Range", 1, 0, "C", True)
        pdf.cell(40, 8, "Start Date", 1, 0, "C", True)
        pdf.cell(40, 8, "End Date", 1, 1, "C", True)
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
            "C",
            True,
        )
        pdf.cell(
            40,
            8,
            datetime.strptime(self.end_date, "%Y-%m-%dT%H:%M:%S").strftime("%d-%b-%Y"),
            1,
            1,
            "C",
            True,
        )
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

        replication_factor = None
        temp = obj2.replicationFactor()
        if type(temp) != type(None):
            replication_factor = temp

        mt_db_host, mt_db_name, mt_db_type, mt_db_port = None, None, None, None
        database_df = None
        size_breakdown_df = None
        table_df = None
        temp = obj2.getHiveConfigItems(cluster_name)
        if type(temp) != type(None):
            mt_db_host, mt_db_name, mt_db_type, mt_db_port = temp

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

            temp1 = obj2.getHiveDatabaseInfo(database_uri, mt_db_type)
            if type(temp1) != type(None):
                database_df = temp1

            if (type(hdfs_storage_used) != type(None)) and (
                type(database_df) != type(None)
            ):

                temp = obj2.structuredVsUnstructured(hdfs_storage_used, database_df)
                if type(temp) != type(None):
                    size_breakdown_df = temp

            table_df = None
            temp1 = obj2.gethiveMetaStore(database_uri, mt_db_type)
            if type(temp1) != type(None):
                table_df = temp1

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

        list_services_installed_df, new_ref_df = None, None
        temp = obj3.versionMapping(cluster_name)
        if type(temp) != type(None):
            list_services_installed_df, new_ref_df = temp

        base_size, disk_space_consumed = None, None
        temp = obj_app.getHbaseDataSize()
        if type(temp) != type(None):
            base_size, disk_space_consumed = temp

        obj_pdf.summaryTable(
            all_host_data,
            cluster_cpu_usage_avg,
            cluster_memory_usage_avg,
            hadoopVersionMajor,
            hadoopVersionMinor,
            distribution,
            total_storage,
            hdfs_storage_config,
            replication_factor,
            database_df,
            size_breakdown_df,
            table_df,
            yarn_vcore_allocated_avg,
            yarn_memory_allocated_avg,
            new_ref_df,
            base_size,
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

        xml_data = os.popen("cat /etc/hadoop/conf/yarn-site.xml").read()
        root = ET.fromstring(xml_data)
        for val in root.findall("property"):
            name = val.find("name").text
            value = val.find("value").text
            if self.ssl:
                if "yarn.resourcemanager.webapp.https.address" in name:
                    yarn_rm, yarn_port = value.split(":")
            else:
                if "yarn.resourcemanager.webapp.address" in name:
                    yarn_rm, yarn_port = value.split(":")

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

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Hardware and OS Metrics", 0, ln=1)

        pdf.set_font("Arial", "", 12)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 8, "Types of Servers Details:", 0, ln=1)

        database_server, dns_server, web_server, ntp_server = None, None, None, None
        t1 = obj1.dataBaseServer()
        t2 = obj1.dnsServer()
        t3 = obj1.webServer()
        t4 = obj1.ntpServer()

        if type(t1) != type(None):
            database_server = t1
            obj_pdf.dataBaseServer(database_server)

        if type(t2) != type(None):
            dns_server = t2
            obj_pdf.dnsServer(dns_server)

        if type(t3) != type(None):
            web_server = t3
            obj_pdf.webServer(web_server)

        if type(t4) != type(None):
            ntp_server = t4
            obj_pdf.ntpServer(ntp_server)

        pdf.set_font("Arial", "", 12)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 8, "Manufacturer and Processor Details:", 0, ln=1)

        (
            manufacturer_name,
            serial_no,
            family,
            model_name,
            microcode,
            cpu_mhz,
            cpu_family,
        ) = (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        t1 = obj1.manufacturerName()
        t2 = obj1.serialNo()
        t3 = obj1.family()
        t4 = obj1.modelName()
        t5 = obj1.microcode()
        t6 = obj1.cpuMHz()
        t7 = obj1.cpuFamily()

        if type(t1) != type(None):
            manufacturer_name = t1
            obj_pdf.manufacturerName(manufacturer_name)

        if type(t2) != type(None):
            serial_no = t2
            obj_pdf.serialNo(serial_no)

        if type(t3) != type(None):
            family = t3
            obj_pdf.family(family)

        if type(t4) != type(None):
            model_name = t4
            obj_pdf.modelName(model_name)

        if type(t5) != type(None):
            microcode = t5
            obj_pdf.microcode(microcode)

        if type(t6) != type(None):
            cpu_mhz = t6
            obj_pdf.cpuMHz(cpu_mhz)

        if type(t7) != type(None):
            cpu_family = t7
            obj_pdf.cpuFamily(cpu_family)

        nic_details = None
        temp = obj1.networkInterfaceDetails()
        if type(temp) != type(None):
            nic_details = temp
            obj_pdf.networkInterfaceDetails(nic_details)

        patch_dataframe, os_name = None, None
        temp = obj1.appliedPatches()
        if type(temp) != type(None):
            patch_dataframe, os_name = temp
            obj_pdf.appliedPatches(patch_dataframe, os_name)

        hadoop_native_df = None
        temp = obj1.listHadoopNonHadoopLibs()
        if type(temp) != type(None):
            hadoop_native_df = temp
            obj_pdf.listHadoopNonHadoopLibs(hadoop_native_df)

        python_flag, java_flag, scala_flag = None, None, None
        temp = obj1.checkLibrariesInstalled()
        if type(temp) != type(None):
            python_flag, java_flag, scala_flag = temp
            obj_pdf.checkLibrariesInstalled(python_flag, java_flag, scala_flag)

        security_software = None
        temp = obj1.securitySoftware()
        if type(temp) != type(None):
            security_software = temp
            obj_pdf.securitySoftware(security_software)

        gpu_status = None
        temp = obj1.specialityHardware()
        if type(temp) != type(None):
            gpu_status = temp
            obj_pdf.specialityHardware(gpu_status)

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

        pdf.set_font("Arial", "", 12)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 8, "Third Party Software and Their Version:", 0, ln=1)

        third_party_package = None
        temp = obj3.thirdPartySoftware()
        if type(temp) != type(None):
            third_party_package = temp
            obj_pdf.thirdPartySoftware(third_party_package)

        package_version = None
        temp = obj3.versionPackage()
        if type(temp) != type(None):
            package_version = temp
            obj_pdf.versionPackage(package_version)

        pdf.set_font("Arial", "", 12)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 8, "Drivers and Connectors:", 0, ln=1)

        df_ngdbc, df_salesforce = None, None
        temp = obj3.salesFroceSapDriver()
        if type(temp) != type(None):
            df_ngdbc, df_salesforce = temp
            obj_pdf.salesFroceSapDriver(df_ngdbc, df_salesforce)

        final_df = None
        temp = obj3.jdbcOdbcDriver()
        if type(temp) != type(None):
            final_df = temp
            obj_pdf.jdbcOdbcDriver(final_df)

        connectors_present = None
        temp = obj3.installedConnectors()
        if type(temp) != type(None):
            connectors_present = temp
            obj_pdf.installedConnectors(connectors_present)

        p_bar.update(1)
        p_bar.set_description(desc="Framework and software details Added in PDF")

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "HDFS Section", 0, ln=1)

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

        value = None
        temp = obj2.checkCompression()
        if type(temp) != type(None):
            value = temp
            obj_pdf.checkCompression(value)

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

        hdfs_storage_df, hdfs_flag = None, None
        temp = obj2.hdfsStorage()
        if type(temp) != type(None):
            hdfs_storage_df, hdfs_flag = temp
            obj_pdf.hdfsStorage(hdfs_storage_df, hdfs_flag)

        grpby_data, max_value, min_value, avg_value = None, None, None, None
        temp = obj2.clusterFileSize()
        if type(temp) != type(None):
            grpby_data, max_value, min_value, avg_value = temp
            obj_pdf.clusterFileSize(grpby_data, max_value, min_value, avg_value)

        hdfs_root_dir = None
        temp = obj2.getCliresult("/")
        if type(temp) != type(None):
            hdfs_root_dir = temp
            pdf.set_font("Arial", "", 12)
            pdf.set_text_color(r=66, g=133, b=244)
            pdf.cell(230, 8, "HDFS Size Breakdown:", 0, ln=1)
            pdf.set_font("Arial", "", 12)
            pdf.set_text_color(r=1, g=1, b=1)
            for i in hdfs_root_dir.splitlines():
                hdfs_dir = i.split()
                if len(hdfs_dir) == 5:
                    hdfs_dir[0] = hdfs_dir[0] + b" " + hdfs_dir[1]
                    hdfs_dir[1] = hdfs_dir[2] + b" " + hdfs_dir[3]
                    hdfs_dir[2] = hdfs_dir[4]
                pdf.cell(
                    230,
                    8,
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
                        8,
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
                formats,
                transaction_locking_concurrency,
                hive_interactive_status,
            ) = (None, [None, None], [None, None], None, None, None, None)
            t1 = obj2.getHiveDatabaseCount(database_uri, mt_db_type)
            t2 = obj2.getHivePartitionedTableCount(database_uri, mt_db_type)
            t3 = obj2.getHiveInternalExternalTables(database_uri, mt_db_type)
            t4 = obj2.getHiveExecutionEngine()
            t5 = obj2.getHiveFileFormats(database_uri, mt_db_type)
            t6 = obj2.getTransactionLockingConcurrency()
            t7 = obj2.interactiveQueriesStatus()
            if (
                (type(t1) != type(None))
                and (type(t2) != type(None))
                and (type(t3) != type(None))
                and (type(t4) != type(None))
                and (type(t5) != type(None))
                and (type(t6) != type(None))
                and (type(t7) != type(None))
            ):
                (
                    database_count,
                    [tables_with_partition, tables_without_partition],
                    [internal_tables, external_tables],
                    hive_execution_engine,
                    formats,
                    transaction_locking_concurrency,
                    hive_interactive_status,
                ) = (t1, t2, t3, t4, t5, t6, t7)
                obj_pdf.hiveDetails(
                    database_count,
                    tables_with_partition,
                    tables_without_partition,
                    internal_tables,
                    external_tables,
                    hive_execution_engine,
                    formats,
                    transaction_locking_concurrency,
                    hive_interactive_status,
                )

            database_df = None
            temp1 = obj2.getHiveDatabaseInfo(database_uri, mt_db_type)
            if type(temp1) != type(None):
                database_df = temp1
                obj_pdf.hiveDatabasesSize(database_df)

            if (type(hdfs_storage_used) != type(None)) and (
                type(database_df) != type(None)
            ):
                size_breakdown_df = None
                temp = obj2.structuredVsUnstructured(hdfs_storage_used, database_df)
                if type(temp) != type(None):
                    size_breakdown_df = temp
                    obj_pdf.structuredVsUnstructured(size_breakdown_df)

            table_df = None
            temp1 = obj2.gethiveMetaStore(database_uri, mt_db_type)
            if type(temp1) != type(None):
                table_df = temp1
                obj_pdf.hiveAccessFrequency(table_df)

            query_type_count_df = None
            temp1 = obj2.getHiveAdhocEtlQuery(yarn_rm, yarn_port)
            if type(temp1) != type(None):
                query_type_count_df = temp1
                obj_pdf.hiveAdhocEtlQuery(query_type_count_df)

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

        luks_detect = None
        temp = obj4.checkLuks()
        if type(temp) != type(None):
            luks_detect = temp
            obj_pdf.checkLuks(luks_detect)

        Mr_ssl, hdfs_ssl, yarn_ssl = None, None, None
        temp = obj4.sslStatus()
        if type(temp) != type(None):
            Mr_ssl, hdfs_ssl, yarn_ssl = temp
            obj_pdf.sslStatus(Mr_ssl, hdfs_ssl, yarn_ssl)

        hue_flag, hdfs_flag, yarn_flag_1, yarn_flag_2, mapred_flag = (
            None,
            None,
            None,
            None,
            None,
        )
        temp = obj4.kerberosHttpAuth()
        if type(temp) != type(None):
            hue_flag, hdfs_flag, yarn_flag_1, yarn_flag_2, mapred_flag = temp
            obj_pdf.kerberosHttpAuth(
                hue_flag, hdfs_flag, yarn_flag_1, yarn_flag_2, mapred_flag
            )

        port_df = None
        temp = obj4.portUsed()
        if type(temp) != type(None):
            port_df = temp
            obj_pdf.portUsed(port_df)

        key_list = None
        temp = obj4.keyList()
        if type(temp) != type(None):
            key_list = temp
            obj_pdf.keyList(key_list)

        enc_zoneList = None
        temp = obj4.encryptionZone()
        if type(temp) != type(None):
            enc_zoneList = temp
            obj_pdf.encryptionZone(enc_zoneList)

        p_bar.update(1)
        p_bar.set_description(desc="Security Metrics Added in PDF")

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Network, Traffic and Monitoring Metrics", 0, ln=1)

        max_bandwidth = None
        temp = obj5.maxBandwidth()
        if type(temp) != type(None):
            max_bandwidth = temp
            obj_pdf.maxBandwidth(max_bandwidth)

        max_value, min_value, avg_value, curr_value = (
            None,
            None,
            None,
            None,
        )
        temp = obj5.ingress()
        if type(temp) != type(None):
            max_value, min_value, avg_value, curr_value = temp
            obj_pdf.ingress(max_value, min_value, avg_value, curr_value)

        max_value, min_value, avg_value, curr_value = (
            None,
            None,
            None,
            None,
        )
        temp = obj5.egress()
        if type(temp) != type(None):
            max_value, min_value, avg_value, curr_value = temp
            obj_pdf.egress(max_value, min_value, avg_value, curr_value)

        total_disk_read, total_disk_write = None, None
        temp = obj5.diskReadWrite()
        if type(temp) != type(None):
            total_disk_read, total_disk_write = temp
            obj_pdf.diskReadWrite(total_disk_read, total_disk_write)

        (
            softwares_installed,
            prometheus_server,
            grafana_server,
            ganglia_server,
            check_mk_server,
        ) = (None, None, None, None, None)
        temp = obj5.thirdPartyMonitor()
        if type(temp) != type(None):
            (
                softwares_installed,
                prometheus_server,
                grafana_server,
                ganglia_server,
                check_mk_server,
            ) = temp
            obj_pdf.thirdPartyMonitor(
                softwares_installed,
                prometheus_server,
                grafana_server,
                ganglia_server,
                check_mk_server,
            )

        oozie_flag, crontab_flag, airflow_flag = None, None, None
        temp = obj5.orchestrationTools()
        if type(temp) != type(None):
            oozie_flag, crontab_flag, airflow_flag = temp
            obj_pdf.orchestrationTools(oozie_flag, crontab_flag, airflow_flag)

        ddog, splunk, new_relic, elastic_search = None, None, None, None
        temp = obj5.loggingTool()
        if type(temp) != type(None):
            ddog, splunk, new_relic, elastic_search = temp
            obj_pdf.loggingTool(ddog, splunk, new_relic, elastic_search)

        logs = None
        temp = obj5.getLogs()
        if type(temp) != type(None):
            logs = temp
            obj_pdf.getLogs(logs)

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Yarn Metrics", 0, ln=1)

        resource = None
        temp = obj_app.dynamicResoucePool()
        if type(temp) != type(None):
            resource = temp
            obj_pdf.dynamicResoucePool(resource)

        zookeeper_ha, hive_ha, yarn_ha, hdfs_ha = None, None, None, None
        temp = obj_app.identifyHA()
        if type(temp) != type(None):
            zookeeper_ha, hive_ha, yarn_ha, hdfs_ha = temp
            obj_pdf.identifyHA(zookeeper_ha, hive_ha, yarn_ha, hdfs_ha)

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

            only_streaming = None
            temp1 = obj_app.streamingJobs(yarn_application_df)
            if type(temp1) != type(None):
                only_streaming = temp1
                obj_pdf.streamingJobs(only_streaming)

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

            job_launch_df = None
            temp1 = obj_app.getJobLaunchFrequency(yarn_application_df)
            if type(temp1) != type(None):
                job_launch_df = temp1
                obj_pdf.yarnJobLaunchFrequency(job_launch_df)

            bursty_app_time_df, bursty_app_vcore_df, bursty_app_mem_df = (
                None,
                None,
                None,
            )
            temp1 = obj_app.getBurstyApplicationDetails(yarn_application_df)
            if type(temp1) != type(None):
                bursty_app_time_df, bursty_app_vcore_df, bursty_app_mem_df = temp1
                if bursty_app_time_df.size != 0:
                    pdf.add_page()
                    pdf.set_font("Arial", "B", 18)
                    pdf.set_text_color(r=66, g=133, b=244)
                    pdf.cell(230, 10, "Bursty Applications", 0, ln=1)
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
        pdf.cell(230, 10, "Yarn Pending and Running Applications", 0, ln=1)

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

        yarn_running_apps_df = None
        temp = obj_app.getRunningApplication(cluster_name)
        if type(temp) != type(None):
            yarn_running_apps_df = temp
            obj_pdf.yarnRunningApp(yarn_running_apps_df)

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "HBase Metrics", 0, ln=1)

        NumNodesServing = None
        temp = obj_app.nodesServingHbase()
        if type(temp) != type(None):
            NumNodesServing = temp
            obj_pdf.nodesServingHbase(NumNodesServing)

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

        hbasehive_var = None
        temp = obj_app.hBaseOnHive()
        if type(temp) != type(None):
            hbasehive_var = temp
            obj_pdf.hBaseOnHive(hbasehive_var)

        phoenixHbase = None
        temp = obj_app.phoenixinHBase()
        if type(temp) != type(None):
            phoenixHbase = temp
            obj_pdf.phoenixinHBase(phoenixHbase)

        coprocessorHbase = None
        temp = obj_app.coprocessorinHBase()
        if type(temp) != type(None):
            coprocessorHbase = temp
            obj_pdf.coprocessorinHBase(coprocessorHbase)

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
        temp = obj_app.sparkComponentsUsed()
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

        rdd_flag, dataset_flag, sql_flag, df_flag, mllib_flag, stream_flag = (
            None,
            None,
            None,
            None,
            None,
            None,
        )
        temp = obj_app.sparkComponentsUsed()
        if type(temp) != type(None):
            rdd_flag, dataset_flag, sql_flag, df_flag, mllib_flag, stream_flag = temp
            obj_pdf.sparkComponentsUsed(
                rdd_flag, dataset_flag, sql_flag, df_flag, mllib_flag, stream_flag
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

        br = None
        temp = obj_app.backupAndRecovery()
        if type(temp) != type(None):
            br = temp
            obj_pdf.backupAndRecovery(br)

        services = None
        temp = obj_app.getClouderaServicesUsedForIngestion(cluster_name)
        if type(temp) != type(None):
            services = temp
            obj_pdf.servicesUsedForIngestion(services)

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
