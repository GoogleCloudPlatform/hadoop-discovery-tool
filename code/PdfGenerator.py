from imports import *
from HardwareOSAPI import *
from FrameworkDetailsAPI import *
from DataAPI import *
from SecurityAPI import *
from ApplicationAPI import *
from PdfFunctions import *


class PdfGenerator:
    def __init__(self, inputs):
        self.inputs = inputs
        self.version = inputs["version"]
        self.cloudera_manager_host_ip = inputs["cloudera_manager_host_ip"]
        self.cloudera_manager_username = inputs["cloudera_manager_username"]
        self.cloudera_manager_password = inputs["cloudera_manager_password"]
        self.cluster_name = inputs["cluster_name"]
        self.logger = inputs["logger"]

    def run_5(self):
        pdf = FPDF(format=(250, 350))
        obj1 = HardwareOSAPI(self.inputs)
        obj2 = DataAPI(self.inputs)
        obj3 = FrameworkDetailsAPI(self.inputs)
        obj_app = ApplicationAPI(self.inputs)
        obj_pdf = PdfFunctions(self.inputs, pdf)
        yarn_rm = ""
        hive_server2 = ""
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
        pdf.set_text_color(r=1, g=1, b=1)
        pdf.set_font("Arial", "", 12)
        pdf.cell(
            230,
            8,
            "Report Date Range : Start  {} ".format(date_range_start),
            0,
            ln=1,
            align="R",
        )
        pdf.cell(0, 8, "End  {} ".format(date_range_end), 0, ln=1, align="R")

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
                    if "HIVESERVER2" in role["roleName"].upper():
                        hive_server2 = host["ipAddress"]

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

        pdf.add_page()

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

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Data Section", 0, ln=1)

        individual_node_size, total_storage = None, None
        temp = obj2.totalSizeConfigured()
        if type(temp) != type(None):
            individual_node_size, total_storage = temp
            obj_pdf.totalHDFSSize(total_storage)

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
        pdf.cell(230, 10, "Yarn Metrics", 0, ln=1)

        pdf.set_font("Arial", "", 12)
        pdf.set_text_color(r=1, g=1, b=1)
        pdf.cell(230, 10, "VCore Details:", 0, ln=1)

        yarn_total_vcores_count = None
        temp = obj_app.getYarnTotalVcore(yarn_rm)
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
        pdf.set_text_color(r=1, g=1, b=1)
        pdf.cell(230, 10, "Memory Details:", 0, ln=1)

        yarn_total_memory_count = None
        temp = obj_app.getYarnTotalMemory(yarn_rm)
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
        temp = obj_app.getApplicationDetails(yarn_rm)
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

            pdf.add_page()

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

            pdf.add_page()
            pdf.set_font("Arial", "B", 18)
            pdf.set_text_color(r=66, g=133, b=244)
            pdf.cell(230, 10, "Failed Applications", 0, ln=1)

            yarn_failed_app = None
            temp1 = obj_app.getFailedApplicationDetails(yarn_application_df)
            if type(temp1) != type(None):
                yarn_failed_app = temp1
                obj_pdf.yarnFailedApp(yarn_failed_app)

            pdf.add_page()
            pdf.set_font("Arial", "B", 18)
            pdf.set_text_color(r=66, g=133, b=244)
            pdf.cell(230, 10, "Yarn Queues", 0, ln=1)

            yarn_queues_list = None
            temp1 = obj_app.getQueueDetails(yarn_rm)
            if type(temp1) != type(None):
                yarn_queues_list = temp1
                obj_pdf.yarnQueue(yarn_queues_list)

            queue_app_count_df, queue_elapsed_time_df = None, None
            temp1 = obj_app.getQueueApplication(yarn_application_df)
            if type(temp1) != type(None):
                queue_app_count_df, queue_elapsed_time_df = temp1
                obj_pdf.yarnQueueApp(queue_app_count_df, queue_elapsed_time_df)

            pdf.add_page()

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

        pdf.output("Discovery_Report/{}.pdf".format(cluster_name), "F")

    def run_6(self):
        pdf = FPDF(format=(250, 350))
        obj1 = HardwareOSAPI(self.inputs)
        obj2 = DataAPI(self.inputs)
        obj3 = FrameworkDetailsAPI(self.inputs)
        obj_app = ApplicationAPI(self.inputs)
        obj_pdf = PdfFunctions(self.inputs, pdf)
        yarn_rm = ""
        hive_server2 = ""
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
        pdf.set_text_color(r=1, g=1, b=1)
        pdf.set_font("Arial", "", 12)
        pdf.cell(
            230,
            8,
            "Report Date Range : Start  {} ".format(date_range_start),
            0,
            ln=1,
            align="R",
        )
        pdf.cell(0, 8, "End  {} ".format(date_range_end), 0, ln=1, align="R")

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
                    if "HIVESERVER2" in role["roleName"].upper():
                        hive_server2 = host["ipAddress"]

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

        pdf.add_page()

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

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Data Section", 0, ln=1)

        individual_node_size, total_storage = None, None
        temp = obj2.totalSizeConfigured()
        if type(temp) != type(None):
            individual_node_size, total_storage = temp
            obj_pdf.totalHDFSSize(total_storage)

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
        pdf.cell(230, 10, "Yarn Metrics", 0, ln=1)

        pdf.set_font("Arial", "", 12)
        pdf.set_text_color(r=1, g=1, b=1)
        pdf.cell(230, 10, "VCore Details:", 0, ln=1)

        yarn_total_vcores_count = None
        temp = obj_app.getYarnTotalVcore(yarn_rm)
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
        pdf.set_text_color(r=1, g=1, b=1)
        pdf.cell(230, 10, "Memory Details:", 0, ln=1)

        yarn_total_memory_count = None
        temp = obj_app.getYarnTotalMemory(yarn_rm)
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
        temp = obj_app.getApplicationDetails(yarn_rm)
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

            pdf.add_page()

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

            pdf.add_page()
            pdf.set_font("Arial", "B", 18)
            pdf.set_text_color(r=66, g=133, b=244)
            pdf.cell(230, 10, "Failed Applications", 0, ln=1)

            yarn_failed_app = None
            temp1 = obj_app.getFailedApplicationDetails(yarn_application_df)
            if type(temp1) != type(None):
                yarn_failed_app = temp1
                obj_pdf.yarnFailedApp(yarn_failed_app)

            pdf.add_page()
            pdf.set_font("Arial", "B", 18)
            pdf.set_text_color(r=66, g=133, b=244)
            pdf.cell(230, 10, "Yarn Queues", 0, ln=1)

            yarn_queues_list = None
            temp1 = obj_app.getQueueDetails(yarn_rm)
            if type(temp1) != type(None):
                yarn_queues_list = temp1
                obj_pdf.yarnQueue(yarn_queues_list)

            queue_app_count_df, queue_elapsed_time_df = None, None
            temp1 = obj_app.getQueueApplication(yarn_application_df)
            if type(temp1) != type(None):
                queue_app_count_df, queue_elapsed_time_df = temp1
                obj_pdf.yarnQueueApp(queue_app_count_df, queue_elapsed_time_df)

            pdf.add_page()

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

        pdf.output("Discovery_Report/{}.pdf".format(cluster_name), "F")

    def run_7(self):
        pdf = FPDF(format=(250, 350))
        obj1 = HardwareOSAPI(self.inputs)
        obj2 = DataAPI(self.inputs)
        obj3 = FrameworkDetailsAPI(self.inputs)
        obj_app = ApplicationAPI(self.inputs)
        obj_pdf = PdfFunctions(self.inputs, pdf)
        yarn_rm = ""
        hive_server2 = ""
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
        pdf.set_text_color(r=1, g=1, b=1)
        pdf.set_font("Arial", "", 12)
        pdf.cell(
            230,
            8,
            "Report Date Range : Start  {} ".format(date_range_start),
            0,
            ln=1,
            align="R",
        )
        pdf.cell(0, 8, "End  {} ".format(date_range_end), 0, ln=1, align="R")

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
                    if "HIVESERVER2" in role["roleName"].upper():
                        hive_server2 = host["ipAddress"]

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

        pdf.add_page()

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

        pdf.add_page()
        pdf.set_font("Arial", "B", 18)
        pdf.set_text_color(r=66, g=133, b=244)
        pdf.cell(230, 10, "Data Section", 0, ln=1)

        individual_node_size, total_storage = None, None
        temp = obj2.totalSizeConfigured()
        if type(temp) != type(None):
            individual_node_size, total_storage = temp
            obj_pdf.totalHDFSSize(total_storage)

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
        pdf.cell(230, 10, "Yarn Metrics", 0, ln=1)

        pdf.set_font("Arial", "", 12)
        pdf.set_text_color(r=1, g=1, b=1)
        pdf.cell(230, 10, "VCore Details:", 0, ln=1)

        yarn_total_vcores_count = None
        temp = obj_app.getYarnTotalVcore(yarn_rm)
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
        pdf.set_text_color(r=1, g=1, b=1)
        pdf.cell(230, 10, "Memory Details:", 0, ln=1)

        yarn_total_memory_count = None
        temp = obj_app.getYarnTotalMemory(yarn_rm)
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
        temp = obj_app.getApplicationDetails(yarn_rm)
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

            pdf.add_page()

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

            pdf.add_page()
            pdf.set_font("Arial", "B", 18)
            pdf.set_text_color(r=66, g=133, b=244)
            pdf.cell(230, 10, "Failed Applications", 0, ln=1)

            yarn_failed_app = None
            temp1 = obj_app.getFailedApplicationDetails(yarn_application_df)
            if type(temp1) != type(None):
                yarn_failed_app = temp1
                obj_pdf.yarnFailedApp(yarn_failed_app)

            pdf.add_page()
            pdf.set_font("Arial", "B", 18)
            pdf.set_text_color(r=66, g=133, b=244)
            pdf.cell(230, 10, "Yarn Queues", 0, ln=1)

            yarn_queues_list = None
            temp1 = obj_app.getQueueDetails(yarn_rm)
            if type(temp1) != type(None):
                yarn_queues_list = temp1
                obj_pdf.yarnQueue(yarn_queues_list)

            queue_app_count_df, queue_elapsed_time_df = None, None
            temp1 = obj_app.getQueueApplication(yarn_application_df)
            if type(temp1) != type(None):
                queue_app_count_df, queue_elapsed_time_df = temp1
                obj_pdf.yarnQueueApp(queue_app_count_df, queue_elapsed_time_df)

            pdf.add_page()

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

        pdf.output("Discovery_Report/{}.pdf".format(cluster_name), "F")

