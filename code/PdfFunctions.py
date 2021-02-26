# ------------------------------------------------------------------------------
# pdffunction is a helper module to generate pdf which handles exception and
# based on that the flow will go the particular module as a argument
# ------------------------------------------------------------------------------

# Importing required libraries
from imports import *


class PdfFunctions:
    """This Class has helper functions for pdf generation.

    Args:
        inputs (dict): Contains user input attributes.
        pdf (obj): PDF object.
    """

    def __init__(self, inputs, pdf):
        """Initialize inputs"""

        self.inputs = inputs
        self.version = inputs["version"]
        self.cloudera_manager_host_ip = inputs["cloudera_manager_host_ip"]
        self.cloudera_manager_username = inputs["cloudera_manager_username"]
        self.cloudera_manager_password = inputs["cloudera_manager_password"]
        self.cluster_name = inputs["cluster_name"]
        self.logger = inputs["logger"]
        self.start_date = inputs["start_date"]
        self.end_date = inputs["end_date"]
        self.pdf = pdf

    def summaryTable(
        self,
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
    ):
        """Add cluster information in PDF

        Args:
            data (dict): Key value pair data for summary table
        """

        self.pdf.set_font("Arial", "B", 12)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(175, 5, "Metrics", 1, 0, "C", True)
        self.pdf.cell(50, 5, "Value", 1, 1, "C", True)
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        if type(all_host_data) != type(None):
            host_df = pd.DataFrame(columns=["Hostname"])
            namenodes_df = pd.DataFrame(columns=["HostName"])
            datanodes_df = pd.DataFrame(columns=["HostName"])
            edgenodes_df = pd.DataFrame(columns=["HostName"])
            for i, host in enumerate(all_host_data):
                host_df = host_df.append(
                    pd.DataFrame({"Hostname": [host["hostname"]],}), ignore_index=True,
                )
                for role in host["roleRefs"]:
                    if re.search(r"\bNAMENODE\b", role["roleName"]):
                        namenodes_df = namenodes_df.append(
                            pd.DataFrame({"HostName": [host["hostname"]],}),
                            ignore_index=True,
                        )
                    if re.search(r"\bDATANODE\b", role["roleName"]):
                        datanodes_df = datanodes_df.append(
                            pd.DataFrame({"HostName": [host["hostname"]],}),
                            ignore_index=True,
                        )
                    if (
                        re.search(r"\bGATEWAY\b", role["roleName"])
                        and "hdfs" in role["serviceName"]
                    ):
                        edgenodes_df = edgenodes_df.append(
                            pd.DataFrame({"HostName": [host["hostname"]],}),
                            ignore_index=True,
                        )
            self.pdf.cell(175, 5, "Number of Host", 1, 0, "L", True)
            self.pdf.cell(50, 5, str(len(host_df)), 1, 1, "C", True)
            self.pdf.cell(175, 5, "Number of NameNodes", 1, 0, "L", True)
            self.pdf.cell(50, 5, str(len(namenodes_df)), 1, 1, "C", True)
            self.pdf.cell(175, 5, "Number of DataNodes", 1, 0, "L", True)
            self.pdf.cell(50, 5, str(len(datanodes_df)), 1, 1, "C", True)
            self.pdf.cell(175, 5, "Number of EdgeNodes", 1, 0, "L", True)
            self.pdf.cell(50, 5, str(len(edgenodes_df)), 1, 1, "C", True)

        if type(cluster_cpu_usage_avg) != type(None):
            self.pdf.cell(175, 5, "Average Cluster CPU Utilization", 1, 0, "L", True)
            self.pdf.cell(
                50, 5, "{: .2f}%".format(cluster_cpu_usage_avg), 1, 1, "C", True
            )

        if type(cluster_memory_usage_avg) != type(None):
            self.pdf.cell(175, 5, "Average Cluster Memory Utilization", 1, 0, "L", True)
            self.pdf.cell(
                50, 5, "{: .2f} GB".format(cluster_memory_usage_avg), 1, 1, "C", True
            )

        if type(hadoopVersionMajor) != type(None):
            self.pdf.cell(175, 5, "Hadoop Major Version", 1, 0, "L", True)
            self.pdf.cell(50, 5, hadoopVersionMajor, 1, 1, "C", True)

        if type(hadoopVersionMinor) != type(None):
            self.pdf.cell(175, 5, "Hadoop Minor Version", 1, 0, "L", True)
            self.pdf.cell(50, 5, hadoopVersionMinor, 1, 1, "C", True)

        if type(distribution) != type(None):
            self.pdf.cell(175, 5, "Hadoop Distribution", 1, 0, "L", True)
            self.pdf.cell(50, 5, distribution, 1, 1, "C", True)

        if type(total_storage) != type(None):
            self.pdf.cell(
                175, 5, "Total Size Configured in the Cluster", 1, 0, "L", True
            )
            self.pdf.cell(50, 5, "{: .2f} GB".format(total_storage), 1, 1, "C", True)

        if type(hdfs_storage_config) != type(None):
            self.pdf.cell(175, 5, "HDFS Storage Available", 1, 0, "L", True)
            self.pdf.cell(
                50, 5, "{: .2f} GB".format(hdfs_storage_config), 1, 1, "C", True
            )

        if type(hdfs_storage_used) != type(None):
            self.pdf.cell(175, 5, "HDFS Storage Used", 1, 0, "L", True)
            self.pdf.cell(
                50, 5, "{: .2f} GB".format(hdfs_storage_used), 1, 1, "C", True
            )

        if type(yarn_vcore_allocated_avg) != type(None):
            self.pdf.cell(175, 5, "Average No. of Yarn Vcores Used", 1, 0, "L", True)
            self.pdf.cell(
                50, 5, "{: .2f}".format(yarn_vcore_allocated_avg), 1, 1, "C", True
            )

        if type(yarn_memory_allocated_avg) != type(None):
            self.pdf.cell(175, 5, "Average Yarn Memory Used", 1, 0, "L", True)
            self.pdf.cell(
                50, 5, "{: .2f} GB".format(yarn_memory_allocated_avg), 1, 1, "C", True
            )

    def clusterInfo(self, cluster_items):
        """Add cluster information in PDF

        Args:
            cluster_items (dict): Metrics of all clusters
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Number of Cluster Configured", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(len(cluster_items)), 0, 1,
        )
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "Cluster Details : ", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        cluster_df = pd.DataFrame(
            cluster_items, columns=["name", "fullVersion", "entityStatus"]
        )
        cluster_df.index = cluster_df.index + 1
        cluster_df = cluster_df.rename(
            columns={
                "name": "Cluster Name",
                "fullVersion": "Cloudera Version",
                "entityStatus": "Health Status",
            }
        )
        self.pdf.set_font("Arial", "B", 12)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(40, 5, "Cluster Name", 1, 0, "C", True)
        self.pdf.cell(40, 5, "Cloudera Version", 1, 0, "C", True)
        self.pdf.cell(50, 5, "Health Status", 1, 1, "C", True)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        self.pdf.set_font("Arial", "", 12)
        for pos in range(0, len(cluster_df)):
            x = self.pdf.get_x()
            y = self.pdf.get_y()
            line_width = 1
            line_width = max(
                line_width,
                self.pdf.get_string_width(cluster_df["Cluster Name"].iloc[pos]),
            )
            cell_y = line_width / 39.0
            line_width = max(
                line_width,
                self.pdf.get_string_width(cluster_df["Cloudera Version"].iloc[pos]),
            )
            cell_y = max(cell_y, line_width / 39.0)
            line_width = max(
                line_width,
                self.pdf.get_string_width(cluster_df["Health Status"].iloc[pos]),
            )
            cell_y = max(cell_y, line_width / 49.0)
            cell_y = line_width / 39.0
            cell_y = math.ceil(cell_y)
            cell_y = max(cell_y, 1)
            cell_y = cell_y * 5
            line_width = self.pdf.get_string_width(cluster_df["Cluster Name"].iloc[pos])
            y_pos = line_width / 39.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                40,
                y_pos,
                "{}".format(cluster_df["Cluster Name"].iloc[pos]),
                1,
                "C",
                fill=True,
            )
            self.pdf.set_xy(x + 40, y)
            line_width = self.pdf.get_string_width(
                cluster_df["Cloudera Version"].iloc[pos]
            )
            y_pos = line_width / 39.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                40,
                y_pos,
                "{}".format(cluster_df["Cloudera Version"].iloc[pos]),
                1,
                "C",
                fill=True,
            )
            self.pdf.set_xy(x + 80, y)
            line_width = self.pdf.get_string_width(
                cluster_df["Health Status"].iloc[pos]
            )
            y_pos = line_width / 49.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                50,
                y_pos,
                "{}".format(cluster_df["Health Status"].iloc[pos]),
                1,
                "C",
                fill=True,
            )
        self.pdf.cell(230, 10, "", 0, ln=1)

    def clusterHostInfo(self, cluster_host_items, all_host_data, os_version):
        """Add detailed information of all host in cluster in PDF.

        Args:
            cluster_host_items (dict): Summary of all hosts in cluster
            all_host_all (list) : Detailed specs of all hosts
            os_version (str): OS version and distribution of host
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        host_df = pd.DataFrame(
            columns=[
                "Hostname",
                "Host ip",
                "Number of cores",
                "Physical Memory",
                "Health Status",
                "Distribution",
            ]
        )
        namenodes_df = pd.DataFrame(columns=["HostName", "Cores", "Memory"])
        datanodes_df = pd.DataFrame(columns=["HostName", "Cores", "Memory"])
        edgenodes_df = pd.DataFrame(columns=["HostName", "Cores", "Memory"])
        client_gateway_df = pd.DataFrame(columns=["service"])
        for i, host in enumerate(all_host_data):
            if "distribution" in host:
                host_df = host_df.append(
                    pd.DataFrame(
                        {
                            "Hostname": [host["hostname"]],
                            "Host IP": [host["ipAddress"]],
                            "Number of cores": [host["numCores"]],
                            "Physical Memory": [
                                "{: .2f}".format(
                                    float(host["totalPhysMemBytes"])
                                    / 1024
                                    / 1024
                                    / 1024
                                )
                            ],
                            "Health Status": [host["entityStatus"]],
                            "Distribution": [
                                host["distribution"]["name"]
                                + " "
                                + host["distribution"]["version"]
                            ],
                        }
                    ),
                    ignore_index=True,
                )
            else:
                host_df = host_df.append(
                    pd.DataFrame(
                        {
                            "Hostname": [host["hostname"]],
                            "Host IP": [host["ipAddress"]],
                            "Number of cores": [host["numCores"]],
                            "Physical Memory": [
                                "{: .2f}".format(
                                    float(host["totalPhysMemBytes"])
                                    / 1024
                                    / 1024
                                    / 1024
                                )
                            ],
                            "Health Status": [host["entityStatus"]],
                            "Distribution": [os_version],
                        }
                    ),
                    ignore_index=True,
                )
            for role in host["roleRefs"]:
                if re.search(r"\bNAMENODE\b", role["roleName"]):
                    namenodes_df = namenodes_df.append(
                        pd.DataFrame(
                            {
                                "HostName": [host["hostname"]],
                                "Cores": [host["numCores"]],
                                "Memory": [
                                    "{: .2f}".format(
                                        float(host["totalPhysMemBytes"])
                                        / 1024
                                        / 1024
                                        / 1024
                                    )
                                ],
                            }
                        ),
                        ignore_index=True,
                    )
                if re.search(r"\bDATANODE\b", role["roleName"]):
                    datanodes_df = datanodes_df.append(
                        pd.DataFrame(
                            {
                                "HostName": [host["hostname"]],
                                "Cores": [host["numCores"]],
                                "Memory": [
                                    "{: .2f}".format(
                                        float(host["totalPhysMemBytes"])
                                        / 1024
                                        / 1024
                                        / 1024
                                    )
                                ],
                            }
                        ),
                        ignore_index=True,
                    )
                if (
                    re.search(r"\bGATEWAY\b", role["roleName"])
                    and "hdfs" in role["serviceName"]
                ):
                    edgenodes_df = edgenodes_df.append(
                        pd.DataFrame(
                            {
                                "HostName": [host["hostname"]],
                                "Cores": [host["numCores"]],
                                "Memory": [
                                    "{: .2f}".format(
                                        float(host["totalPhysMemBytes"])
                                        / 1024
                                        / 1024
                                        / 1024
                                    )
                                ],
                            }
                        ),
                        ignore_index=True,
                    )
                if "GATEWAY" in role["roleName"]:
                    client_gateway_df = client_gateway_df.append(
                        pd.DataFrame({"service": [role["serviceName"]]}),
                        ignore_index=True,
                    )
        client_gateway_df.drop_duplicates(inplace=True)
        self.pdf.cell(
            150, 8, "Number of Host", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(len(all_host_data)), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Number of NameNodes", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(len(namenodes_df)), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Number of DataNodes", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(len(datanodes_df)), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Number of Edge Nodes", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(len(edgenodes_df)), 0, 1,
        )
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "Host Details : ", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Total Cores in the cluster", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(host_df["Number of cores"].sum()), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Total Memory in the cluster", 0, 0,
        )
        self.pdf.cell(
            75,
            8,
            ": {: .2f} GB".format(host_df["Physical Memory"].astype("float64").sum()),
            0,
            1,
        )
        self.pdf.set_font("Arial", "B", 12)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(65, 5, "Hostname", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Host IP", 1, 0, "C", True)
        self.pdf.cell(15, 5, "Cores", 1, 0, "C", True)
        self.pdf.cell(25, 5, "Memory", 1, 0, "C", True)
        self.pdf.cell(35, 5, "Health Status", 1, 0, "C", True)
        self.pdf.cell(55, 5, "Distribution", 1, 1, "C", True)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        self.pdf.set_font("Arial", "", 12)
        for pos in range(0, len(host_df)):
            x = self.pdf.get_x()
            y = self.pdf.get_y()
            line_width = 0
            line_width = max(
                line_width, self.pdf.get_string_width(host_df["Hostname"].iloc[pos]),
            )
            cell_y = line_width / 64.0
            line_width = max(
                line_width, self.pdf.get_string_width(host_df["Host IP"].iloc[pos]),
            )
            cell_y = max(cell_y, line_width / 29.0)
            line_width = max(
                line_width,
                self.pdf.get_string_width(str(host_df["Number of cores"].iloc[pos])),
            )
            cell_y = max(cell_y, line_width / 14.0)

            line_width = max(
                line_width,
                self.pdf.get_string_width(str(host_df["Physical Memory"].iloc[pos])),
            )
            cell_y = max(cell_y, line_width / 24.0)

            line_width = max(
                line_width,
                self.pdf.get_string_width(host_df["Health Status"].iloc[pos]),
            )
            cell_y = max(cell_y, line_width / 34.0)
            line_width = max(
                line_width,
                self.pdf.get_string_width(host_df["Distribution"].iloc[pos]),
            )
            cell_y = max(cell_y, line_width / 54.0)
            cell_y = math.ceil(cell_y)
            cell_y = max(cell_y, 1)
            cell_y = cell_y * 5
            line_width = self.pdf.get_string_width(host_df["Hostname"].iloc[pos])
            y_pos = line_width / 64.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                65,
                y_pos,
                "{}".format(host_df["Hostname"].iloc[pos]),
                1,
                "C",
                fill=True,
            )
            self.pdf.set_xy(x + 65, y)
            line_width = self.pdf.get_string_width(host_df["Host IP"].iloc[pos])
            y_pos = line_width / 29.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                30, y_pos, "{}".format(host_df["Host IP"].iloc[pos]), 1, "C", fill=True,
            )
            self.pdf.set_xy(x + 95, y)
            line_width = self.pdf.get_string_width(
                str(host_df["Number of cores"].iloc[pos])
            )
            y_pos = line_width / 14.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                15,
                y_pos,
                "{}".format(str(host_df["Number of cores"].iloc[pos])),
                1,
                "C",
                fill=True,
            )
            self.pdf.set_xy(x + 110, y)
            line_width = self.pdf.get_string_width(
                str(host_df["Physical Memory"].iloc[pos])
            )
            y_pos = line_width / 24.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                25,
                y_pos,
                "{}".format(str(host_df["Physical Memory"].iloc[pos])),
                1,
                "C",
                fill=True,
            )
            self.pdf.set_xy(x + 135, y)
            line_width = self.pdf.get_string_width(host_df["Health Status"].iloc[pos])
            y_pos = line_width / 34.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                35,
                y_pos,
                "{}".format(host_df["Health Status"].iloc[pos]),
                1,
                "C",
                fill=True,
            )
            self.pdf.set_xy(x + 170, y)
            line_width = self.pdf.get_string_width(host_df["Distribution"].iloc[pos])
            y_pos = line_width / 54.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                55,
                y_pos,
                "{}".format(host_df["Distribution"].iloc[pos]),
                1,
                "C",
                fill=True,
            )
        self.pdf.cell(230, 10, "", 0, ln=1)
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "MasterNodes Details : ", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Total Cores Assigned to All the MasterNode", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(namenodes_df["Cores"].sum()), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Total Memory Assigned to All the MasterNodes", 0, 0,
        )
        self.pdf.cell(
            75,
            8,
            ": {: .2f} GB".format(namenodes_df["Memory"].astype("float64").sum()),
            0,
            1,
        )
        if len(namenodes_df) != 0:
            self.pdf.set_font("Arial", "B", 12)
            self.pdf.set_fill_color(r=66, g=133, b=244)
            self.pdf.set_text_color(r=255, g=255, b=255)
            self.pdf.cell(100, 5, "Hostname", 1, 0, "C", True)
            self.pdf.cell(20, 5, "Cores", 1, 0, "C", True)
            self.pdf.cell(40, 5, "Memory", 1, 1, "C", True)
            self.pdf.set_text_color(r=1, g=1, b=1)
            self.pdf.set_fill_color(r=244, g=244, b=244)
            self.pdf.set_font("Arial", "", 12)
            for pos in range(0, len(namenodes_df)):
                self.pdf.cell(
                    100,
                    5,
                    "{}".format(namenodes_df["HostName"].iloc[pos]),
                    1,
                    0,
                    "C",
                    True,
                )
                self.pdf.cell(
                    20, 5, "{}".format(namenodes_df["Cores"].iloc[pos]), 1, 0, "C", True
                )
                self.pdf.cell(
                    40,
                    5,
                    "{} GB".format(namenodes_df["Memory"].iloc[pos]),
                    1,
                    1,
                    "C",
                    True,
                )
        self.pdf.cell(230, 10, "", 0, ln=1)
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "DataNodes Details : ", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Total Cores Assigned to All the DataNode", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(datanodes_df["Cores"].sum()), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Total Memory Assigned to All the DataNodes", 0, 0,
        )
        self.pdf.cell(
            75,
            8,
            ": {: .2f} GB".format(datanodes_df["Memory"].astype("float64").sum()),
            0,
            1,
        )
        if len(datanodes_df) != 0:
            self.pdf.set_font("Arial", "B", 12)
            self.pdf.set_fill_color(r=66, g=133, b=244)
            self.pdf.set_text_color(r=255, g=255, b=255)
            self.pdf.cell(100, 5, "Hostname", 1, 0, "C", True)
            self.pdf.cell(20, 5, "Cores", 1, 0, "C", True)
            self.pdf.cell(40, 5, "Memory", 1, 1, "C", True)
            self.pdf.set_text_color(r=1, g=1, b=1)
            self.pdf.set_fill_color(r=244, g=244, b=244)
            self.pdf.set_font("Arial", "", 12)
            for pos in range(0, len(datanodes_df)):
                self.pdf.cell(
                    100,
                    5,
                    "{}".format(datanodes_df["HostName"].iloc[pos]),
                    1,
                    0,
                    "C",
                    True,
                )
                self.pdf.cell(
                    20, 5, "{}".format(datanodes_df["Cores"].iloc[pos]), 1, 0, "C", True
                )
                self.pdf.cell(
                    40,
                    5,
                    "{} GB".format(datanodes_df["Memory"].iloc[pos]),
                    1,
                    1,
                    "C",
                    True,
                )
        self.pdf.cell(230, 10, "", 0, ln=1)
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "EdgeNodes Details : ", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Total Cores Assigned to All the EdgeNode", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(edgenodes_df["Cores"].sum()), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Total Memory Assigned to All the EdgeNodes", 0, 0,
        )
        self.pdf.cell(
            75,
            8,
            ": {: .2f} GB".format(edgenodes_df["Memory"].astype("float64").sum()),
            0,
            1,
        )
        if len(edgenodes_df) != 0:
            self.pdf.set_font("Arial", "B", 12)
            self.pdf.set_fill_color(r=66, g=133, b=244)
            self.pdf.set_text_color(r=255, g=255, b=255)
            self.pdf.cell(100, 5, "Hostname", 1, 0, "C", True)
            self.pdf.cell(20, 5, "Cores", 1, 0, "C", True)
            self.pdf.cell(40, 5, "Memory", 1, 1, "C", True)
            self.pdf.set_text_color(r=1, g=1, b=1)
            self.pdf.set_fill_color(r=244, g=244, b=244)
            self.pdf.set_font("Arial", "", 12)
            for pos in range(0, len(edgenodes_df)):
                self.pdf.cell(
                    100,
                    5,
                    "{}".format(edgenodes_df["HostName"].iloc[pos]),
                    1,
                    0,
                    "C",
                    True,
                )
                self.pdf.cell(
                    20, 5, "{}".format(edgenodes_df["Cores"].iloc[pos]), 1, 0, "C", True
                )
                self.pdf.cell(
                    40,
                    5,
                    "{} GB".format(edgenodes_df["Memory"].iloc[pos]),
                    1,
                    1,
                    "C",
                    True,
                )
        self.pdf.cell(230, 10, "", 0, ln=1)
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "Clients Installed on Gateway : ", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_font("Arial", "B", 12)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(70, 5, "Services", 1, 1, "C", True)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        self.pdf.set_font("Arial", "", 12)
        for pos in range(0, len(client_gateway_df)):
            self.pdf.cell(
                70,
                5,
                "{}".format(client_gateway_df["service"].iloc[pos]),
                1,
                1,
                "C",
                True,
            )
        self.pdf.cell(230, 10, "", 0, ln=1)

    def clusterServiceInfo(self, cluster_service_item):
        """Add service installed data in PDF.

        Args:
            cluster_service_item (dict): All services installed in cluster
        """

        service_df = pd.DataFrame(
            columns=["Service Name", "Health Status", "Health Concerns"]
        )
        for i, k in enumerate(cluster_service_item):
            if k["serviceState"] != "STARTED":
                continue
            concerns = ""
            if k["entityStatus"] != "GOOD_HEALTH":
                for l in k["healthChecks"]:
                    if l["summary"] != "GOOD":
                        if concerns == "":
                            concerns = l["name"]
                        else:
                            concerns = concerns + "\n" + l["name"]
            service_df = service_df.append(
                pd.DataFrame(
                    {
                        "Service Name": [k["name"]],
                        "Health Status": [k["entityStatus"]],
                        "Health Concerns": [concerns],
                    }
                ),
                ignore_index=True,
            )
        self.pdf.set_font("Arial", "", 12)
        self.pdf.cell(230, 3, "", 0, ln=1)
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "Services Running in the Cluster : ", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_font("Arial", "B", 12)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(60, 5, "Service Name", 1, 0, "C", True)
        self.pdf.cell(60, 5, "Health Status", 1, 0, "C", True)
        self.pdf.cell(90, 5, "Health Concerns", 1, 1, "C", True)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        self.pdf.set_font("Arial", "", 12)
        for pos in range(0, len(service_df)):
            x = self.pdf.get_x()
            y = self.pdf.get_y()
            line_width = 0
            line_width = max(
                line_width,
                self.pdf.get_string_width(service_df["Service Name"].iloc[pos]),
            )
            cell_y = line_width / 59.0
            line_width = max(
                line_width,
                self.pdf.get_string_width(service_df["Health Status"].iloc[pos]),
            )
            cell_y = max(cell_y, line_width / 59.0)
            line_width = max(
                line_width,
                self.pdf.get_string_width(service_df["Health Concerns"].iloc[pos]),
            )
            cell_y = max(cell_y, line_width / 89.0)
            cell_y = math.ceil(cell_y)
            cell_y = max(cell_y, 1)
            cell_y = cell_y * 5
            line_width = self.pdf.get_string_width(service_df["Service Name"].iloc[pos])
            y_pos = line_width / 59.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                60,
                y_pos,
                "{}".format(service_df["Service Name"].iloc[pos]),
                1,
                "C",
                fill=True,
            )
            self.pdf.set_xy(x + 60, y)
            line_width = self.pdf.get_string_width(
                service_df["Health Status"].iloc[pos]
            )
            y_pos = line_width / 59.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                60,
                y_pos,
                "{}".format(service_df["Health Status"].iloc[pos]),
                1,
                "C",
                fill=True,
            )
            self.pdf.set_xy(x + 120, y)
            line_width = self.pdf.get_string_width(
                service_df["Health Concerns"].iloc[pos]
            )
            y_pos = line_width / 89.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                90,
                y_pos,
                "{}".format(service_df["Health Concerns"].iloc[pos]),
                1,
                "C",
                fill=True,
            )
        self.pdf.cell(230, 10, "", 0, ln=1)

    def clusterVcoreAvg(self, cluster_cpu_usage_avg):
        """Add average vcore utilization of cluster in PDF.

        Args:
            cluster_cpu_usage_avg (float): Average CPU usage in cluster
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Average Cluster CPU Utilization", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {: .2f} %".format(cluster_cpu_usage_avg), 0, 1,
        )

    def clusterVcorePlot(self, cluster_total_cores_df, cluster_cpu_usage_df):
        """Add cluster vcore data graph in PDF.

        Args:
            cluster_total_cores_df (DataFrame): Total cores available over time.
            cluster_cpu_usage_df (DataFrame): CPU usage over time
        """

        plt.figure()
        cluster_total_cores_plot = cluster_total_cores_df["Mean"].plot(
            color="steelblue", label="Available Cores"
        )
        cluster_total_cores_plot.set_ylabel("Total CPU Cores")
        cluster_total_cores_plot.legend()
        plt.title("Cluster Vcore Availability")
        plt.savefig("cluster_total_cores_plot.png")
        self.pdf.image(
            "cluster_total_cores_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )
        plt.figure()
        cluster_cpu_usage_plot = cluster_cpu_usage_df["Max"].plot(
            color="red", linestyle="--", label="Max Core Allocated", linewidth=1
        )
        cluster_cpu_usage_plot = cluster_cpu_usage_df["Mean"].plot(
            color="steelblue", label="Mean Cores Allocated"
        )
        cluster_cpu_usage_plot = cluster_cpu_usage_df["Min"].plot(
            color="lime", linestyle="--", label="Min Cores Allocated", linewidth=1
        )
        cluster_cpu_usage_plot.legend()
        cluster_cpu_usage_plot.set_ylabel("CPU Utilization %")
        plt.title("Cluster Vcore Usage")
        plt.savefig("cluster_cpu_usage_plot.png")
        self.pdf.image(
            "cluster_cpu_usage_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def clusterMemoryAvg(self, cluster_memory_usage_avg):
        """Add average memory utilization of cluster in PDF.

        Args:
            cluster_memory_usage_avg (float): Average memory usage in cluster
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Average Cluster Memory Utilization", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {: .2f} GB".format(cluster_memory_usage_avg), 0, 1,
        )

    def clusterMemoryPlot(self, cluster_total_memory_df, cluster_memory_usage_df):
        """Add cluster memory data graph in PDF.

        Args:
            cluster_total_memory_df (DataFrame): Total memory available over time
            cluster_memory_usage_df (DataFrame): Memory usage over time
        """

        plt.figure()
        cluster_total_memory_plot = cluster_total_memory_df["Mean"].plot(
            color="steelblue", label="Avaliable Memory"
        )
        cluster_total_memory_plot.set_ylabel("Total Memory(GB)")
        cluster_total_memory_plot.legend()
        plt.title("Cluster Memory Availability")
        plt.savefig("cluster_total_memory_plot.png")
        self.pdf.image(
            "cluster_total_memory_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )
        plt.figure()
        cluster_memory_usage_plot = cluster_memory_usage_df["Mean"].plot(
            color="steelblue", label="Memory Allocated"
        )
        cluster_memory_usage_plot.legend()
        cluster_memory_usage_plot.set_ylabel("Memory Utilization %")
        plt.title("Cluster Memory Usage")
        plt.savefig("cluster_memory_usage_plot.png")
        self.pdf.image(
            "cluster_memory_usage_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def hadoopVersion(self, hadoop_major, hadoop_minor, distribution):
        """Add Hadoop version details in PDF.

        Args:
            hadoop_major (str): Hadoop major version
            hadoop_minor (str): Hadoop miror version
            distribution (str): Hadoop vendor name
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Hadoop Major Version", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(hadoop_major), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Hadoop Minor Version", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(hadoop_minor), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Hadoop Distribution", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(distribution), 0, 1,
        )

    def serviceInstalled(self, new_ref_df):
        """Add list of service installed with their versions in PDF.

        Args:
            new_ref_df (DataFrame): Services mapped with their version.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "List of Services Installed  : ", 0, ln=1)
        self.pdf.set_font("Arial", "B", 12)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(70, 5, "Name", 1, 0, "C", True)
        self.pdf.cell(70, 5, "Version", 1, 1, "C", True)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        self.pdf.set_font("Arial", "", 12)
        for pos in range(0, len(new_ref_df)):
            self.pdf.cell(
                70, 5, "{}".format(new_ref_df["name"].iloc[pos]), 1, 0, "C", True
            )
            self.pdf.cell(
                70, 5, "{}".format(new_ref_df["sub_version"].iloc[pos]), 1, 1, "C", True
            )
        self.pdf.cell(230, 10, "", 0, ln=1)

    def totalHDFSSize(self, total_storage):
        """Add HDFS configured size in PDF.

        Args:
            total_storage (float): Total storage of cluster.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Total Size Configured in the Cluster", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {: .2f} GB".format(total_storage), 0, 1,
        )

    def individualHDFSSize(self, mapped_df):
        """Add HDFS configured size for each node in PDF.

        Args:
            mapped_df (DataFrame): Storage for each node of cluster.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(230, 8, "Size Configured for each Node in the Cluster: ", 0, ln=1)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(60, 5, "Host Name", 1, 0, "C", True)
        self.pdf.cell(50, 5, "Storage Capacity", 1, 1, "C", True)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        self.pdf.set_font("Arial", "", 12)
        for pos in range(0, len(mapped_df)):
            x = self.pdf.get_x()
            y = self.pdf.get_y()
            if y > 300:
                self.pdf.add_page()
                x = self.pdf.get_x()
                y = self.pdf.get_y()
            line_width = 0
            line_width = max(
                line_width, self.pdf.get_string_width(mapped_df["Hostname"].iloc[pos])
            )
            cell_y = line_width / 59.0
            line_width = max(
                line_width,
                self.pdf.get_string_width(mapped_df["Configured_Capacity"].iloc[pos]),
            )
            cell_y = max(cell_y, line_width / 49.0)
            cell_y = line_width / 49.0
            cell_y = math.ceil(cell_y)
            cell_y = max(cell_y, 1)
            cell_y = cell_y * 5
            line_width = self.pdf.get_string_width(mapped_df["Hostname"].iloc[pos])
            y_pos = line_width / 59.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                60,
                y_pos,
                "{}".format(mapped_df["Hostname"].iloc[pos]),
                1,
                "C",
                fill=True,
            )
            self.pdf.set_xy(x + 60, y)
            line_width = self.pdf.get_string_width(
                mapped_df["Configured_Capacity"].iloc[pos]
            )
            y_pos = line_width / 49.0
            y_pos = math.ceil(y_pos)
            y_pos = max(y_pos, 1)
            y_pos = cell_y / y_pos
            self.pdf.multi_cell(
                50,
                y_pos,
                "{}".format(mapped_df["Configured_Capacity"].iloc[pos]),
                1,
                "C",
                fill=True,
            )
        self.pdf.cell(230, 10, "", 0, ln=1)

    def repFactor(self, replication_factor):
        """Add HDFS replication faction in PDF.

        Args:
            replication_factor (str): Replication factor value
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Replication Factor", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(replication_factor), 0, 1,
        )

    def trashInterval(self, trash_flag):
        """Add HDFS trash interval data in PDF.

        Args:
            trash_flag (str): Trash interval value
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Trash Interval Setup in the Cluster", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(trash_flag), 0, 1,
        )

    def availableHDFSStorage(self, hdfs_storage_config):
        """Add HDFS available size in PDF.

        Args:
            hdfs_storage_config (float): Average HDFS storage available
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "HDFS Storage Available", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {: .2f} GB".format(hdfs_storage_config), 0, 1,
        )

    def usedHDFSStorage(self, hdfs_storage_used):
        """Add HDFS used size in PDF.

        Args:
            hdfs_storage_used (float): Average HDFS storage used
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "HDFS Storage Used", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {: .2f} GB".format(hdfs_storage_used), 0, 1,
        )

    def HDFSStoragePlot(self, hdfs_capacity_df, hdfs_capacity_used_df):
        """Add HDFS storage size graph in PDF.

        Args:
            hdfs_capacity_df (DataFrame): HDFS storage available over time
            hdfs_capacity_used_df (DataFrame): HDFS storage used over time
        """

        plt.figure()
        hdfs_usage_plot = hdfs_capacity_df["Mean"].plot(
            color="steelblue", label="Storage Available"
        )
        hdfs_usage_plot = hdfs_capacity_used_df["Mean"].plot(
            color="darkorange", label="Storage Used"
        )
        hdfs_usage_plot.legend()
        hdfs_usage_plot.set_ylabel("HDFS Capacity(GB)")
        plt.title("HDFS Usage")
        plt.savefig("hdfs_usage_plot.png")
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(230, 3, "", 0, ln=1)
        self.pdf.image(
            "hdfs_usage_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def hiveMetaStoreDetails(self, mt_db_host, mt_db_name, mt_db_type, mt_db_port):
        """Add Hive metastore details in PDF.

        Args:
            mt_db_host (str): Metastore database host name
            mt_db_name (str): Metastore database name
            mt_db_type (str): Metastore database type
            mt_db_port (str): Metastore database port number
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "Hive Metastore Details:", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Metastore Host", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(mt_db_host), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Metastore Database", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(mt_db_type), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Metastore Database Name", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(mt_db_name), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Metastore Database Port", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(mt_db_port), 0, 1,
        )

    def hiveDetails(
        self,
        database_count,
        tables_with_partition,
        tables_without_partition,
        internal_tables,
        external_tables,
        hive_execution_engine,
    ):
        """Add Hive details in PDF.

        Args:
            database_count (int): Number of databases in hive.
            tables_with_partition (int): Number of tables with partition in hive
            tables_without_partition (int): Number of tables without partition in hive
            internal_tables (int): Number of internal tables in hive
            external_tables (int): Number of external tables in hive
            hive_execution_engine (str): Execution engine used by hive.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "Hive Details:", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Number of Databases", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(database_count), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Number of tables with partition", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(tables_with_partition), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Number of tables without partition", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(tables_without_partition), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Number of Internal Tables", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(internal_tables), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Number of External Tables", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(external_tables), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Hive Execution Engine", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(hive_execution_engine), 0, 1,
        )

    def hiveDatabasesSize(self, database_df):
        """Add Hive databases size table in PDF.

        Args:
            database_df (DataFrame): List of databases and thier size in hive.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "Hive Databases:", 0, ln=1)
        self.pdf.set_font("Arial", "B", 12)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(60, 5, "Database", 1, 0, "C", True)
        self.pdf.cell(40, 5, "Size", 1, 0, "C", True)
        self.pdf.cell(40, 5, "No. of Tables", 1, 1, "C", True)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        self.pdf.set_font("Arial", "", 12)
        for pos in range(0, len(database_df)):
            self.pdf.cell(
                60, 5, "{}".format(database_df["Database"].iloc[pos]), 1, 0, "C", True
            )
            self.pdf.cell(
                40,
                5,
                "{: .2f} GB".format(float(database_df["File_Size"].iloc[pos])),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                40, 5, "{}".format(database_df["Count"].iloc[pos]), 1, 1, "C", True
            )
        self.pdf.cell(230, 10, "", 0, ln=1)

    def hiveAccessFrequency(self, table_df):
        """Add Hive access frequency graph in PDF.

        Args:
            table_df (DataFrame): List of tables and database in hive.
        """

        plt.figure()
        table_plot = table_df.plot.pie(
            y="Table Count",
            figsize=(6, 6),
            autopct="%.1f%%",
            title="Table Count By Access Frequency",
        )
        plt.savefig("table_type_count_plot.png")
        self.pdf.image(
            "table_type_count_plot.png", x=15, y=None, w=95, h=95, type="", link=""
        )

    def kerberosInfo(self, kerberos):
        """Add Kerberos details in PDF.

        Args:
            kerberos (str): Kerberos information of cluster.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Kerberos Details", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(kerberos), 0, 1,
        )

    def ADServerNameAndPort(self, ADServer):
        """Add AD server details in PDF.

        Args:
            ADServer (str): Url and port of AD server.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "AD Server Name and Port", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(ADServer), 0, 1,
        )

    def adServerBasedDN(self, Server_dn):
        """Add AD server details based on domain name details in PDF.

        Args:
            Server_dn (str): Domain name of LDAP bind parameter.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "AD Server Based DN", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(Server_dn), 0, 1,
        )

    def keytabFiles(self, keytab_files):
        """Add keytab files information in PDF.

        Args:
            keytab_files (str): Keytab files information.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Keytab Files Details", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(keytab_files), 0, 1,
        )

    def yarnVcoreTotal(self, yarn_total_vcores_count):
        """Add yarn total vcore in PDF.

        Args:
            yarn_total_vcores_count (float): Total vcores configured to yarn
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Total Yarn Vcore", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {: .2f}".format(yarn_total_vcores_count), 0, 1,
        )

    def yarnVcoreAvg(self, yarn_vcore_allocated_avg):
        """Add yarn average vcore in PDF.

        Args:
            yarn_vcore_allocated_avg (float): Average vcores allocated in cluster.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Average No. of Vcores Used", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {: .2f}".format(yarn_vcore_allocated_avg), 0, 1,
        )

    def yarnVcoreUsage(self, yarn_vcore_available_df, yarn_vcore_allocated_df):
        """Add yarn vcore usage graph in PDF.

        Args:
            yarn_vcore_available_df (DataFrame): Vcores available over time.
            yarn_vcore_allocated_df (DataFrame): Vcores allocation over time.
        """

        plt.figure()
        yarn_vcore_usage_plot = yarn_vcore_available_df["Mean"].plot(
            color="steelblue", label="Vcores Available"
        )
        yarn_vcore_usage_plot = yarn_vcore_allocated_df["Mean"].plot(
            color="darkorange", label="Vcores Allocated (Mean)"
        )
        yarn_vcore_usage_plot = yarn_vcore_allocated_df["Max"].plot(
            color="red", label="Vcores Allocated (Max)", linestyle="--", linewidth=1
        )
        yarn_vcore_usage_plot.legend()
        yarn_vcore_usage_plot.set_ylabel("Total Vcore Usage")
        plt.title("Yarn Vcore Usage")
        plt.savefig("yarn_vcore_usage_plot.png")
        self.pdf.image(
            "yarn_vcore_usage_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnVcoreSeasonality(self, yarn_vcore_allocated_pivot_df):
        """Add yarn vcore seasonality graph in PDF.

        Args:
            yarn_vcore_allocated_pivot_df (DataFrame): Seasonality of vcores allocation over time.
        """

        plt.figure()
        yarn_vcore_usage_heatmap = sns.heatmap(
            yarn_vcore_allocated_pivot_df, cmap="OrRd"
        )
        plt.title("Yarn Vcore Usage")
        plt.savefig("yarn_vcore_usage_heatmap.png")
        self.pdf.image(
            "yarn_vcore_usage_heatmap.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnMemoryTotal(self, yarn_total_memory_count):
        """Add yarn total memory in PDF.

        Args:
            yarn_total_memory_count (float): Total memory configured to yarn.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Total Yarn Memory", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {: .2f} GB".format(yarn_total_memory_count), 0, 1,
        )

    def yarnMemoryAvg(self, yarn_memory_allocated_avg):
        """Add yarn average memory in PDF.

        Args:
            yarn_memory_allocated_avg (float): Average memory allocated in cluster.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Average Yarn Memory Used", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {: .2f} MB".format(yarn_memory_allocated_avg), 0, 1,
        )

    def yarnMemoryUsage(self, yarn_memory_available_df, yarn_memory_allocated_df):
        """Add yarn memory usage graph in PDF

        Args:
            yarn_memory_available_df (DataFrame): Memory available over time.
            yarn_memory_allocated_df (DataFrame): Memory allocation over time.
        """

        plt.figure()
        yarn_memory_usage_plot = yarn_memory_available_df["Mean"].plot(
            color="steelblue", label="Memory Available"
        )
        yarn_memory_usage_plot = yarn_memory_allocated_df["Mean"].plot(
            color="darkorange", label="Memory Allocated (Mean)"
        )
        yarn_memory_usage_plot = yarn_memory_allocated_df["Max"].plot(
            color="red", label="Memory Allocated (Max)", linestyle="--", linewidth=1
        )
        yarn_memory_usage_plot.legend()
        yarn_memory_usage_plot.set_ylabel("Total Yarn Memory(MB)")
        plt.title("Yarn Memory Usage")
        plt.savefig("yarn_memory_usage_plot.png")
        self.pdf.image(
            "yarn_memory_usage_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnMemorySeasonality(self, yarn_memory_allocated_pivot_df):
        """Add yarn memory seasonality graph in PDF.

        Args:
            yarn_memory_allocated_pivot_df (DataFrame): Seasonality of memory allocation over time.
        """

        plt.figure()
        yarn_memory_usage_heatmap = sns.heatmap(
            yarn_memory_allocated_pivot_df, cmap="OrRd"
        )
        plt.title("Yarn Memory Usage")
        plt.savefig("yarn_memory_usage_heatmap.png")
        self.pdf.image(
            "yarn_memory_usage_heatmap.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnAppCount(self, app_count_df):
        """Add yarn application count table in PDF.

        Args:
            app_count_df (DataFrame): Application count in yarn.
        """

        self.pdf.set_font("Arial", "B", 12)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(40, 5, "Application Type", 1, 0, "C", True)
        self.pdf.cell(40, 5, "Status", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Count", 1, 1, "C", True)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        self.pdf.set_font("Arial", "", 12)
        for pos in range(0, len(app_count_df)):
            self.pdf.cell(
                40,
                5,
                "{}".format(app_count_df["Application Type"].iloc[pos]),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                40, 5, "{}".format(app_count_df["Status"].iloc[pos]), 1, 0, "C", True
            )
            self.pdf.cell(
                30, 5, "{}".format(app_count_df["Count"].iloc[pos]), 1, 1, "C", True
            )
        self.pdf.cell(230, 10, "", 0, ln=1)

    def yarnAppTypeStatus(self, app_type_count_df, app_status_count_df):
        """Add yarn application type and status pie chart in PDF.

        Args:
            app_type_count_df (DataFrame): Application count by type in yarn.
            app_status_count_df (DataFrame): Application count by status in yarn.
        """

        x = self.pdf.get_x()
        y = self.pdf.get_y()
        plt.figure()
        app_type_count_pie_plot = app_type_count_df.plot.pie(
            y="Count",
            figsize=(6, 6),
            autopct="%.1f%%",
            title="Yarn Application by Type",
        )
        plt.savefig("app_type_count_pie_plot.png")
        self.pdf.image(
            "app_type_count_pie_plot.png", x=15, y=None, w=95, h=95, type="", link=""
        )
        self.pdf.set_xy(x, y)
        plt.figure()
        app_status_count_pie_plot = app_status_count_df.plot.pie(
            y="Count",
            figsize=(6, 6),
            autopct="%.1f%%",
            title="Yarn Application by Status",
        )
        plt.savefig("app_status_count_pie_plot.png")
        self.pdf.image(
            "app_status_count_pie_plot.png", x=130, y=None, w=95, h=95, type="", link=""
        )

    def yarnAppVcoreMemory(self, app_vcore_df, app_memory_df):
        """Add yarn vcore and memory by application pie chart in PDF.

        Args:
            app_vcore_df (DataFrame): Vcores usage by applications
            app_memory_df (DataFrame): Memory usage by applications
        """

        x = self.pdf.get_x()
        y = self.pdf.get_y()
        plt.figure()
        app_vcore_plot = app_vcore_df.plot.pie(
            y="Vcore",
            figsize=(6, 6),
            autopct="%.1f%%",
            title="Yarn Application Vcore Usage",
        )
        plt.savefig("app_vcore_plot.png")
        self.pdf.image("app_vcore_plot.png", x=15, y=None, w=95, h=95, type="", link="")
        self.pdf.set_xy(x, y)
        plt.figure()
        app_memory_plot = app_memory_df.plot.pie(
            y="Memory",
            figsize=(6, 6),
            autopct="%.1f%%",
            title="Yarn Application Memory Usage",
        )
        plt.savefig("app_memory_plot.png")
        self.pdf.image(
            "app_memory_plot.png", x=130, y=None, w=95, h=95, type="", link=""
        )

    def yarnAppVcoreUsage(self, app_vcore_df, app_vcore_usage_df):
        """Add yarn vcore usage graph in PDF.

        Args:
            app_vcore_df (DataFrame): Vcore breakdown by application
            app_vcore_usage_df (DataFrame): Vcore usage over time
        """

        plt.figure()
        for i in app_vcore_df["Application Type"].unique():
            app_vcore_df_temp = pd.DataFrame(None)
            app_vcore_df_temp = app_vcore_df[app_vcore_df["Application Type"] == i]
            app_vcore_usage_df[i] = 0
            for index, row in app_vcore_df_temp.iterrows():
                val = (row["Launch Time"], 0)
                if val not in app_vcore_usage_df["Date"]:
                    app_vcore_usage_df.loc[len(app_vcore_usage_df)] = val
                val = (row["Finished Time"], 0)
                if val not in app_vcore_usage_df["Date"]:
                    app_vcore_usage_df.loc[len(app_vcore_usage_df)] = val
                app_vcore_usage_df.loc[
                    (app_vcore_usage_df["Date"] >= row["Launch Time"])
                    & (app_vcore_usage_df["Date"] < row["Finished Time"]),
                    i,
                ] = (
                    app_vcore_usage_df.loc[
                        (app_vcore_usage_df["Date"] >= row["Launch Time"])
                        & (app_vcore_usage_df["Date"] < row["Finished Time"])
                    ][i]
                    + row["Vcore"]
                )
            app_vcore_usage_plot = app_vcore_usage_df.set_index("Date")[i].plot(label=i)
            app_vcore_usage_df = app_vcore_usage_df.drop([i], axis=1)
        app_vcore_usage_plot.legend()
        app_vcore_usage_plot.set_ylabel("Application Vcores")
        plt.title("Vcore Breakdown By Application Type")
        plt.savefig("app_vcore_usage_plot.png")
        self.pdf.image(
            "app_vcore_usage_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnAppMemoryUsage(self, app_memory_df, app_memory_usage_df):
        """Add yarn memory usage graph in PDF.

        Args:
            app_memory_df (DataFrame): Memory breakdown by application
            app_memory_usage_df (DataFrame): Memory usage over time
        """

        plt.figure()
        for i in app_memory_df["Application Type"].unique():
            app_memory_df_temp = pd.DataFrame(None)
            app_memory_df_temp = app_memory_df[app_memory_df["Application Type"] == i]
            app_memory_usage_df[i] = 0
            for index, row in app_memory_df_temp.iterrows():
                val = (row["Launch Time"], 0)
                if val not in app_memory_usage_df["Date"]:
                    app_memory_usage_df.loc[len(app_memory_usage_df)] = val
                val = (row["Finished Time"], 0)
                if val not in app_memory_usage_df["Date"]:
                    app_memory_usage_df.loc[len(app_memory_usage_df)] = val
                app_memory_usage_df.loc[
                    (app_memory_usage_df["Date"] >= row["Launch Time"])
                    & (app_memory_usage_df["Date"] < row["Finished Time"]),
                    i,
                ] = (
                    app_memory_usage_df.loc[
                        (app_memory_usage_df["Date"] >= row["Launch Time"])
                        & (app_memory_usage_df["Date"] < row["Finished Time"])
                    ][i]
                    + row["Memory"]
                )
            app_memory_usage_plot = app_memory_usage_df.set_index("Date")[i].plot(
                label=i
            )
            app_memory_usage_df = app_memory_usage_df.drop([i], axis=1)
        app_memory_usage_plot.legend()
        app_memory_usage_plot.set_ylabel("Application Memory")
        plt.title("Memory Breakdown By Application Type")
        plt.savefig("app_memory_usage_plot.png")
        self.pdf.image(
            "app_memory_usage_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnBurstyAppTime(self, bursty_app_time_df):
        """Add yarn bursty application details in PDF.

        Args:
            bursty_app_time_df (DataFrame): Time taken by bursty application.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(230, 8, "Bursty Applications - Elapsed Time", 0, ln=1)
        self.pdf.set_font("Arial", "B", 12)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(110, 5, "Application Name", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Min Time", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Mean Time", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Max Time", 1, 1, "C", True)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        self.pdf.set_font("Arial", "", 12)
        for pos in range(0, len(bursty_app_time_df)):
            self.pdf.cell(
                110,
                5,
                "{}".format(bursty_app_time_df["Application Name"].iloc[pos]),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                30,
                5,
                "{: .0f} sec".format(bursty_app_time_df["Min"].iloc[pos]),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                30,
                5,
                "{: .0f} sec".format(bursty_app_time_df["Mean"].iloc[pos]),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                30,
                5,
                "{: .0f} sec".format(bursty_app_time_df["Max"].iloc[pos]),
                1,
                1,
                "C",
                True,
            )
        self.pdf.cell(230, 10, "", 0, ln=1)
        plt.figure()
        bursty_app_time_df = bursty_app_time_df.set_index("Application Name")
        bursty_app_time_plot = bursty_app_time_df.plot.barh(stacked=True).legend(
            loc="upper center", ncol=3
        )
        plt.title("Bursty Applications - Elapsed Time")
        plt.xlabel("Time(secs)")
        plt.ylabel("Applications")
        plt.tight_layout()
        plt.savefig("bursty_app_time_plot.png")
        self.pdf.image(
            "bursty_app_time_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnBurstyAppVcore(self, bursty_app_vcore_df):
        """Add yarn bursty application vcore graph in PDF.

        Args:
            bursty_app_vcore_df (DataFrame): Vcores taken by bursty application.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(230, 8, "Bursty Applications - Vcore Seconds", 0, ln=1)
        self.pdf.set_font("Arial", "B", 11)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(110, 5, "Application Name", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Min Time", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Mean Time", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Max Time", 1, 1, "C", True)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        self.pdf.set_font("Arial", "", 12)
        for pos in range(0, len(bursty_app_vcore_df)):
            self.pdf.cell(
                110,
                5,
                "{}".format(bursty_app_vcore_df["Application Name"].iloc[pos]),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                30,
                5,
                "{: .0f} sec".format(bursty_app_vcore_df["Min"].iloc[pos]),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                30,
                5,
                "{: .0f} sec".format(bursty_app_vcore_df["Mean"].iloc[pos]),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                30,
                5,
                "{: .0f} sec".format(bursty_app_vcore_df["Max"].iloc[pos]),
                1,
                1,
                "C",
                True,
            )
        self.pdf.cell(230, 10, "", 0, ln=1)
        plt.figure()
        bursty_app_vcore_df = bursty_app_vcore_df.set_index("Application Name")
        bursty_app_vcore_plot = bursty_app_vcore_df.plot.barh(stacked=True).legend(
            loc="upper center", ncol=3
        )
        plt.title("Bursty Applications - Vcore Seconds")
        plt.xlabel("Time(secs)")
        plt.ylabel("Applications")
        plt.tight_layout()
        plt.savefig("bursty_app_vcore_plot.png")
        self.pdf.image(
            "bursty_app_vcore_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarBurstyAppMemory(self, bursty_app_mem_df):
        """Add yarn bursty application memory graph in PDF.

        Args:
            bursty_app_time_df (DataFrame): Time taken by bursty application.
            bursty_app_vcore_df (DataFrame): Vcores taken by bursty application.
            bursty_app_mem_df (DataFrame): Memory taken by bursty application.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(230, 8, "Bursty Applications - Memory Seconds", 0, ln=1)
        self.pdf.set_font("Arial", "B", 12)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(110, 5, "Application Name", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Min Time", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Mean Time", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Max Time", 1, 1, "C", True)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        self.pdf.set_font("Arial", "", 12)
        for pos in range(0, len(bursty_app_mem_df)):
            self.pdf.cell(
                110,
                5,
                "{}".format(bursty_app_mem_df["Application Name"].iloc[pos]),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                30,
                5,
                "{: .0f} sec".format(bursty_app_mem_df["Min"].iloc[pos]),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                30,
                5,
                "{: .0f} sec".format(bursty_app_mem_df["Mean"].iloc[pos]),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                30,
                5,
                "{: .0f} sec".format(bursty_app_mem_df["Max"].iloc[pos]),
                1,
                1,
                "C",
                True,
            )
        self.pdf.cell(230, 10, "", 0, ln=1)
        plt.figure()
        bursty_app_mem_df = bursty_app_mem_df.set_index("Application Name")
        bursty_app_mem_plot = bursty_app_mem_df.plot.barh(stacked=True).legend(
            loc="upper center", ncol=3
        )
        plt.title("Bursty Applications - Memory Seconds")
        plt.xlabel("Memory Seconds")
        plt.ylabel("Applications")
        plt.tight_layout()
        plt.savefig("bursty_app_mem_plot.png")
        self.pdf.image(
            "bursty_app_mem_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnFailedApp(self, yarn_failed_app):
        """Add failed or killed yarn application in PDF.

        Args:
            yarn_failed_app (DataFrame): RCA of failed or killed application.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Run time of Failed/Killed Applications", 0, 0,
        )
        self.pdf.cell(
            75,
            8,
            ": {: .2f} seconds".format(yarn_failed_app["ElapsedTime"].sum()),
            0,
            1,
        )
        self.pdf.cell(
            150, 8, "Vcores Seconds Used by Failed/Killed Applications", 0, 0,
        )
        self.pdf.cell(
            75,
            8,
            ": {: .2f} seconds".format(yarn_failed_app["MemorySeconds"].sum()),
            0,
            1,
        )
        self.pdf.cell(
            150, 8, "Memory Seconds Used Failed/Killed Applications", 0, 0,
        )
        self.pdf.cell(
            75,
            8,
            ": {: .2f} seconds".format(yarn_failed_app["VcoreSeconds"].sum()),
            0,
            1,
        )
        if yarn_failed_app.size != 0:
            yarn_failed_app = yarn_failed_app.head(10)
            self.pdf.set_font("Arial", "", 12)
            self.pdf.cell(230, 3, "", 0, ln=1)
            self.pdf.cell(
                230, 8, "Top long running failed application diagnostics : ", 0, ln=1
            )
            self.pdf.set_font("Arial", "B", 12)
            self.pdf.set_fill_color(r=66, g=133, b=244)
            self.pdf.set_text_color(r=255, g=255, b=255)
            self.pdf.cell(40, 5, "App Id", 1, 0, "C", True)
            self.pdf.cell(30, 5, "Final Status", 1, 0, "C", True)
            self.pdf.cell(30, 5, "Elapsed Time", 1, 0, "C", True)
            self.pdf.cell(130, 5, "Diagnostics", 1, 1, "C", True)
            self.pdf.set_text_color(r=1, g=1, b=1)
            self.pdf.set_fill_color(r=244, g=244, b=244)
            self.pdf.set_font("Arial", "", 12)
            for pos in range(0, len(yarn_failed_app)):
                x = self.pdf.get_x()
                y = self.pdf.get_y()
                diag = yarn_failed_app["Diagnostics"].iloc[pos][:300]
                line_width = 0
                line_width = max(
                    line_width,
                    self.pdf.get_string_width(
                        yarn_failed_app["ApplicationId"].iloc[pos]
                    ),
                )
                cell_y = line_width / 39.0
                line_width = max(
                    line_width,
                    self.pdf.get_string_width(yarn_failed_app["FinalStatus"].iloc[pos]),
                )
                cell_y = max(cell_y, line_width / 29.0)
                line_width = max(
                    line_width,
                    self.pdf.get_string_width(
                        str(yarn_failed_app["ElapsedTime"].iloc[pos])
                    ),
                )
                cell_y = max(cell_y, line_width / 29.0)
                line_width = max(line_width, self.pdf.get_string_width(diag))
                cell_y = max(cell_y, line_width / 129.0)
                cell_y = math.ceil(cell_y)
                cell_y = max(cell_y, 1)
                cell_y = cell_y * 5
                line_width = self.pdf.get_string_width(
                    yarn_failed_app["ApplicationId"].iloc[pos]
                )
                y_pos = line_width / 39.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos, 1)
                y_pos = cell_y / y_pos
                self.pdf.multi_cell(
                    40,
                    y_pos,
                    "{}".format(yarn_failed_app["ApplicationId"].iloc[pos]),
                    1,
                    "C",
                    fill=True,
                )
                self.pdf.set_xy(x + 40, y)
                line_width = self.pdf.get_string_width(
                    yarn_failed_app["FinalStatus"].iloc[pos]
                )
                y_pos = line_width / 29.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos, 1)
                y_pos = cell_y / y_pos
                self.pdf.multi_cell(
                    30,
                    y_pos,
                    "{}".format(yarn_failed_app["FinalStatus"].iloc[pos]),
                    1,
                    "C",
                    fill=True,
                )
                self.pdf.set_xy(x + 70, y)
                line_width = self.pdf.get_string_width(
                    str(yarn_failed_app["ElapsedTime"].iloc[pos])
                )
                y_pos = line_width / 29.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos, 1)
                y_pos = cell_y / y_pos
                self.pdf.multi_cell(
                    30,
                    y_pos,
                    "{}".format(yarn_failed_app["ElapsedTime"].iloc[pos]),
                    1,
                    "C",
                    fill=True,
                )
                self.pdf.set_xy(x + 100, y)
                line_width = self.pdf.get_string_width(diag)
                y_pos = line_width / 129.0
                y_pos = math.ceil(y_pos)
                y_pos = max(y_pos, 1)
                y_pos = cell_y / y_pos
                self.pdf.multi_cell(130, y_pos, "{}".format(diag), 1, "C", fill=True)
        self.pdf.cell(230, 10, "", 0, ln=1)

    def yarnQueue(self, yarn_queues_list):
        """Add yarn queue details in PDF.

        Args:
            yarn_queues_list (list): Yarn queue details
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)

        def yarn_queue(yarn_queues_list, count):
            for queue in yarn_queues_list:
                if "queues" in queue:
                    self.pdf.cell(10 * count, 5, "", 0, 0)
                    self.pdf.cell(
                        30,
                        5,
                        "|-- {} - (Absolute Capacity - {}, Max Capacity - {})".format(
                            queue["queueName"],
                            queue["absoluteCapacity"],
                            queue["absoluteMaxCapacity"],
                        ),
                        0,
                        ln=1,
                    )
                    yarn_queue(queue["queues"]["queue"], count + 1)
                else:
                    self.pdf.cell(10 * count, 5, "", 0, 0)
                    self.pdf.cell(
                        30,
                        5,
                        "|-- {} - (Absolute Capacity - {}, Max Capacity - {})".format(
                            queue["queueName"],
                            queue["absoluteCapacity"],
                            queue["absoluteMaxCapacity"],
                        ),
                        0,
                        ln=1,
                    )

        self.pdf.cell(230, 5, "Queue Structure : ", 0, ln=1)
        self.pdf.cell(
            230, 5, "Root - (Absolute Capacity - 100, Max Capacity - 100)", 0, ln=1
        )
        yarn_queue(yarn_queues_list, 1)
        self.pdf.cell(230, 10, "", 0, ln=1)

    def yarnQueueApp(self, queue_app_count_df, queue_elapsed_time_df):
        """Add yarn queued application count pie chart in PDF.

        Args:
            queue_app_count_df (DataFrame): Queued application count
            queue_elapsed_time_df (DataFrame): Queued application elapsed time
        """

        def make_autopct(values):
            def my_autopct(pct):
                total = sum(values)
                val = int(round(pct * total / 100.0))
                return "{p:.2f}%  ({v:d})".format(p=pct, v=val)

            return my_autopct

        plt.figure()
        x = self.pdf.get_x()
        y = self.pdf.get_y()
        queue_app_count_plot = queue_app_count_df.plot.pie(
            y="Application Count",
            figsize=(6, 6),
            autopct=make_autopct(queue_app_count_df["Application Count"]),
            title="Queue Application Count (Weekly)",
        )
        plt.savefig("queue_app_count_plot.png")
        self.pdf.image(
            "queue_app_count_plot.png", x=15, y=None, w=95, h=95, type="", link=""
        )
        self.pdf.set_xy(x, y)
        plt.figure()
        queue_elapsed_time_plot = queue_elapsed_time_df.plot.pie(
            y="Elapsed Time",
            figsize=(6, 6),
            autopct="%.1f%%",
            title="Queue Elapsed Time (Weekly)",
        )
        plt.savefig("queue_elapsed_time_plot.png")
        self.pdf.image(
            "queue_elapsed_time_plot.png", x=130, y=None, w=95, h=95, type="", link=""
        )

    def yarnQueueVcore(self, queue_vcore_df, queue_vcore_usage_df):
        """Add yarn queued application vcore graph in PDF.

        Args:
            queue_vcore_df (DataFrame): Queue vcores details
            queue_vcore_usage_df (DataFrame): Queue vcores usage over time
        """

        plt.figure()
        for i in queue_vcore_df["Queue"].unique():
            queue_vcore_df_temp = pd.DataFrame(None)
            queue_vcore_df_temp = queue_vcore_df[queue_vcore_df["Queue"] == i]
            queue_vcore_usage_df[i] = 0
            for index, row in queue_vcore_df_temp.iterrows():
                val = (row["Launch Time"], 0)
                if val not in queue_vcore_usage_df["Date"]:
                    queue_vcore_usage_df.loc[len(queue_vcore_usage_df)] = val
                val = (row["Finished Time"], 0)
                if val not in queue_vcore_usage_df["Date"]:
                    queue_vcore_usage_df.loc[len(queue_vcore_usage_df)] = val
                queue_vcore_usage_df.loc[
                    (queue_vcore_usage_df["Date"] >= row["Launch Time"])
                    & (queue_vcore_usage_df["Date"] < row["Finished Time"]),
                    i,
                ] = (
                    queue_vcore_usage_df.loc[
                        (queue_vcore_usage_df["Date"] >= row["Launch Time"])
                        & (queue_vcore_usage_df["Date"] < row["Finished Time"])
                    ][i]
                    + row["Vcore"]
                )
            queue_vcore_usage_plot = queue_vcore_usage_df.set_index("Date")[i].plot(
                label=i
            )
            queue_vcore_usage_df = queue_vcore_usage_df.drop([i], axis=1)
        queue_vcore_usage_plot.legend()
        queue_vcore_usage_plot.set_ylabel("Application Vcores")
        plt.title("Vcore Breakdown By Queue")
        plt.savefig("queue_vcore_usage_plot.png")
        self.pdf.image(
            "queue_vcore_usage_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnQueueMemory(self, queue_memory_df, queue_memory_usage_df):
        """Add yarn queued application memory graph in PDF.

        Args:
            queue_memory_df (DataFrame): Queue memory details
            queue_memory_usage_df (DataFrame): Queue memory usage over time
        """

        plt.figure()
        for i in queue_memory_df["Queue"].unique():
            queue_memory_df_temp = pd.DataFrame(None)
            queue_memory_df_temp = queue_memory_df[queue_memory_df["Queue"] == i]
            queue_memory_usage_df[i] = 0
            for index, row in queue_memory_df_temp.iterrows():
                val = (row["Launch Time"], 0)
                if val not in queue_memory_usage_df["Date"]:
                    queue_memory_usage_df.loc[len(queue_memory_usage_df)] = val
                val = (row["Finished Time"], 0)
                if val not in queue_memory_usage_df["Date"]:
                    queue_memory_usage_df.loc[len(queue_memory_usage_df)] = val
                queue_memory_usage_df.loc[
                    (queue_memory_usage_df["Date"] >= row["Launch Time"])
                    & (queue_memory_usage_df["Date"] < row["Finished Time"]),
                    i,
                ] = (
                    queue_memory_usage_df.loc[
                        (queue_memory_usage_df["Date"] >= row["Launch Time"])
                        & (queue_memory_usage_df["Date"] < row["Finished Time"])
                    ][i]
                    + row["Memory"]
                )
            queue_memory_usage_plot = queue_memory_usage_df.set_index("Date")[i].plot(
                label=i
            )
            queue_memory_usage_df = queue_memory_usage_df.drop([i], axis=1)
        queue_memory_usage_plot.legend()
        queue_memory_usage_plot.set_ylabel("Application Memory")
        plt.title("Memory Breakdown By Queue")
        plt.savefig("queue_memory_usage_plot.png")
        self.pdf.image(
            "queue_memory_usage_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnQueuePendingApp(self, app_queue_df, app_queue_usage_df):
        """Add yarn pending queued application graph in PDF.

        Args:
            app_queue_df (DataFrame): Pending queued application list
            app_queue_usage_df (DataFrame): Pending queued application usage over time.
        """

        plt.figure()
        for i in app_queue_df["Queue"].unique():
            app_queue_df_temp = pd.DataFrame(None)
            app_queue_df_temp = app_queue_df[
                (app_queue_df["Queue"] == i)
                & (app_queue_df["Wait Time"] > timedelta(minutes=5))
            ]
            app_queue_usage_df[i] = 0
            for index, row in app_queue_df_temp.iterrows():
                val = (row["Start Time"], 0)
                if val not in app_queue_usage_df["Date"]:
                    app_queue_usage_df.loc[len(app_queue_usage_df)] = val
                val = (row["Launch Time"], 0)
                if val not in app_queue_usage_df["Date"]:
                    app_queue_usage_df.loc[len(app_queue_usage_df)] = val
                app_queue_usage_df.loc[
                    (app_queue_usage_df["Date"] >= row["Start Time"])
                    & (app_queue_usage_df["Date"] < row["Launch Time"]),
                    i,
                ] = (
                    app_queue_usage_df.loc[
                        (app_queue_usage_df["Date"] >= row["Start Time"])
                        & (app_queue_usage_df["Date"] < row["Launch Time"])
                    ][i]
                    + 1
                )
            app_queue_usage_plot = app_queue_usage_df.set_index("Date")[i].plot(label=i)
            app_queue_usage_df = app_queue_usage_df.drop([i], axis=1)
        app_queue_usage_plot.legend()
        app_queue_usage_plot.set_ylabel("Application Count")
        plt.title("Application Pending by Queue")
        plt.savefig("app_queue_usage_plot.png")
        self.pdf.image(
            "app_queue_usage_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnPendingApp(self, yarn_pending_apps_df):
        """Add yarn pending application count graph in PDF.

        Args:
            yarn_pending_apps_df (DataFrame): Pending application count over time.
        """

        plt.figure()
        yarn_pending_apps_plot = yarn_pending_apps_df["Max"].plot(
            color="steelblue", label="Pending Applications"
        )
        yarn_pending_apps_plot.legend()
        yarn_pending_apps_plot.set_ylabel("Application Count")
        plt.title("Total Pending Applications Across YARN Pools")
        plt.savefig("yarn_pending_apps_plot.png")
        self.pdf.image(
            "yarn_pending_apps_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnPendingVcore(self, yarn_pending_vcore_df):
        """Add yarn pending application vcore graph in PDF.

        Args:
            yarn_pending_vcore_df (DataFrame): Pending vcores over time.
        """

        plt.figure()
        yarn_pending_vcore_plot = yarn_pending_vcore_df["Mean"].plot(
            color="steelblue", label="Pending Vcores"
        )
        yarn_pending_vcore_plot.legend()
        yarn_pending_vcore_plot.set_ylabel("Vcores")
        plt.title("Total Pending VCores Across YARN Pools")
        plt.savefig("yarn_pending_vcore_plot.png")
        self.pdf.image(
            "yarn_pending_vcore_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def yarnPendingMemory(self, yarn_pending_memory_df):
        """Add yarn pending application memory graph in PDF.

        Args:
            yarn_pending_memory_df (DataFrame): Pending memory over time.
        """

        plt.figure()
        yarn_pending_memory_plot = yarn_pending_memory_df["Mean"].plot(
            color="steelblue", label="Pending Memory"
        )
        yarn_pending_memory_plot.legend()
        yarn_pending_memory_plot.set_ylabel("Memory (MB)")
        plt.title("Total Pending Memory Across YARN Pools")
        plt.savefig("yarn_pending_memory_plot.png")
        self.pdf.image(
            "yarn_pending_memory_plot.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    def hbaseStorage(self, base_size, disk_space_consumed):
        """Add HBase storage details in PDF.

        Args:
            base_size (float) : Base size of HBase
            disk_space_consumed (float) : Disk size consumed by HBase
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Base Size of Data", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(base_size), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Disk Space Consumed", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(disk_space_consumed), 0, 1,
        )

    def hbaseReplication(self, replication):
        """Add HBase replication factor in PDF.

        Args:
            replication (str): HBase replication factor.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Is Hbase replicated to other datacenter", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(replication), 0, 1,
        )

    def hbaseIndexing(self, indexing):
        """Add HBase secondary indexing details in PDF.

        Args:
            indexing (str): HBase secondary index value.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Do you use Secondary Index on Hbase", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(indexing), 0, 1,
        )

    def sparkVersion(self, spark_version):
        """Add Spark version details in PDF.

        Args:
            spark_version (str): Spark version
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Spark Version", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(spark_version), 0, 1,
        )

    def sparkLanguages(self, languages):
        """Add list of languages used by spark programs in PDF.

        Args:
            language_list (str): List of languages separated by comma.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Programming Languages Used By Spark Api", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(languages), 0, 1,
        )

    def sparkDynamicAllocationAndResourceManager(
        self, dynamic_allocation, spark_resource_manager
    ):
        """Add spark config details in PDF.

        Args:
            dynamic_allocation (str): Dynamic Allocation value.
            spark_resource_manager (str): Spark resource manager value.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Spark Resource Manager", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(spark_resource_manager), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Dynamic Allocation", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(dynamic_allocation), 0, 1,
        )

    def retentionPeriod(self, retention_period):
        """Add retention period of kafka in PDF.

        Args:
            retention_period (str): Kafka retention period
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Kafka Retention Period", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(retention_period), 0, 1,
        )

    def numTopics(self, num_topics):
        """Add num of topics in kafka in PDF.

        Args:
            num_topics (int): Number of topics in kafka.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Number Of Topics in Kafka", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(num_topics), 0, 1,
        )

    def msgSize(self, sum_size):
        """Add volume of message in kafka in bytes in PDF.

        Args:
            sum_size (int): Message size of Kafka
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Total Size Of Message in Kafka", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {: .2f} MB".format(sum_size), 0, 1,
        )

    def msgCount(self, sum_count):
        """Add count of messages in kafka topics in PDF.

        Args:
            sum_count (int): Number of messages in Kafka
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Total Number Of Message in Kafka", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(sum_count), 0, 1,
        )

    def clusterSizeAndBrokerSize(self, total_size, brokersize):
        """Add per cluster storage and kafka cluster storage in kafka in PDF.

        Args:
            total_size (float): Total size of kafka cluster
            brokersize (DataFrame): Size for each broker.
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            150, 8, "Total Storage of Kafka Cluster", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(total_size), 0, 1,
        )
        self.pdf.cell(
            150, 8, "Storage of Kafka Cluster Per Broker", 0, 0,
        )
        self.pdf.cell(
            75, 8, ": {}".format(brokersize), 0, 1,
        )

    def impala(self, impala):
        """Add Impala information in PDF.

        Args:
            impala (str): Impala service value
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(230, 8, "{}".format(impala), 0, ln=1)

    def sentry(self, sentry):
        """Add Sentry information in PDF.

        Args:
            impala (str): Sentry service value
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(230, 8, "{}".format(sentry), 0, ln=1)

    def kudu(self, kudu):
        """Add Kudu information in PDF.

        Args:
            impala (str): Kudu service value
        """

        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(230, 8, "{}".format(kudu), 0, ln=1)
