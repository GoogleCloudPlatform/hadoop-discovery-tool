# Importing Required Libraries
from imports import *


# This Class has helper functions for pdf generation
class PdfFunctions:
    # Initialize Inputs
    def __init__(self, inputs, pdf):
        self.inputs = inputs
        self.version = inputs["version"]
        self.cloudera_manager_host_ip = inputs["cloudera_manager_host_ip"]
        self.cloudera_manager_username = inputs["cloudera_manager_username"]
        self.cloudera_manager_password = inputs["cloudera_manager_password"]
        self.cluster_name = inputs["cluster_name"]
        self.logger = inputs["logger"]
        self.pdf = pdf

    # Add cluster information in PDF
    def clusterInfo(self, cluster_items):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            8,
            "Number of Cluster Configured : {}".format(len(cluster_items)),
            0,
            ln=1,
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

    # Add detailed information of all host in cluster
    def clusterHostInfo(self, cluster_host_items, all_host_data, os_version):
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
        client_gateway_df.drop_duplicates()
        self.pdf.cell(
            230,
            8,
            "Number of Host               : {}".format(len(all_host_data)),
            0,
            ln=1,
        )
        self.pdf.cell(
            230, 8, "Number of NameNodes  : {}".format(len(namenodes_df)), 0, ln=1
        )
        self.pdf.cell(
            230, 8, "Number of DataNodes    : {}".format(len(datanodes_df)), 0, ln=1
        )
        self.pdf.cell(
            230, 8, "Number of Edge Nodes  : {} ".format(len(edgenodes_df)), 0, ln=1
        )
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "Host Details : ", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            8,
            "Total Cores in the cluster       : {} ".format(
                host_df["Number of cores"].sum()
            ),
            0,
            ln=1,
        )
        self.pdf.cell(
            230,
            8,
            "Total Memory in the cluster : {} GB  ".format(
                host_df["Physical Memory"].sum()
            ),
            0,
            ln=1,
        )
        self.pdf.set_font("Arial", "B", 12)
        self.pdf.set_fill_color(r=66, g=133, b=244)
        self.pdf.set_text_color(r=255, g=255, b=255)
        self.pdf.cell(90, 5, "Hostname", 1, 0, "C", True)
        self.pdf.cell(20, 5, "Host IP", 1, 0, "C", True)
        self.pdf.cell(15, 5, "Cores", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Memory", 1, 0, "C", True)
        self.pdf.cell(30, 5, "Health Status", 1, 0, "C", True)
        self.pdf.cell(40, 5, "Distribution", 1, 1, "C", True)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.set_fill_color(r=244, g=244, b=244)
        self.pdf.set_font("Arial", "", 12)
        for pos in range(0, len(host_df)):
            self.pdf.cell(
                90, 5, "{}".format(host_df["Hostname"].iloc[pos]), 1, 0, "C", True
            )
            self.pdf.cell(
                20, 5, "{}".format(host_df["Host IP"].iloc[pos]), 1, 0, "C", True
            )
            self.pdf.cell(
                15,
                5,
                "{}".format(host_df["Number of cores"].iloc[pos]),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                30,
                5,
                "{} GB".format(host_df["Physical Memory"].iloc[pos]),
                1,
                0,
                "C",
                True,
            )
            self.pdf.cell(
                30, 5, "{}".format(host_df["Health Status"].iloc[pos]), 1, 0, "C", True
            )
            self.pdf.cell(
                40, 5, "{}".format(host_df["Distribution"].iloc[pos]), 1, 1, "C", True
            )
        self.pdf.add_page()
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "MasterNodes Details : ", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            8,
            "Total Cores Assigned to All the MasterNode        : {} ".format(
                namenodes_df["Cores"].sum()
            ),
            0,
            ln=1,
        )
        self.pdf.cell(
            230,
            8,
            "Total Memory Assigned to All the MasterNodes : {} GB  ".format(
                namenodes_df["Memory"].sum()
            ),
            0,
            ln=1,
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
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "DataNodes Details : ", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            8,
            "Total Cores Assigned to All the DataNodes        : {} ".format(
                datanodes_df["Cores"].sum()
            ),
            0,
            ln=1,
        )
        self.pdf.cell(
            230,
            8,
            "Total Memory Assigned to All the DataNodes : {} GB  ".format(
                datanodes_df["Memory"].sum()
            ),
            0,
            ln=1,
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
        self.pdf.set_text_color(r=66, g=133, b=244)
        self.pdf.cell(230, 8, "EdgeNodes Details : ", 0, ln=1)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            8,
            "Total Cores Assigned to All the EdgeNodes       : {} ".format(
                edgenodes_df["Cores"].sum()
            ),
            0,
            ln=1,
        )
        self.pdf.cell(
            230,
            8,
            "Total Memory Assigned to All the EdgeNodes : {} GB  ".format(
                edgenodes_df["Memory"].sum()
            ),
            0,
            ln=1,
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
        self.pdf.add_page()
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

    # Add service installed data in PDF
    def clusterServiceInfo(self, cluster_service_item):
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

    # Add average vcore utilization of cluster in PDF
    def clusterVcoreAvg(self, cluster_cpu_usage_avg):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            5,
            "Average Cluster CPU Utilization is {: .2f}%".format(cluster_cpu_usage_avg),
            0,
            ln=1,
        )

    # Add cluster vcore data graph in PDF
    def clusterVcorePlot(self, cluster_total_cores_df, cluster_cpu_usage_df):
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

    # Add average memory utilization of cluster in PDF
    def clusterMemoryAvg(self, cluster_memory_usage_avg):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            5,
            "Average Cluster Memory Utilization is {: .2f}%".format(
                cluster_memory_usage_avg
            ),
            0,
            ln=1,
        )

    # Add cluster memory data graph in PDF
    def clusterMemoryPlot(self, cluster_total_memory_df, cluster_memory_usage_df):
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

    # Add Hadoop version details in PDF
    def hadoopVersion(self, hadoopVersionMajor, hadoopVersionMinor, distribution):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            8,
            "Hadoop Major Version Is     : {} ".format(hadoopVersionMajor),
            0,
            ln=1,
        )
        self.pdf.cell(
            230,
            8,
            "Hadoop Minor Version Is     : {} ".format(hadoopVersionMinor),
            0,
            ln=1,
        )
        self.pdf.cell(
            230, 8, "Hadoop Distribution Is      : {} ".format(distribution), 0, ln=1
        )

    # Add list on service installed with their verions in PDF
    def serviceInstalled(self, new_ref_df):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
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

    # Add HDFS configured size in PDF
    def totalHDFSSize(self, total_storage):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            8,
            "Total Size Configured in the Cluster: {: .2f} GB ".format(total_storage),
            0,
            ln=1,
        )

    # Add HDFS replication faction in PDF
    def repFactor(self, replication_factor):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            8,
            "Replication Factor                           : {} ".format(
                replication_factor
            ),
            0,
            ln=1,
        )

    # Add HDFS trash interval data in PDF
    def trashInterval(self, trash_flag):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            8,
            "Trash Interval Setup in the Cluster  : {} ".format(trash_flag),
            0,
            ln=1,
        )

    # Add HDFS available size in PDF
    def availableHDFSStorage(self, hdfs_storage_config):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            5,
            "HDFS Storage Available : {: .0f} GB".format(hdfs_storage_config),
            0,
            ln=1,
        )

    # Add HDFS used size in PDF
    def usedHDFSStorage(self, hdfs_storage_used):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            5,
            "HDFS Storage Used       : {: .0f} GB".format(hdfs_storage_used),
            0,
            ln=1,
        )

    # Add HDFS storage size graph in PDF
    def HDFSStoragePlot(self, hdfs_capacity_df, hdfs_capacity_used_df):
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

    # Add yarn total vcore in PDF
    def yarnVcoreTotal(self, yarn_total_vcores_count):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230, 5, "Total Yarn Vcore : {:.0f}".format(yarn_total_vcores_count), 0, ln=1
        )

    # Add yarn average vcore in PDF
    def yarnVcoreAvg(self, yarn_vcore_allocated_avg):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            5,
            "Average No. of Vcores Used : {: .2f}".format(yarn_vcore_allocated_avg),
            0,
            ln=1,
        )

    # Add yarn vcore usage graph in PDF
    def yarnVcoreUsage(self, yarn_vcore_available_df, yarn_vcore_allocated_df):
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

    # Add yarn vcore seasonality graph in PDF
    def yarnVcoreSeasonality(self, yarn_vcore_allocated_pivot_df):
        plt.figure()
        yarn_vcore_usage_heatmap = sns.heatmap(
            yarn_vcore_allocated_pivot_df, cmap="OrRd"
        )
        plt.title("Yarn Vcore Usage")
        plt.savefig("yarn_vcore_usage_heatmap.png")
        self.pdf.image(
            "yarn_vcore_usage_heatmap.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    # Add yarn total memory in PDF
    def yarnMemoryTotal(self, yarn_total_memory_count):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            5,
            "Total Yarn Memory : {:.0f} GB".format(yarn_total_memory_count),
            0,
            ln=1,
        )

    # Add yarn average memory in PDF
    def yarnMemoryAvg(self, yarn_memory_allocated_avg):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            5,
            "Average Yarn Memory Used : {:.0f} MB".format(yarn_memory_allocated_avg),
            0,
            ln=1,
        )

    # Add yarn memory usage graph in PDF
    def yarnMemoryUsage(self, yarn_memory_available_df, yarn_memory_allocated_df):
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

    # Add yarn memory seasonality graph in PDF
    def yarnMemorySeasonality(self, yarn_memory_allocated_pivot_df):
        plt.figure()
        yarn_memory_usage_heatmap = sns.heatmap(
            yarn_memory_allocated_pivot_df, cmap="OrRd"
        )
        plt.title("Yarn Memory Usage")
        plt.savefig("yarn_memory_usage_heatmap.png")
        self.pdf.image(
            "yarn_memory_usage_heatmap.png", x=0, y=None, w=250, h=85, type="", link=""
        )

    # Add yarn application count table in PDF
    def yarnAppCount(self, app_count_df):
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

    # Add yarn application type and status pie chart in PDF
    def yarnAppTypeStatus(self, app_type_count_df, app_status_count_df):
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

    # Add yarn vcore and memory by application pie chart in PDF
    def yarnAppVcoreMemory(self, app_vcore_df, app_memory_df):
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

    # Add yarn vcore usage graph in PDF
    def yarnAppVcoreUsage(self, app_vcore_df, app_vcore_usage_df):
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

    # Add yarn memory usage graph in PDF
    def yarnAppMemoryUsage(self, app_memory_df, app_memory_usage_df):
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

    # Add yarn bursty application details in PDF
    def yarnBurstyAppTime(self, bursty_app_time_df):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(230, 5, "Bursty Applications - Elapsed Time", 0, ln=1)
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

    # Add yarn bursty application vcore graph in PDF
    def yarnBurstyAppVcore(self, bursty_app_vcore_df):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(230, 5, "Bursty Applications - Vcore Seconds", 0, ln=1)
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

    # Add yarn bursty application memory graph in PDF
    def yarBurstyAppMemory(self, bursty_app_mem_df):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(230, 5, "Bursty Applications - Memory Seconds", 0, ln=1)
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

    # Add failed or killed yarn application in PDF
    def yarnFailedApp(self, yarn_failed_app):
        self.pdf.set_font("Arial", "", 12)
        self.pdf.set_text_color(r=1, g=1, b=1)
        self.pdf.cell(
            230,
            5,
            "Run time of Failed/Killed Applications = {: .2f} seconds".format(
                yarn_failed_app["ElapsedTime"].sum()
            ),
            0,
            ln=1,
        )
        self.pdf.cell(
            230,
            5,
            "Vcores Seconds Used by Failed/Killed Applications = {} seconds".format(
                yarn_failed_app["MemorySeconds"].sum()
            ),
            0,
            ln=1,
        )
        self.pdf.cell(
            230,
            5,
            "Memory Seconds Used Failed/Killed Applications = {} seconds".format(
                yarn_failed_app["VcoreSeconds"].sum()
            ),
            0,
            ln=1,
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

    # Add yarn queue details in PDF
    def yarnQueue(self, yarn_queues_list):
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

    # Add yarn queued application count pie chart in PDF
    def yarnQueueApp(self, queue_app_count_df, queue_elapsed_time_df):
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

    # Add yarn queued application vcore graph in PDF
    def yarnQueueVcore(self, queue_vcore_df, queue_vcore_usage_df):
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

    # Add yarn queued application memory graph in PDF
    def yarnQueueMemory(self, queue_memory_df, queue_memory_usage_df):
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

    # Add yarn pending queued application graph in PDF
    def yarnQueuePendingApp(self, app_queue_df, app_queue_usage_df):
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

    # Add yarn pending application count graph in PDF
    def yarnPendingApp(self, yarn_pending_apps_df):
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

    # Add yarn pending application vcore graph in PDF
    def yarnPendingVcore(self, yarn_pending_vcore_df):
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

    # Add yarn pending application memory graph in PDF
    def yarnPendingMemory(self, yarn_pending_memory_df):
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
