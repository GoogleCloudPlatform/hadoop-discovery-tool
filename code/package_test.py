import os

print("Os Dependecies Installing...")

# initialise flags to identify packages
nload_dt, gcc_dt, odbc_dt, sasl_dt1 = "", "", "", ""

# list to hold installed and non installed data
installed, not_installed = [], []

# This command will fetch os-name for ex. centos,debian,opensuse etc.
os_name = os.popen("grep PRETTY_NAME /etc/os-release").read()
os_name = os_name.lower()

"""Here based on the os of the current system respective block will execute and 
will install packages with their respective package managers.
"""

if "centos" in os_name:
    os.popen("yum install nload -y").read()
    os.popen("yum install gcc gcc-c++ -y").read()
    os.popen("yum install cyrus-sasl-devel -y").read()
    os.popen("yum install unixODBC-devel -y").read()
    nload_dt = os.popen("rpm -qa | grep nload").read()
    gcc_dt = os.popen("rpm -qa | grep gcc-c++").read()
    sasl_dt = os.popen("rpm -qa | grep cyrus-sasl-devel").read()
    odbc_dt = os.popen("rpm -qa | grep unixODBC-devel").read()
elif "debian" in os_name:
    os.popen("apt install -y nload").read()
    os.popen("apt install -y g++").read()
    os.popen("apt-get install sasl2-bin").read()
    os.popen("apt-get install -y unixodbc-dev").read()
    nload_dt = os.popen("apt list --installed | grep nload").read()
    gcc_dt = os.popen("apt list --installed | grep g++-8").read()
    sasl_dt = os.popen("apt list --installed | grep sasl2-bin").read()
    odbc_dt = os.popen("apt list --installed | grep unixodbc-dev").read()
elif "ubuntu" in os_name:
    os.popen("apt-get install -y nload").read()
    os.popen("apt install -y g++").read()
    os.popen("apt-get install sasl2-bin").read()
    os.popen("apt-get install unixodbc-dev -y").read()
    nload_dt = os.popen("apt list --installed | grep nload").read()
    gcc_dt = os.popen("apt list --installed | grep g++-7").read()
    sasl_dt = os.popen("apt list --installed | grep sasl2-bin").read()
    odbc_dt = os.popen("apt list --installed | grep unixodbc-dev").read()
elif "red hat" in os_name:
    os.popen("yum install nload -y").read()
    os.popen("yum install gcc gcc-c++ -y").read()
    os.popen("yum install cyrus-sasl-devel -y").read()
    os.popen("yum install unixODBC-devel -y").read()
    nload_dt = os.popen("rpm -qa | grep nload").read()
    gcc_dt = os.popen("rpm -qa | grep gcc-c++").read()
    sasl_dt = os.popen("rpm -qa | grep cyrus-sasl-devel").read()
    odbc_dt = os.popen("rpm -qa | grep unixODBC-devel").read()
elif "suse" in os_name:
    os.popen("zypper -n install nload").read()
    os.popen("zypper -n install gcc gcc-c++").read()
    os.popen("zypper -n install cyrus-sasl-devel").read()
    os.popen("zypper -n install unixODBC-devel").read()
    nload_dt = os.popen("rpm -qa | grep nload").read()
    gcc_dt = os.popen("rpm -qa | grep gcc-c++").read()
    sasl_dt = os.popen("rpm -qa | grep cyrus-sasl-devel").read()
    odbc_dt = os.popen("rpm -qa | grep unixODBC-devel").read()

"""Here Code will check if flags have been set then accordingly lists will be 
appened with proper data
"""

if nload_dt != "":
    installed.append(nload_dt)
else:
    not_installed.append("Nload")
if gcc_dt != "":
    installed.append(gcc_dt)
else:
    not_installed.append("GCC-C++")
if odbc_dt != "":
    installed.append(odbc_dt)
else:
    not_installed.append("Unix ODBC-Devel")
if sasl_dt != "":
    installed.append(sasl_dt)
else:
    not_installed.append("Cyrus SASL-Devel")

installed_string = ",".join(installed)
not_installed_string = ",".join(not_installed)

"""Here the code will decide based on the size of list which message to show 
to user about os packages installation
"""

if len(installed) == 0:
    print("Installed packages :- None")
else:
    print("Installed Packages " + installed_string)
if len(not_installed) == 0:
    print("Packages not Installed :- None")
else:
    print("Packages not Installed " + not_installed_string)
