import os
print("Os Dependencies Installing...")
#initialise flags to identify packages
nload_dt,vnstat_dt,gcc_dt,odbc_dt,sasl_dt1,pydevel_dt="","","","","",""
#list to hold installed and non installed data
installed,not_installed=[],[]
#This command will fetch os-name for ex. centos,debian,opensuse etc.
os_name=os.popen("grep PRETTY_NAME /etc/os-release").read()
os_name=os_name.lower()
"""
Here based on the os of the current system respective block will execute and will install packages 
with their respective package managers.
"""
try:
    if "centos" in os_name:
        os.popen('yum install nload -y').read()
        os.popen('yum install vnstat -y').read()
        os.popen('yum install gcc gcc-c++ -y').read()
        os.popen('yum install cyrus-sasl-devel -y').read()
        os.popen('yum install unixODBC-devel -y').read()
        os.popen('yum install -y python3-devel').read()
        nload_dt=os.popen('rpm -qa | grep nload').read()
        vnstat_dt=os.popen('rpm -qa | grep vnstat').read()
        gcc_dt=os.popen('rpm -qa | grep gcc-c++').read()
        sasl_dt=os.popen('rpm -qa | grep cyrus-sasl-devel').read()
        odbc_dt=os.popen('rpm -qa | grep unixODBC-devel').read()
        pydevel_dt=os.popen('rpm -qa | grep python3-devel').read()
    elif "debian" in os_name:
        os.popen('apt install -y nload').read()
        os.popen('apt-get install -y vnstat').read()
        os.popen('apt install -y g++').read()
        os.popen('apt-get install sasl2-bin').read()
        os.popen('apt-get install -y unixodbc-dev').read()
        os.popen('apt-get install -y python3-dev').read()
        nload_dt=os.popen('apt list --installed | grep nload').read()
        vnstat_dt=os.popen('apt list --installed | grep vnstat').read()
        gcc_dt=os.popen('apt list --installed | grep g++-8').read()
        sasl_dt=os.popen('apt list --installed | grep sasl2-bin').read()
        odbc_dt=os.popen('apt list --installed | grep unixodbc-dev').read()
        pydevel_dt=os.popen('apt list --installed | grep python3-dev').read()
    elif "ubuntu" in os_name:
        os.popen('apt-get install -y nload').read()
        os.popen('apt install -y vnstat').read()
        os.popen('apt install -y g++').read()
        os.popen('apt-get install sasl2-bin').read()
        os.popen('apt-get install unixodbc-dev -y').read()
        os.popen('apt-get install -y python3-dev').read()
        nload_dt=os.popen('apt list --installed | grep nload').read()
        vnstat_dt=os.popen('apt list --installed | grep vnstat').read()
        gcc_dt=os.popen('apt list --installed | grep g++-7').read()
        sasl_dt=os.popen('apt list --installed | grep sasl2-bin').read()
        odbc_dt=os.popen('apt list --installed | grep unixodbc-dev').read()
        pydevel_dt=os.popen('apt list --installed | grep python3-dev').read()
    elif "red hat" in os_name:
        os.popen('yum install nload -y').read()
        os.popen('yum install vnstat -y').read()
        os.popen('yum install gcc gcc-c++ -y').read()
        os.popen('yum install cyrus-sasl-devel -y').read()
        os.popen('yum install unixODBC-devel -y').read()
        os.popen('yum install -y python3-devel').read()
        nload_dt=os.popen('rpm -qa | grep nload').read()
        vnstat_dt=os.popen('rpm -qa | grep vnstat').read()
        gcc_dt=os.popen('rpm -qa | grep gcc-c++').read()
        sasl_dt=os.popen('rpm -qa | grep cyrus-sasl-devel').read()
        odbc_dt=os.popen('rpm -qa | grep unixODBC-devel').read()
        pydevel_dt=os.popen('rpm -qa | grep python3-devel').read()
    elif "suse" in os_name:
        os.popen('zypper -n install nload').read()
        os.popen('zypper -n install vnstat').read()
        os.popen('zypper -n install gcc gcc-c++').read()
        os.popen('zypper -n install cyrus-sasl-devel').read()
        os.popen('zypper -n install unixODBC-devel').read()
        os.popen('zypper -n install python3-devel').read()
        nload_dt=os.popen('rpm -qa | grep nload').read()
        vnstat_dt=os.popen('rpm -qa | grep vnstat').read()
        gcc_dt=os.popen('rpm -qa | grep gcc-c++').read()
        sasl_dt=os.popen('rpm -qa | grep cyrus-sasl-devel').read()
        odbc_dt=os.popen('rpm -qa | grep unixODBC-devel').read()
        pydevel_dt=os.popen('rpm -qa | grep python3-devel').read()        
except Exception as e:
    pass
#Here Code will check if flags have been set then accordingly lists will be appened with proper data    
if nload_dt != "":
    installed.append(nload_dt)
else:
    not_installed.append("Nload")
if vnstat_dt != "":
    installed.append(vnstat_dt)
else:
    not_installed.append("Vnstat")    
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
if pydevel_dt != "":
    installed.append(pydevel_dt)
else:
    not_installed.append("Python3-Devel")    
installed_string = ','.join(installed)
not_installed_string = ','.join(not_installed)
#Here the code will decide based on the size of list which message to show to user about os packages installation
if(len(installed)==0):
    print("Installed packages :- None")
else:
    print("Installed Packages "+installed_string)
if(len(not_installed)==0):
    print("Packages not Installed :- None")
else:
    print("Packages not Installed "+not_installed_string)

