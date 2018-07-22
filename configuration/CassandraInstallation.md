# Setting up Cassandra

Create the VM - sizing not required yet - **replace information in brackets**

    slcli -y vs create -t Git/MIDS_W251_Benchmarking/configuration/Templates/SoftLayer/small_2disk_private.slcli --hostname=cashost1

Login:

    ssh root@<IP>

Optional: update password to something longer than the short one assigned in Softlayer:

    passwd root

Update:

    yum -y update; reboot

Copy over data from gateway, login to the gateway server and run

	scp -r /software/Cassandra root@<IP>:/tmp/
    
On the Cassandra server do:

    rpm -ivh /tmp/Cassandra/jre-8u181-linux-x64.rpm
	rpm -ivh /tmp/Cassandra/cassandra-3.11.2-1.noarch.rpm
	rpm -ivh /tmp/Cassandra/jemalloc-3.6.0-1.el7.x86_64.rpm

Clean up these temporary files:

	/bin/rm -rf /tmp/Cassandra

Update and reread the system parameters:

	echo -e "vm.max_map_count = 1048575\nvm.swappiness = 10"  >> /etc/sysctl.conf && sysctl -p

Get the 10.X.Y.Z ip address of the host and record it

	ip addr show

Edit the /etc/hosts file, comment out the lines where the hostname is define with 127.0.0.1 and ::1 and fill in the hostname with the 10.X.Y.Z ip address, e.g.
	
	#127.0.0.1 cashost1.w251.mids cashost1
	10.54.41.184    cashost1.w251.mids cashost1
	#::1 cashost1.w251.mids cashost1

Edit the /etc/cassandra/conf/cassandra.yaml file and change the following:

	- seeds: "127.0.0.1"
	to
	- seeds: "<10. ip address of server>"

	listen_address: localhost
	to
	listenaddress: <10. ip address of server>

	rpc_address: localhost
	to
	rpc_address: <10. ip address of server>
