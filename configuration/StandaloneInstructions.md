# Setting up Kafka and Spark on single node and pulling in tweets
Based on Paul's install guide started here: https://docs.google.com/document/d/1ycJo7ZFT63LK5kXDTUkmes5sg9uQbusUXtxm1XWBwxU/edit?ts=5b4fbf8e#

Create the VM - sizing not required yet - **replace information in brackets**

    slcli vs create --datacenter=<datacenter, i.e. sjc01> --hostname=<hostname> --domain=<domainname>
		--billing=hourly --key=<yourkey> --cpu=2 --memory=4096 --disk=100 --os=CENTOS_LATEST_64

Login:

    ssh @root<IP>

Optional: update password to something longer than the short one assigned in Softlayer:

    passwd root
Update:

    yum -y update; reboot
Install required packages, python, iptables, java:

    yum -y install https://centos7.iuscommunity.org/ius-release.rpm
    yum -y install java-1.8.0-openjdk.x86_64 iptables-services python36u python36u-pip.noarch
Install python requirements:

    pip3.6 install --upgrade pip
    pip3 install kafka-python python-twitter tweepy
Setup iptables firewall:  
Edit /etc/sysconfig/iptables and make and after the first line starting with -A add:

		-A INPUT -s 10.0.0.0/8 -j ACCEPT
Disable firewall and enable and start iptables:

    systemctl disable firewalld
    systemctl stop firewalld
    systemctl mask firewalld
    systemctl enable iptables
    systemctl start iptables
Create a java environment file /etc/profile.d/java.sh with:

    export JAVA_HOME=/usr/lib/jvm/jre-1.8.0-openjdk
    export JRE_HOME=/usr/lib/jvm/jre
Set up the environment.
Edit `/etc/hosts`, comment out the lines with the hostname in them and add the 10.X.Y.Z address (obtained with “ip addr show”), don’t forget the ipv6 addresses.

Set up passwordless ssh into localhost:

    ssh-keygen
	cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
Reread the profile (with java environment from previous step)

    . /etc/profile
Download, install and start Kafka 0.8.  I’m installing in /opt, but could be anywhere:  

    cd /opt
    wget -O - http://archive.apache.org/dist/kafka/0.8.2.2/kafka_2.11-0.8.2.2.tgz | tar -xzf -
    ln -s kafka_2.11-0.8.2.2/ kafka
    cd kafka
    bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
    bin/kafka-server-start.sh -daemon config/server.properties
Download, install and start spark:

    cd /opt
    wget -O - http://apache.mirror.globo.tech/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz | tar -xzf -
    ln -s spark-2.3.1-bin-hadoop2.7 spark
    cd spark
    cp conf/log4j.properties.template conf/log4j.properties
Edit `conf/log4j.properties` and change the first INFO to WARN (reduce the logging)

Start spark:

    sbin/start-all.sh

Get the streaming jar file.  This needs to match the version of Kafka (0.8.2), Scala (2.11) and Spark 2.3.1:

    cd /opt/spark/jars; wget http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8-assembly_2.11/2.3.1/spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar
Install git and clone our project's repo into the root directory:

    yum -y install git  
	cd /root
	git clone https://github.com/daxaurora/MIDS_W251_Benchmarking.git

Note: to push back into the git repo from the cluster will require setting up ssh keys or configuring collaborators. So for now the standalone cluster can only receive the repo.

Two python scripts are in the streaming folder of the repo: twitter_connect.py and spark_pull_tweets.py.

Edit `twitter_connect.py` to add your Twitter credentials.

Run twitter_connect.py and run it with python 3:

    python3 /root/MIDS_W251_Benchmarking/streaming/twitter_connect.py

Note: I can't get it to run from python3 yet so haven't tested below.  --Laura

Test if the twitter topic is created and receiving messages:

    /opt/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181

  Should report “twitter” as a topic after twitter_connect.py is running.

Run the `spark_pull_tweets.py` script with spark-submit in the following command line:

    /opt/spark/bin/spark-submit --jars /opt/spark/jars/spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar /root/spark_pull_tweets.py localhost:9092 twitter

While the twitter_connect.py is not running the output of this should look like
```
-------------------------------------------
Time: 2018-07-18 17:25:12
-------------------------------------------
-------------------------------------------
Time: 2018-07-18 17:25:14
-------------------------------------------
-------------------------------------------
Time: 2018-07-18 17:25:16
-------------------------------------------```

However when twitter_connect.py is running the output will be something like:
```

-------------------------------------------
Time: 2018-07-18 17:25:22
-------------------------------------------
(u'', 169)
(u'19:10:09', 1)
(u"\\ud83d\\ude0d\\n\\nWe'll", 3)
(u'ma\\ud83d\\ude11","display_text_range":[13,76],"source":"\\u003ca', 1)
(u'Buenos', 1)
(u'Basquete', 2)
(u'up.', 1)
(u'cr\\u00e8che', 1)
(u'18:17:30', 1)
(u'\\u0628\\u0643\\u0644\\u0645\\u0627\\u062a', 1)
...
```
