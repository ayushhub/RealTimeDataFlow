PRE-REQUISITES: 
H/W: Mac 10.13.3, 16GB RAM, 100GB
S/W: Virtual Box (4 Core, 12GB RAM), HDP Sandbox 2.5, NiFi

STEP ONE:

Virtual Box Installation
	•	https://www.virtualbox.org/wiki/Downloads
	•	Hortonworks VM
	•	HDP 2.5 for Virtual Box
	•	https://hortonworks.com/downloads/#sandbox
	•	Recommended – 4 cores and 12GB RAM

Checking the install
	•	Edit hosts file on your machine to add alias of below
	•	sanbox.hortonworks.com 127.0.0.1
	•	Access VM
	•	Access Ambari >  http://127.0.0.1:8080/#/main/dashboard/metrics
	•	user: xxxxx, password: xxxxx
	•	Access terminal > ssh root@sandbox.hortonworks.com -p 2222
	•	user: xxxx, password: xxxxx
  
STEP TWO: 

Install Nifi - OPTION 1

	•	https://hortonworks.com/downloads/ (Nifi Only)
	•	Untar the file at root directory.
	•	./HDF-2.0.0.0/nifi/bin/nifi.sh start
	•	./HDF-2.0.0.0/nifi/bin/nifi.sh stop

Install Nifi – OPTION 2 (Liked it better)

	•	wget http://d3d0kdwqv675cq.cloudfront.net/HDF/centos6/1.x/updates/1.2.0.1/HDF-1.2.0.1-1.tar.gz
	•	tar -xvf HDF-1.2.0.1-1.tar.gz
	•	cd HDF-1.2.0.1-1/nifi/
	•	Edit NiFi in the conf/nifi.properties file as below
	•	nifi.web.http.port=8090
	•	Add port forwarding to your Virtual Box

STEP THREE:

Access NiFi

	•	http://127.0.0.1:8090/nifi/ (if not try below)
	•	http://sandbox.hortonworks.com:8090

Check Your Environment

	•	Check Hadoop Configuration (do not edit if not sure..)
	•	cd /etc/hadoop/conf
	•	core-site.xml – Cluster wide setting (distributed to all nodes)
	•	hdfs-site.xml – Cluster & Data node specific
	•	yarn-site.xml – Resource Manager (scheduler)

Helpful Admin Commands

	•	hadoop fsck / (File system check)
	•	hadoop fs –du –s –h / (Free Storage)
	•	hadoop fs –count –q / (quota info)
	•	hadoop fsck –blocks –locations / (DN and block of files INFO)
	•	yarn application –list
	•	yarn application –kill
	•	hadoop queue –list

STEP FOUR:

Setting up Kafka

	•	Check on Ambari if zookeeper is running and then if Kafka is running, if not start the services (make sure to check mark “turn off maintenance mode)
	•	Making a new topic/list/view data run below commands
	•	/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper sandbox.hortonworks.com:2181 --replication-factor 1 --partitions 2 --topic topicname
	•	/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper sandbox.hortonworks.com:2181
	•	/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper sandbox.hortonworks.com:2181 --topic topicname

STEP FIVE:

Setting up Zepplin

	•	http://sandbox.hortonworks.com:9995/#/
	•	Import Nifi Flows
	•	Attached XML’s store them on your PC.
	•	On right side top go to “templates”
	•	Select and import. On Left top drag and drop “template” next to “funnel” and this way you will select the imported template.
	•	Explore the settings in the template processors.
