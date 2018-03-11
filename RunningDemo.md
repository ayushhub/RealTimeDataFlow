[![DASHBOARD](https://github.com/ayushhub/RealTimeDataFlow/blob/master/screenshots/39_zeppelin_country_pie_chart-800x202.png)]

FOLLOW PRE-REQUISITES

NEXT,
The first thing you’re going to need if you haven’t done it already is install the Apache NiFi service on your Sandbox. Click on Actions button in the left sidebar on the Ambari Dashboard. Click on Add Service. Then from the list of services, choose NiFi. Then click next, and continue clicking next until you reach the page where a progress bar shows NiFi is being installed. If recommended configurations are not set, you can ignore them since they are for a production cluster, but we are building a demo on a sandbox. You should see NiFi installed successfully.

[![DASHBOARD](https://github.com/ayushhub/RealTimeDataFlow/blob/master/screenshots/solr_install_success.png)]

[![DASHBOARD](https://github.com/ayushhub/RealTimeDataFlow/blob/master/screenshots/add_collection_tweets_sentiment_analysis.png)]

[![DASHBOARD](https://github.com/ayushhub/RealTimeDataFlow/blob/master/screenshots/41_zeppelin_sentiment_average-800x468.png)]

[![DASHBOARD](https://github.com/ayushhub/RealTimeDataFlow/blob/master/screenshots/nifi.png)]

[![DASHBOARD](https://github.com/ayushhub/RealTimeDataFlow/blob/master/screenshots/grab_garden.png)]

[![DASHBOARD](https://github.com/ayushhub/RealTimeDataFlow/blob/master/screenshots/tweetsbi_bar_graph_sentiment_analysis-800x467.png)]

ADD JAR /usr/hdp/2.5.0.0-1245/hive2/lib/json-serde-1.3.8-SNAPSHOT-jar-with-dependencies.jar;

create table IF NOT EXISTS tweets_sentiment stored as orc as select
  tweet_id,
  case
    when sum( polarity ) > 0 then 'positive'
    when sum( polarity ) < 0 then 'negative'  
    else 'neutral' end as sentiment
from l3 group by tweet_id;

hadoop fs -ls /tmp/tweets_staging | awk '{print $8}' | while read f; do hadoop fs -cat $f | grep escravadaonedir >/tmp/null && echo $f; done

SELECT * FROM tweets_clean LIMIT 10;

tweet_id":838029076367568897
displayname":"escravadaonedir
ANALYZING SOCIAL MEDIA AND CUSTOMER SENTIMENT

grep -rnw '/tmp/tweets' -e "838029076367568897"

Access Virtual Machine > http://127.0.0.1:8080/#/main/dashboard/metrics

Make sure setup kafka topics delete to true

ssh root@sandbox.hortonworks.com -p 2222

scp -P 2222 <filename> root@127.0.0.1:/root/.

select tweet_id,
  case
    when sum( polarity ) > 0 then 'positive'
    when sum( polarity ) < 0 then 'negative'  
    else 'neutral' end as sentiment
from l3 
where time_zone != ‘Tokyo’
group by tweet_id limit 100;

Start Nifi
cd /root/HDF-1.2.0.1-1/nifi/bin
sudo service nifi start
sudo service nifi stop
Access Nifi
http://127.0.0.1:8090/nifi/
finale
Check Data
cd /root/nifi_output
hadoop fs –ls /groupm
hive
show databases;
show tables;
select * from truck_events1;
/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper sandbox.hortonworks.com:2181 --topic truck_events2


Elasticsearch


cd /home
sudo su – elastic
cd /opt/elasticsearch-2.4.0
bin/elasticsearch
curl -XGET 127.0.0.1:9200
curl -XGET 127.0.0.1:9200/twitter/_search?pretty

curl -XDELETE localhost:9200/shop

curl -XDELETE 127.0.0.1:9200/twitter

curl -XPUT 'localhost:9200/twitter?pretty' -H 'Content-Type: application/json' -d'
{
    "settings" : {
        "number_of_shards" : 3,
        "number_of_replicas" : 2
    }
}
'

Kibana

cd /opt/kibana-4.6.1-linux-x86_64
bin/kibana

Nifi

Open new window
Start gettwitterinelasticsearch

twitter

Consumer Secret: XXXXXXXXXXXX
Access Token Secret: XXXXXXXXXX
Zepplin
http://sandbox.hortonworks.com:9995/#/
%sh
hadoop fs -ls

%elasticsearch
count /twitter/default

%elasticsearch
search /twitter/default {“aggs” : {“language” : {“terms” : {“field” : “lang” } } } }

%elasticsearch
search /twitter/default {"aggs" : {"geo_enabled" : {"terms" : {"field" : "user.geo_enabled" } } } }

%elasticsearch
search /twitter/default {"aggs" : {    "distinct_users" : {        "terms" : {            "field" : "user.screen_name"                     } } } }

%elasticsearch
size ${limit=10}
search /twitter/default { "query": { "match_all": { } }

Kafka

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --zookeeper sandbox.hortonworks.com:2181 --topic truck_events
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper sandbox.hortonworks.com:2181 --replication-factor 1 --partitions 2 --topic sfo-demo
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper sandbox.hortonworks.com:2181
/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper sandbox.hortonworks.com:2181 --topic sfo-demo


Storm

/root/iot-truck-streaming
config.properties
/usr/maven/bin/mvn clean package -DskipTests
cd ~/iot-truck-streaming
storm jar storm-streaming/target/storm-streaming-1.0-SNAPSHOT.jar com.hortonworks.streaming.impl.topologies.TruckEventKafkaExperimTopology /etc/storm_demo/config.properties


Hive

CREATE EXTERNAL TABLE IF NOT EXISTS truck_events1(        time STRING,         Miles_per_Gallon INT,        Cylinders INT,        Name STRING,        totalmileage INT,         destination STRING,        ride STRING,        locationx STRING,        locationy STRING,        origin STRING)    COMMENT 'truck events'    ROW FORMAT DELIMITED    FIELDS TERMINATED BY '|'    STORED AS TEXTFILE    location '/groupm';
Pig
A = LOAD 'default.truck_events1' USING org.apache.hcatalog.pig.HCatLoader();
B = filter A by miles_per_gallon == 55;
STORE B INTO ‘default.pig_events’ USING org.apache.hcatalog.pig.HCatLoader();

Zepplin

http://sandbox.hortonworks.com:9995/#/

%elasticsearch
count /twitter/default

%elasticsearch
search /twitter/default {“aggs” : {“language” : {“terms” : {“field” : “lang” } } } }

%elasticsearch
search /twitter/default {"aggs" : {"geo_enabled" : {"terms" : {"field" : "user.geo_enabled" } } } }

%elasticsearch
search /twitter/default {"aggs" : {    "distinct_users" : {        "terms" : {            "field" : "user.screen_name"                     } } } }

HBase

su hbase
hbase shell
create 'driver_events', 'allevents'    
create 'driver_dangerous_events', 'events'
create 'driver_dangerous_events_count', 'counters'
list    
exit
hbase shell

list
count 'driver_events'
count 'driver_dangerous_events'
count 'driver_dangerous_events_count'    
exit
Display in correct format
scan 'driver_dangerous_events_count' , {COLUMNS => ['counters:driverId:toInt', 'counters:incidentTotalCount:toLong']}
