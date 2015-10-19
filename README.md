Hadoop PCAP analyzer
===================

This project is a pcap package analysis tool based on Hadoop and Hive.

------------
Installation
------------
    
1. To install Apache Hadoop
    
    http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-multi-node-cluster/
    
2. To install Apache Hive
    
    https://cwiki.apache.org/confluence/display/Hive/GettingStarted
    
------------
Pcap Analysis
------------
    
1. Analyze pcap package on network layer
    
    This will give you information about location of source ip and destination ip on network layer
    
    To use this function, you can use the following command:
    
    hadoop jar ./analyzer-0.0.1-SNAPSHOT.jar com.offline.runner.Network_runner -r[source dir/file] -a[database dir/file] -n[reducer_num] -p[time_period] -i[interval]
    
2. Analyze pcap package on transport layer
    
    This will give you information about the number of transport protocol each time interval on transport layer
    
    To use this function, you can use the following command:
    
    hadoop jar ./analyzer-0.0.1-SNAPSHOT.jar com.offline.runner.Transport_runner -r[source dir/file] -n[reducer_num] -p[time_period] -i[interval]
    
3. Analyze pcap package on application layer
    
    This will give you information about the number of application protocol each time interval on application layer
    
    To use this function, you can use the following command:
    
    hadoop jar ./analyzer-0.0.1-SNAPSHOT.jar com.offline.runner.Application_runner -r[source dir/file] -n[reducer_num] -p[time_period] -i[interval]
    
------------
Pcap Deserialzation
------------
    
In order to use Hive to query the information of Pcap, we need use our own INPUTFORMAT and ROW FORMAT SERDE to deserialize each row.

After we put the analyzer-0.0.1-SNAPSHOT.jar into Hive, we can use the following statement to create the table in Hive:

    CREATE EXTERNAL TABLE pcaps(
    SIP string,
    DIP string,
    SP int,
    DP int,
    LEN bigint,
    TSTAMP bigint)
    ROW FORMAT SERDE 'com.offline.Hive.SerDe.PcapSerDe' 
    STORED AS INPUTFORMAT 'com.offline.Package_IO.PcapInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION 'hdfs:///hduser/sample_input/';
    
Using this table, you can query pacp's information about source ip(SIP), destination ip(DIP), source port(SP), destination port(DP), package length(LEN) and timestamp(TSTAMP).
 
