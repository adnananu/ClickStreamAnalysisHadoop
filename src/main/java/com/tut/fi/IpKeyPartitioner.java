package com.tut.fi;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
/*
* imp point: A partitioner partitions the key-value pairs of intermediate Map-outputs.
* partitioning the records to sent them to multiple reducers in hadoop. its a custom partitioner and also using the IP
* address only.
* */
public class IpKeyPartitioner extends Partitioner<Ip_TimeStampKey, Text> {

    @Override
    public int getPartition(Ip_TimeStampKey key, Text value, int numPartitions) {
        return Math.abs(key.getIp().hashCode()) % numPartitions;
    }
}
