package com.jackie.kafkademo.demo1;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class SimplePartitioner2 implements Partitioner {

    public int partition(String topic, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        int partition = 0;

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf(".");
        if (offset > 0) {
            partition = Integer.parseInt(stringKey.substring(offset + 1)) % numPartitions;
        }
        return partition;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
