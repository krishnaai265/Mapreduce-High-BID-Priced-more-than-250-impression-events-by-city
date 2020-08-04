package com.epam.highbidpricebycity;

import com.epam.highbidpricebycity.comparable.TextPair;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Performs partitioning based on OS.
 */
public class OSPartitioner<K, V> extends Partitioner<K, V> {

    @Override
    public int getPartition(K key, V value, int numReduceTasks) {
        String os = ((TextPair) key).getSecond().toString();
        return (os.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
