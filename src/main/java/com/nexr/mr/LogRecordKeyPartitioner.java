package com.nexr.mr;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import com.nexr.platform.collector.record.LogRecordKey;

@SuppressWarnings("deprecation")
public class LogRecordKeyPartitioner<K, V> implements Partitioner<LogRecordKey, Writable> {
	public void configure(JobConf arg0) {
		 
	}

	public int getPartition(LogRecordKey key, Writable chunl, int numReduceTasks) {
		return key.getLogId().hashCode() & Integer.MAX_VALUE % numReduceTasks;
	}
}