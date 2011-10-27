package com.nexr.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import com.nexr.platform.collector.record.LogRecordKey;



public class DataInputFormat extends CombineFileInputFormat<LogRecordKey, Writable> {

	public DataInputFormat() {
		setMaxSplitSize(64 * 1024 * 1024L);
	}

	@Override
	public RecordReader<LogRecordKey, Writable> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter) throws IOException {
		return new CombineFileRecordReader(
				job, (CombineFileSplit) split, reporter, (Class) RollingSequenceFileRecordReader.class);
	}
	
	
	public static class RollingSequenceFileRecordReader implements RecordReader<LogRecordKey, Writable> {
		private final SequenceFileRecordReader<LogRecordKey, Writable> delegate;

		public RollingSequenceFileRecordReader(CombineFileSplit split, Configuration conf, Reporter reporter, Integer idx)
				throws IOException {
			FileSplit fileSplit = new FileSplit(split.getPath(idx),
					split.getOffset(idx), split.getLength(idx),
					split.getLocations());
			delegate = new SequenceFileRecordReader<LogRecordKey, Writable>(conf, fileSplit);
		}

		public boolean next(LogRecordKey key, Writable value) throws IOException {
			// TODO Auto-generated method stub
			return delegate.next(key, value);
		}

		public LogRecordKey createKey() {
			// TODO Auto-generated method stub
			return new LogRecordKey();
		}

		public Writable createValue() {
			// TODO Auto-generated method stub
			return new Text();
		}

		public long getPos() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		public float getProgress() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		
	}

	

}
