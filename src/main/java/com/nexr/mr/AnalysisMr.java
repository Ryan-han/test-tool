package com.nexr.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.platform.collector.record.LogRecordKey;

@Deprecated
public class AnalysisMr extends Configured implements Tool {
	
	static final Logger LOG = LoggerFactory.getLogger(AnalysisMr.class);
	
	static class UniqueKeyReduce extends MapReduceBase implements
			Reducer<LogRecordKey, Writable, Text, LongWritable> {
	
		public void reduce(LogRecordKey key, Iterator<Writable> vals,
				OutputCollector<Text, LongWritable> out, Reporter r)
				throws IOException {
			
			int dups = 0;
			while (vals.hasNext()) {
				vals.next();
				dups++;
			}
			if(dups > 1){
				out.collect(new Text(key.getLogId()), new LongWritable(dups));
			}
				r.incrCounter("rolling", "duplication", dups);
		}
	}
	

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: analysis <in> <out>");
      System.exit(2);
    }

    
		JobConf jobConf = new JobConf(conf, AnalysisMr.class);
		jobConf.setMapperClass(IdentityMapper.class);
		jobConf.setReducerClass(UniqueKeyReduce.class);
		jobConf.setJobName("Data-Analysis");
		jobConf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(jobConf, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(otherArgs[1]));

		jobConf.setInputFormat(DataInputFormat.class);
		jobConf.setMapOutputKeyClass(LogRecordKey.class);
		jobConf.setMapOutputValueClass(Text.class);
		JobClient jc = new JobClient(jobConf);
	
		RunningJob runningJob = jc.submitJob(jobConf);
		if (!jc.monitorAndPrintJob(jobConf, runningJob)) {
			throw new IOException("Job failed!");
		}
		

		LOG.info("MAP_INPUT_RECORDS " + runningJob.getCounters().getCounter(Counter.MAP_INPUT_RECORDS));
		LOG.info("MAP_OUTPUT_RECORDS " + runningJob.getCounters().getCounter(Counter.MAP_OUTPUT_RECORDS));
		LOG.info("REDUCE_INPUT_RECORDS " + runningJob.getCounters().getCounter(Counter.REDUCE_INPUT_RECORDS));
		LOG.info("REDUCE_OUTPUT_RECORDS " + runningJob.getCounters().getCounter(Counter.REDUCE_OUTPUT_RECORDS));
		LOG.info("Total Duplicate Count " + (runningJob.getCounters().getCounter(Counter.MAP_INPUT_RECORDS) - runningJob.getCounters().getCounter(Counter.REDUCE_OUTPUT_RECORDS)));
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(otherArgs[1], "result"), Text.class, LongWritable.class);
		writer.append(new Text("MAP_INPUT_RECORDSS"), new LongWritable(runningJob.getCounters().getCounter(Counter.MAP_INPUT_RECORDS)));
		writer.append(new Text("MAP_OUTPUT_RECORDS"), new LongWritable(runningJob.getCounters().getCounter(Counter.MAP_OUTPUT_RECORDS)));
		writer.append(new Text("REDUCE_INPUT_RECORDS"), new LongWritable(runningJob.getCounters().getCounter(Counter.REDUCE_INPUT_RECORDS)));
		writer.append(new Text("REDUCE_OUTPUT_RECORDS"), new LongWritable(runningJob.getCounters().getCounter(Counter.REDUCE_OUTPUT_RECORDS)));
		writer.append(new Text("Total Duplicate Count"), new LongWritable(runningJob.getCounters().getCounter(Counter.MAP_INPUT_RECORDS) - runningJob.getCounters().getCounter(Counter.REDUCE_OUTPUT_RECORDS)));
		writer.close();
		return 0;
	}
	
	public static void main(String[] args){
		AnalysisMr mr = new AnalysisMr();
		try {
			mr.run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
