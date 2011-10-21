package com.nexr.test;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.platform.collector.record.LogRecordKey;
import com.nexr.util.CRCUtil;

public class SeqGenerator {
	static final Logger LOG = LoggerFactory.getLogger(SeqGenerator.class);
	static final String FNAME_START_TAG = "TFNameStart-";
	static final String FNAME_END_TAG = "-TFNameEnd";

	// public static DateFormat dateFormat = null;
	StringBuffer tmp = new StringBuffer();
	String fileName;

	String dataType = null;
	String parserClass = null;
	int timeColnum = 0;
	String timeFormat = null;
	String inputPath = null;
	String outputPath = null;
	String cd = " ";
	String rd = "\n";
	int rp = 0;

	private SeqThread st;

	public SeqGenerator() {
		this.st = new SeqThread();
		st.start();
	}

	public static void main(String[] argv) {
		SeqGenerator sg = new SeqGenerator();
		sg.setup(argv);

		try {
			sg.readData(sg.getInputFiles());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void setup(String argv[]) {

		CommandLine cmd = null;
		Options options = new Options();
		options.addOption("d", true, "DataType");
		options.addOption("cp", true, "Parser class");
		options.addOption("ip", true, "input path");
		options.addOption("op", true, "output path");
		options.addOption("tc", true, "time colnum");
		options.addOption("tf", true, "time format");
		options.addOption("cd", true, "colnum delimeter");
		options.addOption("rd", true, "record delimeter");
		options.addOption("rp", true, "rolling period");

		try {
			CommandLineParser parser = new PosixParser();
			cmd = parser.parse(options, argv);
		} catch (org.apache.commons.cli.ParseException e) {
			HelpFormatter fmt = new HelpFormatter();
			fmt.printHelp("SeqGenerator", options, true);
			System.exit(1);
		}

		if (cmd != null && cmd.hasOption("d")) {
			dataType = cmd.getOptionValue("d");
		}
		
		if (cmd != null && cmd.hasOption("cp")) {
			parserClass = cmd.getOptionValue("cp");
		}

		if (cmd != null && cmd.hasOption("tc")) {
			timeColnum = Integer.parseInt(cmd.getOptionValue("tc"));
		}

		if (cmd != null && cmd.hasOption("tf")) {
			timeFormat = cmd.getOptionValue("tf");
		}

		if (cmd != null && cmd.hasOption("ip")) {
			inputPath = cmd.getOptionValue("ip");
		}

		if (cmd != null && cmd.hasOption("op")) {
			outputPath = cmd.getOptionValue("op");
		}

		if (cmd != null && cmd.hasOption("cd")) {
			if (cmd.getOptionValue("cd") != null) {
				cd = cmd.getOptionValue("cd");
			}
		}

		if (cmd != null && cmd.hasOption("rd")) {
			if (cmd.getOptionValue("rd") != null) {
				rd = cmd.getOptionValue("rd");
			}
		}

		if (cmd != null && cmd.hasOption("rp")) {
			if (cmd.getOptionValue("rp") != null) {
				rp = Integer.parseInt(cmd.getOptionValue("rp"));
			}
		}
	}

	private String[] getInputFiles() {
		File input = new File(inputPath);
		return input.list();
	}

	private boolean readData(String[] inputs) throws FileNotFoundException, IOException {
		boolean res = false;

		InputStream in = null;
		for (int i = 0; i < inputs.length; i++) {
			fileName = FNAME_START_TAG + inputs[i] + FNAME_END_TAG;
			in = new FileInputStream(inputPath + File.separator + inputs[i]);
			int el;
			byte[] buffer = new byte[1 << 15];
			tmp.append(fileName);
			LOG.info("Start File " + fileName);

			buffer = new byte[1 << 15];
			while ((el = in.read(buffer)) != -1) {
				synchronized (tmp) {
					tmp.append(new String(buffer));
					buffer = new byte[1 << 15];
				}
			}
		}

		return res;
	}

	class SeqThread extends Thread {
		LogRecordKey key = new LogRecordKey();
		CRCUtil crcUtil = new CRCUtil();
		boolean complete = false;
		String record = null;
		long timestamp;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		SequenceFile.Writer writer = null;
		long writerCreatTime;
		String fName;
		TimeStampParser tsParsor;

		SeqThread() {
			super("SeqThread");
			try {
				fs = FileSystem.get(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		int z = 0;
		int totalSize = 0;

		public void run() {
			while (!complete) {
				if (tmp.length() > 1) {
					z++;
					synchronized (tmp) {
						int index = tmp.indexOf(rd) + 1;
						record = tmp.substring(0, index);
						if (record.contains(FNAME_START_TAG)) {
							LOG.info("Tag exist " + record);
							fName = record.substring(record.indexOf(FNAME_START_TAG) + FNAME_START_TAG.length(),
									record.indexOf(FNAME_END_TAG));
							LOG.info("FileName ==> " + fName);
							record = record.substring(record.indexOf(FNAME_END_TAG) + FNAME_END_TAG.length(), record.length());
						}

						totalSize += record.length();
						tmp.delete(0, index);

						LOG.debug("Record ==> " + record + " " + z + " " + totalSize);
					}

					/* depend on data format */
					if (tsParsor == null) {
						try {
							tsParsor = ((TimeStampParser) Class.forName(parserClass).newInstance());
						} catch (InstantiationException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IllegalAccessException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					timestamp = tsParsor.getTimeStamp(record, timeFormat);
					/* depend on data format */

					key.setDataType(dataType);
					key.setTime(Long.toString(timestamp));
					key.setLogId(fName + crcUtil.genHash(record.getBytes()));
					Text value = new Text(record);

					if (writer == null) {
						writerCreatTime = System.currentTimeMillis();
						try {
							writer = SequenceFile.createWriter(fs, conf, new Path(outputPath, Long.toString(writerCreatTime)),
									key.getClass(), Text.class);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					if (timestamp != 0) {
						try {
							writer.append(key, value);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					if (rp != 0) {
						if ((writerCreatTime + (rp * 1000)) < System.currentTimeMillis()) {
							IOUtils.closeStream(writer);
							writer = null;
						}
					}

					if (tmp.toString().trim().length() == 0) {
						complete = true;
						IOUtils.closeStream(writer);
						LOG.info("Complete!!");
					}
				}
			}

			if (LOG.isDebugEnabled()) {
				SequenceFile.Reader reader = null;
				try {
					reader = new SequenceFile.Reader(fs, new Path(outputPath, Long.toString(writerCreatTime)), conf);
					Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
					Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
					while (reader.next(key, value)) {
						LOG.debug("Key " + key.toString() + " Value  => " + value.toString());
					}

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					IOUtils.closeStream(reader);
				}
			}
		}

	};
}
