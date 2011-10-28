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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.platform.collector.record.LogRecordKey;
import com.nexr.util.CRCUtil;

public class SeqGenerator {
	static final Logger LOG = LoggerFactory.getLogger(SeqGenerator.class);

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

	public SeqGenerator() {
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

	private String[] getInputFiles() {
		File input = new File(inputPath);
		return input.list();
	}

	private boolean readData(String[] inputs) throws FileNotFoundException, IOException {
		boolean res = false;
		InputStream in = null;
		for (int i = 0; i < inputs.length; i++) {
			SeqThread st = new SeqThread(inputPath + File.separator + inputs[i]);
			st.start();
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
		String input_fileName;
		TimeStampParser tsParsor;
		StringBuffer tmp;
		InputStream in;

		SeqThread(String fileName) {
			super("SeqThread");
			this.input_fileName = fileName;

			this.tmp = new StringBuffer();
			try {
				in = new FileInputStream(fileName);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			try {
				fs = FileSystem.get(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				writer = SequenceFile.createWriter(fs, conf,
						new Path(outputPath, fileName.substring(fileName.lastIndexOf("/") + 1, fileName.length())), key.getClass(),
						Text.class);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public void run() {
			try {
				int el;
				byte[] buffer = new byte[4098];
				LOG.info("Start File " + input_fileName);
				while ((el = in.read(buffer)) != -1) {
					synchronized (tmp) {
						tmp.append(new String(buffer));
						buffer = new byte[4098];

						while (tmp.toString().contains(rd)) {
							int index = tmp.indexOf(rd) + 1;
							record = tmp.substring(0, index);

							tmp.delete(0, index);
							if (tsParsor == null) {
								try {
									tsParsor = ((TimeStampParser) Class.forName(parserClass).newInstance());
									Thread.sleep(10);
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}

							try {
								timestamp = tsParsor.getTimeStamp(record, timeFormat);
							} catch (Exception e1) {
								// TODO Auto-generated catch block
							}
							/* depend on data format */

							key.setDataType(dataType);
							key.setTime(Long.toString(timestamp));
							key.setLogId(input_fileName + crcUtil.genHash(record.getBytes()));
							Text value = new Text(record);

							if (timestamp != 0) {
								try {
									writer.append(key, value);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}

					}
				}
				in.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			LOG.info("Close File " + input_fileName + " " + tmp.length());
			LOG.info(tmp.toString());

			IOUtils.closeStream(writer);
			LOG.info("Finish File " + input_fileName + " " + tmp.length());
		}

	};

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

}
