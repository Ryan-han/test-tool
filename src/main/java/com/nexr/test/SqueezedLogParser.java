package com.nexr.test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqueezedLogParser implements TimeStampParser {

	static final Logger LOG = LoggerFactory.getLogger(SeqGenerator.class);

	public static DateFormat dateFormat = null;

	
	public long getTimeStamp(String record, String timeFormat) throws Exception {
		// TODO Auto-generated method stub
		long timestamp = 0;
		LOG.debug("Time Format " + timeFormat);
		
		if(dateFormat == null){
			dateFormat = new SimpleDateFormat(timeFormat, Locale.US);
		}
		
		Date d = null;
		if (record.length() > 23) {
			try{
				synchronized (dateFormat) {
					d = dateFormat.parse(record.trim().substring(1, 23));
					timestamp = d.getTime();
				}
				
			}catch(Exception e){
				LOG.info("error record " + record.trim());
				e.printStackTrace();
			}
		} else {
			LOG.info("Empty record " + record);
		}

		return timestamp;
	}
	
	public static void main(String[] args){
		String record = "[20110907T07:11:21.588Z|debug|cnode05-m.pod5.epc.ucloud.com|0||xenops] domid 197 has been declared inactive";
		String timeFormat = "yyyyMMdd'T'HH:mm:ss.SSS";
		SqueezedLogParser parser = new SqueezedLogParser();
		try {
			LOG.info("" + parser.getTimeStamp(record, timeFormat));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
