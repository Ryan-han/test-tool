package com.nexr.test;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SqueezedLogParser implements TimeStampParser{

	static final Logger LOG = LoggerFactory.getLogger(SeqGenerator.class);
	
	public static DateFormat dateFormat = null;
	

	public long getTimeStamp(String record, String timeFormat) {
		// TODO Auto-generated method stub
		long timestamp = 0;
		LOG.debug("Time Format " + timeFormat);
		
		dateFormat = new SimpleDateFormat(timeFormat, Locale.US);
		Date d = null;
		if(record.length()>23){
			try {
				d = dateFormat.parse(record.trim().substring(1, 23));
			  timestamp = d.getTime();
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				LOG.info(record);
				e1.printStackTrace();
			}
		}else{
			LOG.info("error record " + record);
		}
		
		return timestamp;
	}

}
