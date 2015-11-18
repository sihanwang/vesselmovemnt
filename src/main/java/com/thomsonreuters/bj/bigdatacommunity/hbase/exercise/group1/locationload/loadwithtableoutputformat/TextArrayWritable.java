package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.loadwithtableoutputformat;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
public class TextArrayWritable extends ArrayWritable {

	public TextArrayWritable() {
		super(Text.class);
		// TODO Auto-generated constructor stub
	}

	public TextArrayWritable(Text[] arg0) {
		super(Text.class);
		super.set(arg0);
		// TODO Auto-generated constructor stub
	}
	
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		Writable[] allfields=super.get();

		String strrow=null;

		for (Writable thisfield : allfields)
		{
			if (strrow==null)
			{
				strrow=((Text)thisfield).toString();
			}
			else
			{
				strrow=strrow + " | " +((Text)thisfield).toString();
			}
		}

		return strrow;
	}
}
