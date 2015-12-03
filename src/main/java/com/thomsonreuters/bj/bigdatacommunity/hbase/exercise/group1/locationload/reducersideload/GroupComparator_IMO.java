package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload;


import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class GroupComparator_IMO extends WritableComparator {

	public GroupComparator_IMO() {
		// TODO Auto-generated constructor stub
		super(Key_IMOAndRecordTime.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Key_IMOAndRecordTime k1 = (Key_IMOAndRecordTime)a;
		Key_IMOAndRecordTime k2 = (Key_IMOAndRecordTime)b;
		
		VLongWritable IMO1=k1.getIMO();
		VLongWritable IMO2=k2.getIMO();
		return IMO1.compareTo(IMO2);
	}
	

}
