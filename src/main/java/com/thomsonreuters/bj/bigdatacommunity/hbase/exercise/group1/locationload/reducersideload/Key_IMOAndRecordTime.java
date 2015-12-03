package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;

public class Key_IMOAndRecordTime implements WritableComparable<Key_IMOAndRecordTime> {


	private VLongWritable IMO;
	private VLongWritable recordTime;
	
	public Key_IMOAndRecordTime()
	{
		set(new VLongWritable(), new VLongWritable());
	}

	public void set(VLongWritable imo, VLongWritable record_Time)
	{
		this.IMO=imo;
		this.recordTime=record_Time;
	}
	
	public Key_IMOAndRecordTime(long imo, Date record_time) {
		IMO = new VLongWritable(imo);
		recordTime = new VLongWritable(record_time.getTime());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

		IMO.readFields(in);
		recordTime.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		IMO.write(out);
		recordTime.write(out);
	}

	@Override
	public int compareTo(Key_IMOAndRecordTime key) {
		// TODO Auto-generated method stub
		int cmp = IMO.compareTo(key.getIMO());
		if (cmp != 0) {
			return cmp;
		}
		return recordTime.compareTo(key.getRecordTime());

	}

	public VLongWritable getIMO() {
		return IMO;
	}

	public VLongWritable getRecordTime() {
		return recordTime;
	}

	
	@Override
	public boolean equals(Object o) {
		// TODO Auto-generated method stub
		if (o instanceof Key_IMOAndRecordTime) {
			Key_IMOAndRecordTime key = (Key_IMOAndRecordTime) o;
			return IMO.equals(key.getIMO())
					&& recordTime.equals(key.getRecordTime());
		}
		return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return IMO.hashCode() * 31 + recordTime.hashCode();
	}
}
