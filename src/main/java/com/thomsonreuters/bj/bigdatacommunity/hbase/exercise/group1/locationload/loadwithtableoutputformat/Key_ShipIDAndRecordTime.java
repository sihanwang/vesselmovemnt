package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.loadwithtableoutputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;

public class Key_ShipIDAndRecordTime implements WritableComparable<Key_ShipIDAndRecordTime> {


	private VLongWritable shipID;
	private VLongWritable recordTime;
	
	public Key_ShipIDAndRecordTime()
	{
		set(new VLongWritable(), new VLongWritable());
	}

	public void set(VLongWritable ship_id, VLongWritable record_Time)
	{
		this.shipID=ship_id;
		this.recordTime=record_Time;
	}
	
	public Key_ShipIDAndRecordTime(long ship_id, Date record_time) {
		shipID = new VLongWritable(ship_id);
		recordTime = new VLongWritable(record_time.getTime());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

		shipID.readFields(in);
		recordTime.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		shipID.write(out);
		recordTime.write(out);
	}

	@Override
	public int compareTo(Key_ShipIDAndRecordTime key) {
		// TODO Auto-generated method stub
		int cmp = shipID.compareTo(key.getShipID());
		if (cmp != 0) {
			return cmp;
		}
		return recordTime.compareTo(key.getRecordTime());

	}

	public VLongWritable getShipID() {
		return shipID;
	}

	public VLongWritable getRecordTime() {
		return recordTime;
	}

	
	@Override
	public boolean equals(Object o) {
		// TODO Auto-generated method stub
		if (o instanceof Key_ShipIDAndRecordTime) {
			Key_ShipIDAndRecordTime key = (Key_ShipIDAndRecordTime) o;
			return shipID.equals(key.getShipID())
					&& recordTime.equals(key.getRecordTime());
		}
		return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return shipID.hashCode() * 31 + recordTime.hashCode();
	}
}
