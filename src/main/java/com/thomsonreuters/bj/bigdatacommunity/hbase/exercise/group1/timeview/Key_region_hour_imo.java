package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;

import com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload.Key_IMOAndRecordTime;

public class Key_region_hour_imo implements WritableComparable<Key_region_hour_imo> {

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.Region.toString()+"|"+Hour.toString()+"|"+IMO.toString();
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return Region.hashCode()*961+Hour.hashCode()*31+IMO.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (obj instanceof Key_region_hour_imo) {
			Key_region_hour_imo key = (Key_region_hour_imo) obj;
			return Region.equals(((Key_region_hour_imo) obj).getRegion()) &&
				   Hour.equals(((Key_region_hour_imo) obj).getHour()) &&
				   IMO.equals(key.getIMO());
		}
		return false;
	}


	private IntWritable Region=new IntWritable();
	private Text Hour=new Text();
	private Text IMO=new Text();
	
	public IntWritable getRegion() {
		return Region;
	}

	public Text getHour() {
		return Hour;
	}

	public Text getIMO() {
		return IMO;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Region.write(out);
		Hour.write(out);
		IMO.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		Region.readFields(in);
		Hour.readFields(in);
		IMO.readFields(in);
	}

	@Override
	public int compareTo(Key_region_hour_imo key) {
		// TODO Auto-generated method stub
		int cmp=this.Region.compareTo(key.getRegion());
		
		if (cmp!=0)
		{
			return cmp;
		}
		
		cmp=this.Hour.compareTo(key.getHour());
		
		if (cmp!=0)
		{
			return cmp;
		}
		
		return this.IMO.compareTo(key.getIMO());
		
	}
	
	public Key_region_hour_imo(int region, String hour, String imo)
	{
		this.Region=new IntWritable(region);
		this.Hour=new Text(hour);
		this.IMO=new Text(imo);
		
	}
	
	
	public Key_region_hour_imo()
	{
		set(new IntWritable(), new Text(), new Text());
	}
	

	public void set(IntWritable region, Text hour, Text imo)
	{
		this.Region=region;
		this.Hour=hour;
		this.IMO=imo;
	}

}
