package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import net.sf.json.JSONObject;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class VesselPoint implements Writable{
	


	private Text Coordinates;
	private Text Speed;
	private Text Destination;
	private Text Timestamp;
	private Text Previouslocation;
	private Text Nextlocation;
	
	
	
	public VesselPoint()
	{
		this.Coordinates=new Text();
		this.Speed=new Text();
		this.Destination=new Text();
		this.Timestamp=new Text();
		this.Previouslocation=new Text();
		this.Nextlocation=new Text();
		
	}
	
	public VesselPoint(String coordinates, String speed, String destination,
			String timestamp, String previouslocation, String nextlocation)
	{
		this.Coordinates=new Text(coordinates);
		this.Speed=new Text(speed);
		this.Destination=new Text(destination);
		this.Timestamp=new Text(timestamp);
		this.Previouslocation=new Text(previouslocation);
		this.Nextlocation=new Text(nextlocation);
	}
	
	public String getCoordinates() {
		return Coordinates.toString();
	}
	public void setCoordinates(String coordinates) {
		Coordinates = new Text(coordinates);
	}
	public String getSpeed() {
		return Speed.toString();
	}
	public void setSpeed(String speed) {
		Speed = new Text(speed);
	}
	public String getDestination() {
		return Destination.toString();
	}
	public void setDestination(String destination) {
		Destination = new Text(destination);
	}

	public String getTimestamp() {
		return Timestamp.toString();
	}

	public void setTimestamp(String timestamp) {
		Timestamp = new Text(timestamp);
	}

	public String getPreviouslocation() {
		return Previouslocation.toString();
	}

	public void setPreviouslocation(String previouslocation) {
		Previouslocation = new Text(previouslocation);
	}

	public String getNextlocation() {
		return Nextlocation.toString();
	}

	public void setNextlocation(String nextlocation) {
		Nextlocation = new Text(nextlocation);
	}
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.Coordinates.write(out);
		this.Speed.write(out);
		this.Destination.write(out);
		this.Timestamp.write(out);
		this.Previouslocation.write(out);
		this.Nextlocation.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method st
		
		this.Coordinates.readFields(in);
		this.Speed.readFields(in);
		this.Destination.readFields(in);
		this.Timestamp.readFields(in);
		this.Previouslocation.readFields(in);
		this.Nextlocation.readFields(in);
		
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		JSONObject jsonObject = JSONObject.fromObject(this);		
		return jsonObject.toString();
	}
	
	public VesselPoint Clone()
	{
		return new VesselPoint(this.Coordinates.toString(),this.Speed.toString(),this.Destination.toString(),this.Timestamp.toString(),this.Previouslocation.toString(),this.Nextlocation.toString());
	}
	
	

}
