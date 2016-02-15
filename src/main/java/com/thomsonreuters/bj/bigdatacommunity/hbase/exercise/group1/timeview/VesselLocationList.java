package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.timeview;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import net.sf.json.JSONObject;

public class VesselLocationList implements Writable{
	

	private Text Type=new Text();
	private ArrayWritable Coordinates = new ArrayWritable(VesselPoint.class);
	
	public String getType() {
		return Type.toString();
	}
	public void setType(String type) {
		Type = new Text(type);
	}
	public VesselPoint[] getCoordinates() {
		
		Writable[] allCoordinates=Coordinates.get();
		
		VesselPoint[] VesselPointList=new VesselPoint[allCoordinates.length];
		
		for (int i=0;i<allCoordinates.length;i++)
		{
			VesselPointList[i]=(VesselPoint)allCoordinates[i];
			
		}
		
		return VesselPointList;
	}
	public void setCoordinates(VesselPoint[] coordinates) {
		
		Coordinates.set(coordinates);
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Type.write(out);
		Coordinates.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		Type.readFields(in);
		Coordinates.readFields(in);
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		
		JSONObject jsonObject = JSONObject.fromObject(this);		
		return jsonObject.toString();
	}
	
	
	public static void main(String[] args)
	{
		VesselLocationList VLL=new VesselLocationList();
		VesselPoint[] VPList=new VesselPoint[3];
		VPList[0]=new VesselPoint("121212,1212","4.5","Beijing","20150203","12321321321","123213");
		VPList[1]=new VesselPoint("23452345,45","4.5","Shanghai","20150903","12321321321","123213");
		VPList[2]=new VesselPoint("78,567","4.5","Nanjing","20151003","12321321321","123213");
		VLL.setType("Tanker");
		VLL.setCoordinates(VPList);
		System.out.println(VLL.toString());
		
		ArrayList<VesselPoint> VesselPointList=new ArrayList<VesselPoint>();
		
		System.out.println("********************************");

		for (VesselPoint VP : VPList)
		{
			System.out.println(VP.toString());
			VesselPointList.add(VP);
		}
		System.out.println("********************************");
					
		VLL= new VesselLocationList();
		
		String VesselType="testtype";
		
		VLL.setType(VesselType);
		
		VesselPoint[] Coordinates=new VesselPoint[VesselPointList.size()];

		
		for (int j=0;j<VesselPointList.size();j++)
		{
			Coordinates[j]=VesselPointList.get(j);
		}
		
		System.out.println("New VesselPoint[]:"+Coordinates.toString());
		
		System.out.println("********************************");
		
		VLL.setCoordinates(Coordinates);

		
		System.out.println("Finally:"+VLL.toString());
		
		System.out.println("********************************");
		
	}
	

}
