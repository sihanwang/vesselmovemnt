package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.filter;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;


public class TheFirstLocationBeforeFilter extends FilterBase {
	
	private String Ship;
	private long Timestamp;
	private boolean filterremaining=false;
	private boolean filterrow=true;
	String row_key;
	
	
	@Override
	public void reset() throws IOException {
		// TODO Auto-generated method stub
		System.out.println("reset()");
		System.out.println();
		filterrow=true;
	}
	
	@Override
	public boolean filterRow() throws IOException {
		// TODO Auto-generated method stub
		System.out.println("filterRow()-->returning:"+filterrow);
		System.out.println();
		return filterrow;
	}
	
	@Override
	public boolean filterAllRemaining() throws IOException {
		// TODO Auto-generated method stub
		System.out.println("filterAllRemaining()-->returning:"+filterremaining);
		System.out.println();
		return filterremaining;
	}
	
	
	@Override
	public boolean filterRowKey(byte[] buffer, int offset, int length)
			throws IOException {
		// TODO Auto-generated method stub
		row_key=Bytes.toString(buffer, offset, length);		
		System.out.println("filterRowKey--->Current row key:"+row_key);
		
		
		String shipid_str=row_key.substring(0, 10);
		
		
		
		if (shipid_str.compareTo(Ship)==0)
		{
			long pos_time= Long.MAX_VALUE-Long.parseLong(row_key.substring(10));
			
			if (pos_time<Timestamp)
			{
				filterremaining=true;
				filterrow=false;
				System.out.println("filterRowKey--->return:"+filterrow);
				System.out.println();
				return filterrow;				
			}
			else
			{
				filterremaining=false;
				filterrow=true;
				System.out.println("filterRowKey--->return:"+filterrow);
				System.out.println();				
				return filterrow;
			}
			
		}
		
		filterremaining=true;
		filterrow=true;
		System.out.println("filterRowKey--->return:"+filterrow);
		System.out.println();
		return filterrow;

	}
	
	
	@Override
	public ReturnCode filterKeyValue(Cell v) throws IOException {
		// TODO Auto-generated method stub
		
		System.out.println("filterKeyValue--->Cell:"+v+"|filterrow:"+filterrow);
		
		
		if (filterrow)
		{
			
			System.out.println("filterKeyValue--->Returning ReturnCode.NEXT_ROW");
			System.out.println();
			return ReturnCode.NEXT_ROW;
		}
		else
		{
			System.out.println("filterKeyValue--->ReturnCode.INCLUDE_AND_NEXT_COL");
			System.out.println();
			return ReturnCode.INCLUDE_AND_NEXT_COL;
		}
	}

	
	public TheFirstLocationBeforeFilter(String shipid, long timestamp)
	{
		this.Ship=shipid;
		this.Timestamp=timestamp;
	}
	
	public byte[] toByteArray() throws IOException
	{
		
		byte[] Timestamp_byte= Bytes.toBytes(this.Timestamp);
		byte[] shipid_byte=Bytes.toBytes(this.Ship);
		
		byte[] total=new byte[Timestamp_byte.length+shipid_byte.length];
		
		
		System.arraycopy(Timestamp_byte,0,total,0,Timestamp_byte.length);
		System.arraycopy(shipid_byte,0,total,Timestamp_byte.length,shipid_byte.length);
		
		return total;
	}
	
	public static TheFirstLocationBeforeFilter parseFrom(final byte [] pbBytes) throws DeserializationException 
	{
		    
		////////////////////////////////////
		long timestamp= Bytes.toLong(pbBytes, 0, 8);
		String ship=Bytes.toString(pbBytes,8,pbBytes.length-8);
		
		return new TheFirstLocationBeforeFilter(ship, timestamp);
	}
	
	
	public static void main(String[] args) throws Exception
	{
		
		//scan 'cdb_vessel:vessel_location',{FILTER=>"(PrefixFilter('0000003162')"}			
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("hbase-site.xml"));


	    Connection connection = ConnectionFactory.createConnection(conf);
	    Table table = connection.getTable(TableName.valueOf("cdb_vessel", "vessel_location"));

	    
	    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	    
	    long record_time=formatter.parse("2014-02-07T23:32:22Z",  new ParsePosition(0)).getTime();
	    
	    TheFirstLocationBeforeFilter filter1 = new TheFirstLocationBeforeFilter("0000003162", record_time);

	    Scan scan = new Scan();
	    scan.setFilter(filter1);
	    ResultScanner scanner = table.getScanner(scan);

	    System.out.println("Results of scan:");

	    for (Result result : scanner) {
	      for (KeyValue kv : result.raw()) {
	        System.out.println("KV: " + kv + ", Value: " +
	          Bytes.toString(kv.getValue()));
	      }
	    }
	    scanner.close();

	}
	
	
	public static String padNum(long num, int pad) {
		String res = Long.toString(num);
		if (pad > 0) {
			while (res.length() < pad) {
				res = "0" + res;
			}
		}
		return res;
	}
	
	
	

}
