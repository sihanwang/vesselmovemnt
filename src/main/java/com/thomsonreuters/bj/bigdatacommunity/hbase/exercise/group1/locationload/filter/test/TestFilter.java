package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.filter.test;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;




public class TestFilter extends FilterBase {

	

	@Override
	public boolean filterAllRemaining() throws IOException {
		// TODO Auto-generated method stub
		System.out.println("filterAllRemaining()------->false");
		System.out.println("");
		return false;
		
	}


	@Override
	public void reset() throws IOException {
		// TODO Auto-generated method stub
		System.out.println("reset()------->");
		System.out.println("");
	}


	@Override
	public boolean filterRow() throws IOException {
		// TODO Auto-generated method stub
		System.out.println("filterRow()------->false");
		System.out.println("");
		return false;
	}

	
	@Override
	public boolean filterRowKey(byte[] buffer, int offset, int length)
			throws IOException {
		// TODO Auto-generated method stub
		String row_key=Bytes.toString(buffer, offset, length);
		
		System.out.println("filterRowKey------->"+row_key+", and return false");		
		System.out.println("");
			
		return false;
	}

	
	int a;
	
	public TestFilter(int arg) {
		a=arg;
	}

	
	boolean flag=false;

	@Override
	public ReturnCode filterKeyValue(Cell cell) throws IOException {

		if (flag)
		{
			System.out.println("filterKeyValue------->"+cell+", and return ReturnCode.NEXT_ROW");		
			System.out.println("");
			flag=false;
			return ReturnCode.NEXT_ROW;
		}
		else
		{
			System.out.println("filterKeyValue------->"+cell+", and return ReturnCode.INCLUDE_AND_NEXT_COL");		
			System.out.println("");
			flag=true;
			return ReturnCode.INCLUDE_AND_NEXT_COL;
		}

	}

	public byte[] toByteArray() throws IOException
	{
		return Bytes.toBytes(a);
	}
	
	
	public static TestFilter parseFrom(final byte [] pbBytes) throws DeserializationException 
	{
		    
		////////////////////////////////////
		int a= Bytes.toInt(pbBytes, 0, 4);
				
		return new TestFilter(a);
	}
	

	public static void main(String[] args) throws IOException
	{
		
		//scan 'cdb_vessel:vessel_location',{FILTER=>"(PrefixFilter('0000003162')"}			
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("hbase-site.xml"));


	    Connection connection = ConnectionFactory.createConnection(conf);
	    Table table = connection.getTable(TableName.valueOf("cdb_vessel", "vessel_location"));
    
	    TestFilter filter1 = new TestFilter(5);

	    Scan scan = new Scan();
	    scan.setStartRow(Bytes.toBytes("0000003162"+padNum(0, 19)));
	    scan.setFilter(filter1);
	    scan.setMaxResultSize(1);
	    
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
