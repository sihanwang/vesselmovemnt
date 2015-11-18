package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload;


import org.apache.hadoop.mapreduce.Partitioner;


public class Partitioner_ShipID extends Partitioner<Key_ShipIDAndRecordTime, TextArrayWritable> {

	
	@Override
	public int getPartition(Key_ShipIDAndRecordTime key, TextArrayWritable value,
			int numReduceTasks) {
		// TODO Auto-generated method stub
		
		return (key.getShipID().hashCode() & Integer.MAX_VALUE) % numReduceTasks;

	}
}
