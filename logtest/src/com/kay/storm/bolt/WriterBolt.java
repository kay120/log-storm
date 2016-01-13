package com.kay.storm.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.base.BaseDateTime;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.kay.hbase.*;
/**
 * �����д���ļ�
 * 
 *
 */
public class WriterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	public static Configuration conf;
	int num = 1;	
	
//	private HTable hTable = null;	
//	private List<Put> puts = null;
	HBaseDAO hbasedao = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
		hbasedao = new HBaseDAOImp();
//		puts = new ArrayList<Put>();
//		try {
//			hTable = new HTable(conf, "log");
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}	
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		String date = input.getStringByField("date");
		String time = input.getStringByField("time");
		String accessip_serverip = input.getStringByField("accessip_serverip");
		String servlet = input.getStringByField("servlet");
		int count = input.getIntegerByField("count");

		System.err.println("WriterBolt:" + num++ + "--->" +accessip_serverip+"_"+date + " " + time + ", "+ servlet + "=" + count);
		hbasedao.insert("log", accessip_serverip+"_"+date + " " + time, "accesslog", "servlet" , servlet);
		hbasedao.insert("log", accessip_serverip+"_"+date + " " + time, "accesslog", "count" , count + "");
	}
}
