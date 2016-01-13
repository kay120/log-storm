package com.kay.storm.spout;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;



import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StormFileSpout extends BaseRichSpout{
	

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private SpoutOutputCollector collector;
//		public StormFileSpout()
//		{
//			
//		}
		@Override
		public void nextTuple() {
			//读取指定目录下所有文件
//			Collection<File> files = FileUtils.listFiles(new File("d:\\test"), new String[]{"txt"}, true);
			Collection<File> files = FileUtils.listFiles(new File("/usr/local/hadoop/test/accesslog"), new String[]{"txt"}, true);
			for (File file : files) {
				try {
					//获取每个文件的所有数据
					LineIterator it = FileUtils.lineIterator(file);
					try {
					    while (it.hasNext()) {
					        String line = it.nextLine();
					        this.collector.emit(new Values(line));
					    }
					} finally {
					    LineIterator.closeQuietly(it);
					}
					
					FileUtils.moveFile(file, new File(file.getAbsolutePath()+System.currentTimeMillis()));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		}

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
//			this.conf = conf;
//			this.context = context;
			this.collector = collector;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("accesslog"));
		}
	
	
	
	
}
