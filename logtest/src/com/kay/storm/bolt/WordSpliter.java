package com.kay.storm.bolt;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.yaml.snakeyaml.emitter.Emitable;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordSpliter extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		String date = "";
		String time = "";
		String serverip = "";
//		String web = "";
		String servlet = "";
		String accessip ="";
		String[] tmp = line.split("\\t");
		if(tmp.length < 15 ) return;
		date = tmp[0];
		time = tmp[1];
		serverip = tmp[3];		
		servlet = tmp[5];		
		accessip = tmp[8];

		String p1 ="/servlet/com.icbc.inbs.servlet.ICBCINBSEstablishSessionServlet";
		
		String p2 = "/servlet/AsynGetDataServlet";
		boolean bp1 = servlet.matches("(.*)" + p1 + "(.*)");
		boolean bp2 = servlet.matches("(.*)" + p2 + "(.*)");
		if( bp1 || bp2)
		{
			collector.emit(new Values(date,time,accessip + "_" + serverip ,servlet));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date","time","accessip_serverip","servlet"));

	}

}
