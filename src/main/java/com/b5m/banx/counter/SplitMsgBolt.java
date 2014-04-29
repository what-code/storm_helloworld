package com.b5m.banx.counter;

import org.apache.log4j.Logger;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Title:SplitMsgBolt.java
 * 
 * Description:SplitMsgBolt.java
 * 
 * Copyright: Copyright (c) 2014-4-22
 * 
 * Company: IZENE Software(Shanghai) Co., Ltd.
 * 
 * @author Shengjie Guo
 * 
 * @version 1.0
 */
public class SplitMsgBolt extends BaseBasicBolt{
	public static final Logger log = Logger.getLogger(SplitMsgBolt.class);
	private static final long serialVersionUID = 3549969494125687983L;
	private static long TOTAL_MESSAGE = 0;

	public void execute(Tuple input, BasicOutputCollector collector) {
		String componentId = input.getSourceComponent();
		String msg = input.getString(0);
		String arr[] = msg.split(",");
		if(componentId.equals("banx_signal")){
			log.info("---TOTAL_MESSAGE_COUNT--->" + TOTAL_MESSAGE);
		}
		if(arr.length == 6){
			TOTAL_MESSAGE++;
			collector.emit(new Values(arr[0],arr[2],msg));
			//log.info("---@@@@@@@@@@@@@@@@@@@@@@@@@@@@@split--->" + msg);
		}else{
			return;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("splitGoodsId","keyWordId","originMsg"));
	}
}
