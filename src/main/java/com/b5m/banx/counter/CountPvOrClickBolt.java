package com.b5m.banx.counter;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Title:CountPvOrClickBolt.java
 * 
 * Description:CountPvOrClickBolt.java
 * 
 * Copyright: Copyright (c) 2014-4-22
 * 
 * Company: IZENE Software(Shanghai) Co., Ltd.
 * 
 * @author Shengjie Guo
 * 
 * @version 1.0
 */
public class CountPvOrClickBolt  extends BaseBasicBolt{

	public static final Logger log = Logger.getLogger(CountPvOrClickBolt.class);
	private static final long serialVersionUID = 8870844467709865581L;
	public ConcurrentHashMap<String,Integer> map = new ConcurrentHashMap<String,Integer>(2048);

	public void execute(Tuple input, BasicOutputCollector collector) {
		String componentId = input.getSourceComponent();
		//if banx signal,emit to update bolt
		if (componentId.equals("banx_signal")) {
			ConcurrentHashMap<String,Integer> tempMap = new ConcurrentHashMap<String,Integer>();
			tempMap.putAll(map);
			collector.emit(new Values(tempMap));
			log.info("---MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM----->" + map);
			map.clear();
		//count pv or click
		}else{
			StringBuffer goodsId = new StringBuffer(input.getString(0));
			String originMsg = input.getString(2);
			//log.info("---oooooooooooooooooooooooooooooooooooooooooooooooooooooo----->" + originMsg);
			String arr[] = originMsg.split(",");
			if(arr.length == 6){
				//key=goodsId+keyWord+keyWordId+hour+type
				String keyWord = arr[1];
				String keyWordId = arr[2];
				String hour = arr[3];
				String date = arr[4];
				String type = arr[5];
				String key = goodsId.append("_").append(keyWord).append("_").append(keyWordId).append("_").append(hour).append("_").append(date).append("#").append(type).toString();
				//count
				Integer count = map.get(key);
				if(count == null){
					map.put(key, 1);
				}else{
					map.put(key, count+1);
				}
			}

		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("resultMap"));
	}

}
