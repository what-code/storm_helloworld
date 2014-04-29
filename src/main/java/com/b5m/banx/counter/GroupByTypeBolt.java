package com.b5m.banx.counter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.b5m.banx.model.BanxModel;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Title:GroupByTypeBolt.java
 * 
 * Description:GroupByTypeBolt.java
 * 
 * Copyright: Copyright (c) 2014-4-23
 * 
 * Company: IZENE Software(Shanghai) Co., Ltd.
 * 
 * @author Shengjie Guo
 * 
 * @version 1.0
 */
public class GroupByTypeBolt extends BaseBasicBolt{
	public static final Logger log = Logger.getLogger(GroupByTypeBolt.class);
	private static final ConcurrentHashMap<String, BanxModel> COUNT_MAP = new ConcurrentHashMap<String, BanxModel>();

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Map<String,Integer> map = (Map<String,Integer>)input.getValue(0);
		//log.info("----GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG_---->" + map);
		Set<String> keys = map.keySet();
		for(String key : keys){
			Integer count = map.get(key);
			//key=goodsId_keyWord_keyWordId_hour#type
			String arr[] = key.split("#");
			String[] tempArr = arr[0].split("_");
			//tempKey=goodsId_keyWordId_hour
			String tempKey = tempArr[0] + "_" + tempArr[2] + "_" + tempArr[3];
			BanxModel bmTemp = COUNT_MAP.get(tempKey);
			if(bmTemp == null){
				try{
					bmTemp = new BanxModel();
					bmTemp.setGoodsId(Integer.parseInt(tempArr[0]));
					bmTemp.setKeyword(tempArr[1]);
					bmTemp.setKeywordId(Integer.parseInt(tempArr[2]));
					//type, 0:pv,1:click
					if(arr[1].equals("0")){
						bmTemp.setPv(count);
						bmTemp.setClick(0);
					}else{
						bmTemp.setPv(0);
						bmTemp.setClick(count);
					}
					bmTemp.setTimeDate(Long.parseLong(tempArr[4]));
					bmTemp.setTimeHour(Long.parseLong(tempArr[3]));
					//bmTemp.setCreateTime("2014-04-24 16:00:00");
					//bmTemp.setUpdateTime("2014-04-24 17:00:00");
					bmTemp.setTableKeywordId(tempKey);
				}catch(Exception e){
					//e.printStackTrace();
					log.info("----GroupByTypeBolt_ERROR----->" + e.toString());
					continue;
				}
			}else{
				if(arr[1].equals("0")){
					bmTemp.setPv(count);
				}else{
					bmTemp.setClick(count);
				}
			}
			COUNT_MAP.put(tempKey, bmTemp);
		}
		Map tempMap = new ConcurrentHashMap<String, BanxModel>();
		tempMap.putAll(COUNT_MAP);
		collector.emit(new Values(tempMap));
		//log.info("---$$$$$$$$$$$$$$$$$$$$$$$$$$$$$---->" + COUNT_MAP);
		COUNT_MAP.clear();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("countResultMap"));
	}

}
