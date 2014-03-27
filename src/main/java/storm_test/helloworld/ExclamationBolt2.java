package storm_test.helloworld;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.io.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Title:ExclamationBolt2.java
 * 
 * Description:ExclamationBolt2.java
 * 
 * Copyright: Copyright (c) 2014-3-25
 * 
 * Company: IZENE Software(Shanghai) Co., Ltd.
 * 
 * @author Shengjie Guo
 * 
 * @version 1.0
 */
public class ExclamationBolt2  extends BaseRichBolt{
	private static  ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<String, Integer>();
	OutputCollector collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		System.out.println("--ExclamationBolt2--prepare-->" + context.getThisTaskId());
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String category = tuple.getString(0);
		String componentId = tuple.getSourceComponent();
		//System.out.println("---****************%%%%%%%%%%%%%%%%%%%%%%%%%%%%%****************-->" + componentId + "--category----->" + category);
		if("bolt1".equals(componentId)){
			category = tuple.getString(1);
			System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%-->" + componentId + "--category----->" + category);
			Integer count = map.get(category);
			if (count == null) {
				map.put(category, 1);
			} else {
				map.put(category, count + 1);
			}
		}
		if (componentId.equals("test_signal")) {
			System.out
					.println("----**************************************exclaim2-->"
							+ category
							+ "---taskid--->"
							+ tuple
							+ "----map--->" + map);
			//writeToFile(map);
		}
		collector.emit(tuple, new Values(category));
		collector.ack(tuple);
	}
	
	private void writeToFile(Map map){
		String fileName = "/opt/tools/storm-0.8.2/logs/20140326.log";
		try {
			FileWriter writer = new FileWriter(fileName, true);
			writer.write(map.toString());
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("cat"));
	}

}
