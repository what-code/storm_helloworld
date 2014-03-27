package storm_test.helloworld;

import java.util.Map;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Title:Snippet.java
 * 
 * Description:Snippet.java
 * 
 * Copyright: Copyright (c) 2014-3-25
 * 
 * Company: IZENE Software(Shanghai) Co., Ltd.
 * 
 * @author Shengjie Guo
 * 
 * @version 1.0
 */

public class ExclamationBolt1 extends BaseRichBolt{
	
	OutputCollector collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		System.out.println("--ExclamationBolt1--prepare-->" + context.getThisTaskId());
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		if(input.getValue(0) instanceof CounterBean && input.getValue(0) != null){
			CounterBean counter = (CounterBean)input.getValue(0);
			String gid = counter.getId();
			String cat = counter.getSource();
			String compId = input.getSourceComponent();
			String taskId = input.getSourceTask() + "";
			System.out.println("--?????????????????????????##############################compId-->" + compId + "---task--->" + taskId + "----->" + cat);
			collector.emit(input, new Values(gid,cat));
			//collector.emit(new Values(gid,cat));
			collector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("gid","cat"));
	}
}
