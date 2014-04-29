package com.b5m.banx.counter;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Title:SignalSpout.java
 * 
 * Description:SignalSpout.java
 * 
 * Copyright: Copyright (c) 2014-4-22
 * 
 * Company: IZENE Software(Shanghai) Co., Ltd.
 * 
 * @author Shengjie Guo
 * 
 * @version 1.0
 */
public class SignalSpout extends BaseRichSpout {
	private static final long serialVersionUID = 5805789137486760422L;
	
	public static final String CONF_STREAM_ID = "streamid";

	public static final String CONF_FIELDS_ID = "fieldsid";

	public static final String CONF_INTERVAL_ID = "interval";

	SpoutOutputCollector _collector;

	private String streamId = "signals";

	private String fieldsId = "action";

	//默认是1hour发送一次信号
	private long interval = 60 * 10 * 1000;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector = collector;
		if (conf.containsKey(CONF_STREAM_ID)) {
			streamId = (String) conf.get(CONF_STREAM_ID);
		}
		
		if (conf.containsKey(CONF_INTERVAL_ID)) {
			interval = Long.parseLong((String) conf.get(CONF_INTERVAL_ID));
		}
	}

	@Override
	public void nextTuple() {
		Utils.sleep(interval);
		this._collector.emit(streamId, new Values("refreshCache"));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(streamId, new Fields(fieldsId));
	}

}
