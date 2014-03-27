package storm_test.helloworld;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * 信号源，通过设置时间间隔来发送信号。
 * @author jacky
 *
 */
public class SignalSpout extends BaseRichSpout {

	public static final String CONF_STREAM_ID = "streamid";
	
	public static final String CONF_FIELDS_ID = "fieldsid";
	
	public static final String CONF_INTERVAL_ID = "interval";
	/**
	 * 
	 */
	private static final long serialVersionUID = 5805789137486760422L;

	SpoutOutputCollector _collector;
	
	private String streamId = "signals";
	
	private String fieldsId = "action";
	
	/**
	 * 默认是一分钟发送一次信号
	 */
	private long interval = 10 * 1000;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector = collector;
		if(conf.containsKey(CONF_STREAM_ID)){
			streamId = (String)conf.get(CONF_STREAM_ID);
		}
		if(conf.containsKey(CONF_FIELDS_ID)){
			fieldsId = (String)conf.get(CONF_FIELDS_ID);
		}
		if(conf.containsKey(CONF_INTERVAL_ID)){
			interval = Long.parseLong((String)conf.get(CONF_INTERVAL_ID));
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
