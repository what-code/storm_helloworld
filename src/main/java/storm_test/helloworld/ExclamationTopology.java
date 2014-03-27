package storm_test.helloworld;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.Scheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {
	public static class ExclamationBolt extends BaseRichBolt {
		private static  ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<String, Integer>();
		OutputCollector _collector;

		@Override
		public void prepare(Map conf, TopologyContext context,
				OutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			Values v = new Values(tuple.getString(0) + "!!!");
			String componentId = tuple.getSourceComponent();
			if (componentId.equals("exclaim1")) {
				Integer count = map.get(tuple.getString(0));
				if (count == null) {
					map.put(tuple.getString(0), 1);
				} else {
					map.put(tuple.getString(0), count + 1);
				}
			}
			if (componentId.equals("test_signal")) {
				System.out
						.println("----**************************************exclaim2-->"
								+ v.get(0)
								+ "---taskid--->"
								+ tuple
								+ "----map--->" + map);
			}
			_collector.emit(tuple, v);
			_collector.ack(tuple);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}

	}

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		//----------type1
		/*
		builder.setSpout("word", new MyTestWordSpout(), 1);
		builder.setSpout("test_signal", new SignalSpout(), 1);
		builder.setBolt("exclaim1", new ExclamationBolt(), 6).shuffleGrouping(
				"word");
		//signals 为SignalSpout的streamId
		builder.setBolt("exclaim2", new ExclamationBolt(), 8)
				.fieldsGrouping("exclaim1", new Fields("word"))
				.allGrouping("test_signal", "signals");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(5);
		*/
		
		//-----------type2
		Config conf = new Config();
		//topic
		conf.put("meta.topic", "test_hotel_count");
		conf.setDebug(true);

		MetaClientConfig mcc = new MetaClientConfig();
		ZKConfig zkConfig = new ZKConfig();
		zkConfig.zkConnect = "10.10.100.1:12181";
		mcc.setZkConfig(zkConfig);
		
		ConsumerConfig cc = new ConsumerConfig();
		cc.setGroup("test_gsj_000652");
		
		Scheme scheme = new StringScheme();
		//spout
		MetaSpout ms = new MetaSpout(mcc,cc,scheme);
		SignalSpout ss = new SignalSpout();
		
		//bolt
		ExclamationBolt1 bolt1 = new ExclamationBolt1();
		ExclamationBolt2 bolt2 = new ExclamationBolt2();
		
		//builder
		builder.setSpout("metaq_test", ms,1);
		builder.setSpout("test_signal", ss, 1);
		builder.setBolt("bolt1", bolt1,6).shuffleGrouping("metaq_test");
		//signals 为SignalSpout的streamId
		builder.setBolt("bolt2", bolt2,1).fieldsGrouping("bolt1", new Fields("cat")).allGrouping("test_signal", "signals");
		
		if (args != null && args.length > 0) {
			//topology 的工作进程数,若worker数量>1，则在某些worker的bolt2中无法收到消息，但是在其他某个特定的worker中可接受到消息，正常处理；
			//是否某个bolt只在某个worker中运行？有待考证
			conf.setNumWorkers(1);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}
