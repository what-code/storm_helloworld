package com.b5m.banx.counter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import java.io.*;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

/**
 * Title:BanxCounterTopology.java
 * 
 * Description:BanxCounterTopology.java
 * 
 * Copyright: Copyright (c) 2014-4-22
 * 
 * Company: IZENE Software(Shanghai) Co., Ltd.
 * 
 * @author Shengjie Guo
 * 
 * @version 1.0
 */
public class BanxCounterTopology {
	public static final Logger logger = Logger.getLogger(BanxCounterTopology.class);
	
	private static ClassPathXmlApplicationContext cpa = new ClassPathXmlApplicationContext(
			"classpath:sping-core.xml");
	
	//metaq server、topic、group
	private static  String MEATQ_SERVER = "10.10.100.1:12181,10.10.100.61:12181,10.10.100.62:12181";
	private static  String METAQ_TOPIC = "search-click-pv-data";
	private static  String METAQ_GROUP = "test_topic_02";
	
	//刷新数据的频率（sec）
	private static  int REFRESH_INTERVAL = 60;
	
	//进程数
	private static  int WORKER_NUMS = 6;
	//每个进程的任务数(线程数)
	private static  int TASKS_PER_WORKER = 3;
	
	static{
		Properties pro = new Properties();
		try {
			String path = BanxCounterTopology.class.getClassLoader().getResource("config.properties").getPath();
			logger.info("----path---->" + path + "---zkServer-->" + System.getProperty("user.dir"));
			//pro.load(new FileInputStream(new File(path)));
			pro.load(BanxCounterTopology.class.getResourceAsStream("config.properties"));
			String temp1 = pro.getProperty("zookeeper.server");
			String temp2 = pro.getProperty("metaq.topic");
			String temp3 = pro.getProperty("metaq.group");
			String temp4 = pro.getProperty("refresh_to_db_interval");
			
			if(StringUtils.isNotBlank(temp1)){
				MEATQ_SERVER = temp1;
			}
			
			if(StringUtils.isNotBlank(temp2)){
				METAQ_TOPIC = temp2;
			}
			
			if(StringUtils.isNotBlank(temp3)){
				METAQ_GROUP = temp3;
			}
			
			if(StringUtils.isNotBlank(temp4)){
				try{
					REFRESH_INTERVAL = Integer.parseInt(temp4);
				}catch(Exception e){}
			}
			logger.info("------TopologyConfig---->"  +  temp1 + "---->" + temp2 + "---->" + temp3 + "----->" + temp4);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();
		//config info
		conf.put("meta.topic", METAQ_TOPIC);
		conf.put("interval", (REFRESH_INTERVAL * 1000) + "");
		conf.setDebug(true);

		MetaClientConfig mcc = new MetaClientConfig();
		ZKConfig zkConfig = new ZKConfig();
		zkConfig.zkConnect = MEATQ_SERVER;
		mcc.setZkConfig(zkConfig);
		
		ConsumerConfig cc = new ConsumerConfig();
		cc.setGroup(METAQ_GROUP);
		Scheme scheme = new StringScheme();
		//bolt
		SplitMsgBolt splitBolt = new SplitMsgBolt();
		CountPvOrClickBolt countBolt = new CountPvOrClickBolt();
		GroupByTypeBolt groupByBolt = new GroupByTypeBolt();
		UpdateResultBolt resultBolt = new UpdateResultBolt();
		
		//spout
		MetaSpout ms = new MetaSpout(mcc,cc,scheme);
		SignalSpout signalSpout = new SignalSpout();
		
		//builder
		builder.setSpout("banx_metaq", ms,1);
		builder.setSpout("banx_signal", signalSpout, 1);
		builder.setBolt("banx_split_bolt", splitBolt,WORKER_NUMS*TASKS_PER_WORKER).shuffleGrouping("banx_metaq").allGrouping("banx_signal", "signals");
		//signals 为SignalSpout的streamId
		builder.setBolt("banx_count_bolt", countBolt,WORKER_NUMS*TASKS_PER_WORKER).fieldsGrouping("banx_split_bolt", new Fields("splitGoodsId","keyWordId")).allGrouping("banx_signal", "signals");
		builder.setBolt("group_by_bolt", groupByBolt,1).shuffleGrouping("banx_count_bolt");
		builder.setBolt("result_bolt", resultBolt,1).shuffleGrouping("group_by_bolt");
		
		if (args != null && args.length > 0) {
			//topology 的工作进程数,若worker数量>1，则在某些worker的banx_split_bolt中无法收到消息，但是在其他某个特定的worker中可接受到消息，正常处理；
			//是否某个bolt只在某个worker中运行？有待考证
			conf.setNumWorkers(WORKER_NUMS);
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
