package com.b5m.banx.counter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.hibernate3.HibernateTemplate;

import com.b5m.banx.model.BanxModel;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

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
public class UpdateResultBolt  extends BaseBasicBolt{

	public static final Logger log = Logger.getLogger(UpdateResultBolt.class);
	private static final long serialVersionUID = 8870844467709865591L;
	private static ClassPathXmlApplicationContext cpa = new ClassPathXmlApplicationContext(
			"classpath:sping-core.xml");
	
	private HibernateTemplate ht;
		
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.ht = (HibernateTemplate)cpa.getBean("hibernateTemplate");
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		Map<String,BanxModel> map = (Map<String,BanxModel>)input.getValue(0);
		Set<String> keys = map.keySet();
		final List<String> idsList = new ArrayList();
		log.info("---PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPOOOOOOOOOOOOOOOOOO---->" + map.size());
		
		for(String key : keys){
			idsList.add("'" + key + "'");
		}
		String ids = idsList.toString().replace("[", "").replace("]", "");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDate = sdf.format(new Date());
		
		List<BanxModel> addList = new ArrayList<BanxModel>();
		List<BanxModel> updateList = new ArrayList<BanxModel>();
		if(!ids.equals("")){
			List<BanxModel> bmList = ht.find("from BanxModel where tableKeywordId in (" + ids + ")");
			log.info("%%%%%%%%%%%##################ids--->" + ids + "------>" + bmList.size());
			if(bmList.size() == 0){
				for(String key : keys){
					BanxModel bm = map.get(key);
					addList.add(bm);
					bm.setCreateTime(currentDate);
					bm.setUpdateTime(currentDate);
					ht.save(bm);
				}
				//TODO write to db
				if(addList.size() > 0){
					//ht.save(addList);
				}
			}else{
				//update(累加)
				for(String key : keys){
					BanxModel bm = map.get(key);
					boolean flag = false;
					for(BanxModel temp : bmList){
						if(key.equals(temp.getTableKeywordId())){
							bm.setId(temp.getId());
							bm.setClick(temp.getClick() + bm.getClick());
							bm.setPv(temp.getPv() + bm.getPv());
							bm.setCreateTime(temp.getCreateTime());
							bm.setUpdateTime(currentDate);
							flag = true;
							//log.info("---ttttttttemppppppppp--->" + temp);
							break;
						}
					}
					if(!flag){
						addList.add(bm);
						bm.setCreateTime(currentDate);
						bm.setUpdateTime(currentDate);
						ht.save(bm);
					}else{
						updateList.add(bm);
						ht.update(bm);
					}
				}
				
				//TODO write to db
				if(addList.size() > 0){
					//ht.saveOrUpdateAll(addList);
				}
				if(updateList.size() > 0){
					//ht.saveOrUpdateAll(updateList);
					//ht.update(updateList);
				}
			}
		}
		log.info("----%%%%%%%%%%%##################---addList----->" + addList);
		log.info("----%%%%%%%%%%%##################---updateList----->" + updateList);
		
		//TODO 入库操作。记录为空：insert,记录不为空:累加计数并将累加结果更新入DB
		/*ht = (HibernateTemplate)cpa.getBean("hibernateTemplate");
		
		BanxModel bm = new BanxModel();
		bm.setGoodsId(1);
		bm.setKeyword("地方考虑123");
		bm.setKeywordId(12);
		bm.setClick(16);
		bm.setPv(0);
		bm.setTimeHour("2014042216");
		bm.setCreateTime("201404221616");
		bm.setUpdateTime("201404221617");
		bm.setTableKeywordId("t123");
		
		//ht.saveOrUpdate(bm);
		
		bm.setKeyword("99999");
		ht.update(bm);*/
		
		//List<BanxModel> list1 = ht.find("from BanxModel where id=1");
		//System.out.println("----%%%%%%%%%%%%%%%%%%%%%%%%%##################---bolt3----->" + list1.get(0).getKeyword());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	public static void main(String[] args){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String currentDate = sdf.format(new Date());
		System.out.println(currentDate);
		String path = UpdateResultBolt.class.getClassLoader().getResource("config.properties").getPath();
		Properties pro = new Properties();
		try {
			pro.load(new FileInputStream(new File(path)));
			System.out.println("------->" + pro.getProperty("zookeeper.server"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
