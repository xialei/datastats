package com.csf.ada.datastats.hq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.csf.ada.datastats.Constants;
import com.csf.ada.datastats.Helper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

@SuppressWarnings("serial")
public class HangqingBolt implements IRichBolt {

	private OutputCollector collector;

	private static final String hq_s = "http://hq.sinajs.cn/list=";

	private DBCollection hq_db1;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		hq_db1 = Helper.db_dev.getCollection(Constants.COLLECTION_HQ);
	}

	@Override
	public void execute(Tuple input) {

		String secu = input.getStringByField("secu");
		String mkt = input.getStringByField("mkt");
		String csfcode = input.getStringByField("csfcode");

		String reqUrl = hq_s + secu;

		String result = com.aug3.sys.util.CommonHttpUtils.executeGetMothedRequest(reqUrl);

		String[] secus_hq = result.split(";");
		String[] csf = csfcode.split(",");

		List<DBObject> list = new ArrayList<DBObject>();
		DBObject m = null;
		int size = secus_hq.length;
		List<String> csfsecu = new ArrayList<String>();
		for (int i = 0; i < size; i++) {
			String[] content_parts = secus_hq[i].split("\"");
			if (content_parts.length > 1) {
				String hq = secus_hq[i].split("\"")[1];
				if (!StringUtils.isBlank(hq)) {
					String[] hq_parts = hq.split(",");

					m = new BasicDBObject();

					m.put("_id", csf[i]);

					if (!mkt.equalsIgnoreCase("HK")) {
						m.put("p", Double.parseDouble(hq_parts[1]));
						m.put("chg", Double.parseDouble(hq_parts[3]));
						m.put("vol", Long.parseLong(hq_parts[4]));
					} else {
						m.put("p", Double.parseDouble(hq_parts[2]));
						m.put("chg", Double.parseDouble(hq_parts[8]));
						m.put("vol", Long.parseLong(hq_parts[12]));
					}
					list.add(m);
					csfsecu.add(csf[i]);
				} else {
					System.out.println(csf[i]);
				}
			}
		}

		hq_db1.remove(new BasicDBObject("_id", new BasicDBObject("$in", csfsecu)));
		hq_db1.insert(list);

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("u", "t", "y", "m", "c", "a", "id"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
