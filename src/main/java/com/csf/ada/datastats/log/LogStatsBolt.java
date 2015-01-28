package com.csf.ada.datastats.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.csf.ada.datastats.Constants;

@SuppressWarnings("serial")
public class LogStatsBolt implements IRichBolt {

	private OutputCollector collector;

	// u,2014,4 --> {a1->10}
	private ConcurrentHashMap<String, Map<String, Object>> userStatsMap = new ConcurrentHashMap<String, Map<String, Object>>();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		// A thread will periodically trigger next job for writing down stats
		Thread reporter = new Thread() {
			public void run() {
				while (true) {
					synchronized (this) {
						if (userStatsMap.size() > 0) {
							ConcurrentHashMap<String, Map<String, Object>> statsMap = new ConcurrentHashMap<String, Map<String, Object>>();
							statsMap.putAll(userStatsMap);
							LogStatsBolt.this.collector.emit(new Values(statsMap));
							userStatsMap.clear();
						}
					}
					Utils.sleep(10000);
				}
			};
		};
		reporter.start(); // TODO need properly finalizing this thread
	}

	@Override
	public void execute(Tuple input) {

		long u = input.getLongByField("u");
		int t = input.getIntegerByField("t");
		int y = input.getIntegerByField("y");
		int m = input.getIntegerByField("m");

		int c = input.getIntegerByField("c");
		int a = input.getIntegerByField("a");
		String id = input.getStringByField("id");

		String key = u + Constants.KEY_SEP + y + Constants.KEY_SEP + m + Constants.KEY_SEP + t;

		synchronized (this) {

			Map<String, Object> userStats = userStatsMap.get(key);

			if (userStats != null) {

				putStats(userStats, "c", c);
				putStats(userStats, "a", a);
				userStats.put("total", (Long) userStats.get("total") + 1L);

				if (Constants.EXPORT_ACTION == a) {
					putStats(userStats, "e", c);
					if (u > 0) {
						List<String> eidlist = (List<String>) userStats.get("eid");
						if (eidlist == null) {
							eidlist = new ArrayList<String>();
							eidlist.add(id);
						} else {
							eidlist.add(id);
						}
						userStats.put("eid", eidlist);
					}
				}

			} else {

				userStats = new HashMap<String, Object>();
				putStats(userStats, "c", c);
				putStats(userStats, "a", a);
				userStats.put("total", 1L);

				if (Constants.EXPORT_ACTION == a) {
					putStats(userStats, "e", c);
					if (u > 0) {
						List<String> eidlist = new ArrayList<String>();
						eidlist.add(id);
						userStats.put("eid", eidlist);
					}
				}
			}

			userStatsMap.put(key, userStats);
		}
	}

	private void putStats(Map<String, Object> sts, String typ, int typeVal) {
		String key = typ + typeVal;
		if (sts.get(key) != null) {
			sts.put(key, (Long) sts.get(key) + 1L);
		} else {
			sts.put(key, 1L);
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("statsMap"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
