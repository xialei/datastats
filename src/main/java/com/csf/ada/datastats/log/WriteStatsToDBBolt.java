package com.csf.ada.datastats.log;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.csf.ada.datastats.Constants;
import com.csf.ada.datastats.Helper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;

@SuppressWarnings("serial")
public class WriteStatsToDBBolt implements IRichBolt {

	private OutputCollector collector;

	//private DBCollection statsColl_sh;

	private DBCollection statsColl_aws;

	private ConcurrentHashMap<String, Map<String, Object>> statsMap;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		//statsColl_sh = Helper.db_sh.getCollection(Constants.COLLECTION_LOG_STATS);

		statsColl_aws = Helper.db_aws.getCollection(Constants.COLLECTION_LOG_STATS);
	}

	@Override
	public void execute(Tuple input) {

		statsMap = (ConcurrentHashMap<String, Map<String, Object>>) input.getValueByField("statsMap");

		for (String uym : statsMap.keySet()) {

			String[] parts = uym.split(Constants.KEY_SEP);

			long u = Long.parseLong(parts[0]);
			int y = Integer.parseInt(parts[1]);
			int m = Integer.parseInt(parts[2]);
			int t = Integer.parseInt(parts[3]);

			BasicDBObject qObj = new BasicDBObject().append("u", u).append("y", y).append("m", m);

			BasicDBObject upsertObj = new BasicDBObject();

			BasicDBObject stsObj = new BasicDBObject();
			Map<String, Object> stats = statsMap.get(uym);
			for (String k : stats.keySet()) {

				if (k.equals("eid")) {
					upsertObj.append("$pushAll", new BasicDBObject().append(k, (List<String>) stats.get(k)));
				} else {
					stsObj.append("v." + k, (Long) stats.get(k));
				}

			}

			upsertObj.append("$inc", stsObj);

			//statsColl_sh.update(qObj, upsertObj, true, false, WriteConcern.SAFE);
			statsColl_aws.update(qObj, upsertObj, true, false, WriteConcern.SAFE);

			// update user yearly stats
			upsertObj.remove("$pushAll");
			qObj.put("m", 0);
			//statsColl_sh.update(qObj, upsertObj, true, false, WriteConcern.SAFE);
			statsColl_aws.update(qObj, upsertObj, true, false, WriteConcern.SAFE);

			// update all user stats
			if (t == 1) {
				qObj.put("u", -1L);
				qObj.put("m", m);
				//statsColl_sh.update(qObj, upsertObj, true, false, WriteConcern.SAFE);
				statsColl_aws.update(qObj, upsertObj, true, false, WriteConcern.SAFE);
			}
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
