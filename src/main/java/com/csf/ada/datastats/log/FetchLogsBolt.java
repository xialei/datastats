package com.csf.ada.datastats.log;

import java.util.Date;
import java.util.Map;

import org.bson.types.ObjectId;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.csf.ada.datastats.Constants;
import com.csf.ada.datastats.Helper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

@SuppressWarnings("serial")
public class FetchLogsBolt implements IRichBolt {

	private OutputCollector collector;

	private DBCollection logColl;

	private DBCollection statsColl;

	private volatile ObjectId lastid = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		statsColl = Helper.db_aws.getCollection(Constants.COLLECTION_LOG_STATS);
	}

	@Override
	public void execute(Tuple input) {

		String dc = input.getStringByField("dc");
		String idfield = "lastid_sh";
		long lastu = -99;
		if (dc.equals("sh")) {
			logColl = Helper.db_sh.getCollection(Constants.COLLECTION_LOG);
			idfield = "lastid_sh";
			lastu = Constants.STATS_LAST_ID;

		} else {
			logColl = Helper.db_aws.getCollection(Constants.COLLECTION_LOG);
			idfield = "lastid_aws";
			lastu = Constants.STATS_LAST_ID + 1;
		}

		DBObject flagObj = new BasicDBObject().append("u", lastu);
		DBObject lastObj = statsColl.findOne(flagObj);

		DBObject qObj = null;

		if (lastObj != null)
			lastid = new ObjectId((String) lastObj.get(idfield));

		try {
			while (true) {

				if (lastid != null)
					qObj = new BasicDBObject("_id", new BasicDBObject("$gt", lastid));

				DBCursor dbcur = logColl
						.find(qObj,
								new BasicDBObject().append("u", 1).append("t", 1).append("c", 1).append("a", 1)
										.append("ts", 1)).sort(new BasicDBObject("_id", 1));

				int count = dbcur.count();
				if (count > 0) {
					System.out.println("processing ... " + count);

					String _id = null;
					while (dbcur.hasNext()) {

						BasicDBObject dbObj = (BasicDBObject) dbcur.next();
						lastid = dbObj.getObjectId("_id");
						_id = lastid.toString();
						long u = dbObj.getLong("u");
						int c = dbObj.getInt("c");
						int a = dbObj.getInt("a");
						int t = dbObj.getInt("t");
						Date d = dbObj.getDate("ts");
						// stupid getYear, java, ah...

						collector.emit(new Values(u, t, d.getYear() + 1900, d.getMonth() + 1, c, a, _id));
					}

					statsColl.update(flagObj, new BasicDBObject().append("u", lastu).append(idfield, _id), true, false,
							WriteConcern.SAFE);

				}

				Utils.sleep(30000);
			}

			// collector.ack(input);
		} catch (Throwable t) {
			t.printStackTrace();
			collector.fail(input);
		}

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
