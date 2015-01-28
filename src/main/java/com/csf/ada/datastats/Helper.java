package com.csf.ada.datastats;

import java.util.ArrayList;
import java.util.List;

import com.aug3.storage.mongoclient.MongoFactory;
import com.aug3.sys.cache.SystemCache;
import com.aug3.sys.props.ConfigProperties;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;

public class Helper {

	public static ConfigProperties cfg = new ConfigProperties("/datastats.properties");

	public static String dbname = cfg.getProperty("mongodb.dbname");

	public static DB db_sh = new MongoFactory(cfg.getProperty("mongodb.host.sh")).newMongoInstance().getDB(dbname);

	public static DB db_aws = new MongoFactory(cfg.getProperty("mongodb.host.aws")).newMongoInstance().getDB(dbname);

	public static DB db_dev = new MongoFactory(cfg.getProperty("mongodb.host.dev")).newMongoInstance().getDB(dbname);

	@SuppressWarnings("unchecked")
	public static List<String> getSecuList() {

		SystemCache sc = new SystemCache();
		List<String> seculist = (List<String>) sc.get("seculist");

		if (seculist == null) {
			DBCursor cur = Helper.db_dev.getCollection("dict_market").find(
					new BasicDBObject("cov", "1").append("ls", "1").append("mkt",
							new BasicDBObject("$in", new String[] { "A", "H" })));
			List<String> market = new ArrayList<String>();
			while (cur.hasNext()) {
				BasicDBObject dbObj = (BasicDBObject) cur.next();
				market.add(dbObj.getString("code"));
			}

			seculist = Helper.db_dev.getCollection("base_stock").distinct("code",
					new BasicDBObject("cov", 1).append("mkt.code", new BasicDBObject("$in", market)));

			sc.put("seculist", seculist, 3600);
		}

		return seculist;
	}

}
