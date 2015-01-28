package com.csf.ada.datastats;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

import com.csf.ada.datastats.hq.HangqingBolt;
import com.csf.ada.datastats.hq.SecuritySpout;
import com.csf.ada.datastats.log.FetchLogsBolt;
import com.csf.ada.datastats.log.LogSpout;
import com.csf.ada.datastats.log.LogStatsBolt;
import com.csf.ada.datastats.log.WriteStatsToDBBolt;

public class MainStats {

	public static StormTopology buildTopologyForLogStats() {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("log-spout", new LogSpout(), 1);

		builder.setBolt("fetchlogs-bolt", new FetchLogsBolt(), 1).shuffleGrouping("log-spout");

		builder.setBolt("logstats-bolt", new LogStatsBolt(), 1).globalGrouping("fetchlogs-bolt");

		builder.setBolt("writedb-bolt", new WriteStatsToDBBolt(), 1).globalGrouping("logstats-bolt");

		return builder.createTopology();

	}

	public static StormTopology buildTopologyForHQ() {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("security-spout", new SecuritySpout(), 1);

		builder.setBolt("hangqing-bolt", new HangqingBolt(), 3).shuffleGrouping("security-spout");

		return builder.createTopology();

	}

	public static void main(String[] args) {

		Config cfg = new Config();
		// TODO
		cfg.setDebug(true);
		cfg.setNumWorkers(4);
		cfg.setMaxSpoutPending(1);
		cfg.setMaxTaskParallelism(3);

		LocalCluster cluster = new LocalCluster();

		cluster.submitTopology("logStatsTopology", cfg, buildTopologyForLogStats());
//		 cluster.submitTopology("hqTopology", cfg, buildTopologyForHQ());
	}

}
