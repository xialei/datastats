package com.csf.ada.datastats.log;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.csf.ada.datastats.SimpleSpout;

@SuppressWarnings("serial")
public class LogSpout extends SimpleSpout {

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
	}

	@Override
	public void nextTuple() {
		//collector.emit(new Values("sh"), "sh");
		collector.emit(new Values("aws"), "aws");
	}

	@Override
	public void ack(Object msgId) {
	}

	@Override
	public void fail(Object msgId) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("dc"));
	}

}
