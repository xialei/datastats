package com.csf.ada.datastats.hq;

import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.csf.ada.datastats.Helper;
import com.csf.ada.datastats.SimpleSpout;

@SuppressWarnings("serial")
public class SecuritySpout extends SimpleSpout {

	private static final int SPLIT_FACTOR = 80;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
	}

	@Override
	public void nextTuple() {

		List<String> seculist = Helper.getSecuList();

		StringBuilder hk = new StringBuilder();
		StringBuilder hk_csf = new StringBuilder();
		StringBuilder sh = new StringBuilder();
		StringBuilder sh_csf = new StringBuilder();
		StringBuilder sz = new StringBuilder();
		StringBuilder sz_csf = new StringBuilder();

		int hk_num = 0;
		int sh_num = 0;
		int sz_num = 0;

		for (String secu : seculist) {
			String[] parts = secu.split("_");
			if ("HK".equalsIgnoreCase(parts[1])) {
				hk.append("rt_hk").append(parts[0]).append(",");
				hk_csf.append(secu).append(",");
				hk_num += 1;
				if (hk_num % SPLIT_FACTOR == 0) {
					collector.emit(new Values(hk.toString(), "HK", hk_csf.toString()));
					hk.delete(0, hk.length());
					hk_csf.delete(0, hk_csf.length());
				}
			} else if ("SH".equalsIgnoreCase(parts[1])) {

				sh.append("s_sh").append(parts[0]).append(",");
				sh_csf.append(secu).append(",");
				sh_num += 1;
				if (sh_num % SPLIT_FACTOR == 0) {
					collector.emit(new Values(sh.toString(), "SH", sh_csf.toString()));
					sh.delete(0, sh.length());
					sh_csf.delete(0, sh_csf.length());
				}

			} else if ("SZ".equalsIgnoreCase(parts[1])) {
				sz.append("s_sz").append(parts[0]).append(",");
				sz_csf.append(secu).append(",");
				sz_num += 1;
				if (sz_num % SPLIT_FACTOR == 0) {
					collector.emit(new Values(sz.toString(), "SZ", sz_csf.toString()));
					sz.delete(0, sz.length());
					sz_csf.delete(0, sz_csf.length());
				}
			}
		}

		if (hk.length() > 0) {
			collector.emit(new Values(hk.toString(), "HK", hk_csf.toString()));
		}

		if (sh.length() > 0) {
			collector.emit(new Values(sh.toString(), "SH", sh_csf.toString()));
		}

		if (sz.length() > 0) {
			collector.emit(new Values(sz.toString(), "SZ", sz_csf.toString()));
		}

		Utils.sleep(1200000);
	}

	@Override
	public void ack(Object msgId) {
	}

	@Override
	public void fail(Object msgId) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("secu", "mkt", "csfcode"));
	}

}
