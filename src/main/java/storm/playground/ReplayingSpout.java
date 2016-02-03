package storm.playground;

import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ReplayingSpout extends BaseRichSpout {

    private final int bound;
    private int count;
    private SpoutOutputCollector collector;
    private Map<Object, Integer> cache = new HashMap<>();
    private MultiCountMetric periodMetric;
    private AssignableMetric totalCountMetric;

    public ReplayingSpout(int bound) {
        this.bound = bound;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        periodMetric = new MultiCountMetric();
        totalCountMetric = new AssignableMetric(count);
        topologyContext.registerMetric("ack_fail_count", periodMetric, 5);
        topologyContext.registerMetric("total_sent", totalCountMetric, 5);
    }

    @Override
    public void nextTuple() {
        if (count == bound) {
            return;
        }

        Object key = UUID.randomUUID().toString();
        int value = ++count;
        cache.put(key, value);
        collector.emit(new Values(value), key);
        totalCountMetric.setValue(count);
    }

    @Override
    public void ack(Object msgId) {
        periodMetric.scope("ack").incr();
        cache.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        periodMetric.scope("fail").incr();
        Integer number = cache.get(msgId);
        if (number != null) {
            collector.emit(new Values(number), msgId);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("number"));
    }
}
