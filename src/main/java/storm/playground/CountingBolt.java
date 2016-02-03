package storm.playground;

import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

public class CountingBolt extends BaseBasicBolt {

    private int count;
    private AssignableMetric receivedMetric;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        receivedMetric = new AssignableMetric(count);
        context.registerMetric("total_received", receivedMetric, 5);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        receivedMetric.setValue(++count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // NOP
    }
}
