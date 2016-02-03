package storm.playground;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;

public class Topology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        // unbounded spout
        builder.setSpout("ReplayingSpout", new ReplayingSpout(-1), 1);
        builder.setBolt("BrokenBolt1", new BrokenBolt(), 100)
                .shuffleGrouping("ReplayingSpout");
        builder.setBolt("BrokenBolt2", new BrokenBolt(), 100)
                .shuffleGrouping("BrokenBolt1");
        builder.setBolt("CountingBolt", new CountingBolt(), 1)
                .globalGrouping("BrokenBolt2");

        Config config = new Config();
        config.registerMetricsConsumer(LoggingMetricsConsumer.class);

        if (args == null || args.length == 0) {
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("local", config, builder.createTopology());
        } else {
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}
