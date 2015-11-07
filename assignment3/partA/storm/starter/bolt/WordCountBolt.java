package storm.starter.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseBasicBolt {
    Map<String, Long> counts;

    public WordCountBolt(){
    	counts = new HashMap<String, Long>();
    	System.out.println("wordcount constructed!");
    }
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      System.out.println("wordcount!!!");
      String word = tuple.getString(0);
      Long count = counts.get(word);
      if (count == null)
        count = 0l;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
      
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
}