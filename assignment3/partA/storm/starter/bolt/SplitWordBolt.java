package storm.starter.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitWordBolt extends BaseBasicBolt{
	
	
	public ArrayList<String> MySplit(String tweet) {
		ArrayList<String> l = new ArrayList<String>();
		Pattern p = Pattern.compile("\\w+");
		Matcher m = p.matcher(tweet);
		
		while (m.find()) {
			l.add(m.group());
		}
		return l;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		List<String> tweets = (List<String>) input.getValueByField("pickedtweets");
		List<String> words = null;
		for (String t : tweets){
			words = MySplit(t);
			for (String w : words){
				collector.emit(new Values(w));
			}
		}
		System.out.println(tweets + "lalalalalalla" + words.toString());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("word"));
		
	}
}
