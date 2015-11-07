/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storm.starter;

import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.Q2FilterTweetBolt;
import storm.starter.bolt.Q2PrintTweetBolt;
import storm.starter.bolt.SingleJoinBolt;
import storm.starter.bolt.SplitWordBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.bolt.WordCountBolt;
import storm.starter.spout.Q2RandomFriendsCountSpout;
import storm.starter.spout.Q2RandomHashtagSpout;
import storm.starter.spout.Q2SeqTwitterSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class Q2Stream {        
    public static void main(String[] args) {
        String consumerKey = "42NRnxnkuqrghnolDWSqbiFyv"; 
        String consumerSecret = "zmwC0g6z1FOBBQigW8w2lrnLYncuH4p3QX25RUCNa8aU1QSCC5"; 
        String accessToken = "2809571326-fyBz1ITFXf4yjuqZvHKgGyy0QcQfNVr8y2OGYq6"; 
        String accessTokenSecret = "MAnEtUccHXheXf0z2pauV75oj2XOm6ag4hiLvbUOh6n6B";
        int interval = 2000;
        
        String[] keyWords = new String[]{"cat", "car", "Stockholm", "snow", "data", "system", "Trump", "Palantir", "blue", "badger",
        		"NFL", "apple", "google", "facebook", "perks", "spg", "cajun", "banana", "taco", "whatever", "weareone", "packers", "green",
        		"NBA", "mlb", "dog", "kitten", "blueberry", "romance", "princess", "phone", "nuts", "sheldon", "mad", "talk", "nasty",
        		"procrastination", "cook", "college", "patriots", "dumnass", "dough", "winter", "game", "thrones", "halloween", "warcraft",
        		"hiking", "intern", "park", "sweater", "epic", "dota", "year", "wrath", "waste", "Blake", "street", "toyota", "arrow", 
        		"warning", "travel", "flight", "reject", "karaoke", "bless", "empire", "survivor", "bank", "dating", "restaurant", "tinder",
        		"shopping", "win", "cold", "recap", "cop", "astronaut", "crime", "book", "http", "injured", "china", "awards", "join", 
        		"ugly", "birthday", "friend", "weather", "shirt", "student", "mail", "sleep", "pet", "sea", "dream", "chritmas", "thanksgiving",
        		"vacation", "california", "church", "love", "fuck", "vote", "election", "bernie", "parade", "disney", "today", "city",
        		"marathon", "trade", "cash", "miles", "fun", "work", "free", "photo", "hard", "water", "god", "speech", "gang", "bear", 
        		"stop", "luck", "vegas", "shame", "food", "fool", "weight", "football", "tennis", "concert", "cancer", "stock", "crazy",
        		"ticket", "play", "project", "russia", "cast", "star", "trailer", "yelp", "video", "hawaii", "law", "rage", "comic", "meme",
        		"swag", "snack", "drink", "alcohol", "peace", "swift", "maroon", "xbox", "surface", "flower", "sport", "music", "traffic", "family",
        		"autograph", "selfie", "fans"}; 
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("friendsCount", new Q2RandomFriendsCountSpout(interval));
        builder.setSpout("hashtags", new Q2RandomHashtagSpout(interval, 100));
        builder.setSpout("tweets", new Q2SeqTwitterSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keyWords));
        builder.setBolt("join1", new SingleJoinBolt(new Fields("seq", "friendsCount", "hashtags")))
        	.fieldsGrouping("friendsCount", new Fields("seq"))
        	.fieldsGrouping("hashtags", new Fields("seq"));
        builder.setBolt("join", new SingleJoinBolt(new Fields("friendsCount", "hashtags", "tweetinfolist")))
        	.fieldsGrouping("join1", new Fields("seq"))
        	.fieldsGrouping("tweets", new Fields("seq"));
        builder.setBolt("filter", new Q2FilterTweetBolt())
        	.shuffleGrouping("join");
//        builder.setBolt("printFiltered", new Q2PrintTweetBolt())
//    		.shuffleGrouping("filter");
//        builder.setBolt("cleanWord", new CleanWordsBolt())
//        	.shuffleGrouping("filter", "filterStream");
        builder.setBolt("splitWord", new SplitWordBolt())
        	.shuffleGrouping("filter");
        builder.setBolt("wordCount", new WordCountBolt(), 12)
        	.fieldsGrouping("splitWord", new Fields("word"));
        builder.setBolt("intermediateRanking", new IntermediateRankingsBolt())
        	.shuffleGrouping("wordCount");
        builder.setBolt("totalRanking",  new TotalRankingsBolt())
        	.globalGrouping("intermediateRanking");
        builder.setBolt("printFinal", new Q2PrintTweetBolt())
        	.shuffleGrouping("totalRanking");

                
        Config conf = new Config();
        conf.setDebug(true);
        
        final LocalCluster cluster = new LocalCluster();
        StormTopology topo = builder.createTopology();
        cluster.submitTopology("Q2", conf, topo);
        
        //Utils.sleep(interval * 100);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology("Q2");
				cluster.shutdown();
			}
		});
        //cluster.shutdown();
    }
}
