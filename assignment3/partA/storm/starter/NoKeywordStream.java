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

import java.text.SimpleDateFormat;
import java.util.Date;

import storm.starter.bolt.SmartPrinterBolt;
import storm.starter.spout.TwitterSampleSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class NoKeywordStream {        
    public static void main(String[] args) {
        String consumerKey = "42NRnxnkuqrghnolDWSqbiFyv"; 
        String consumerSecret = "zmwC0g6z1FOBBQigW8w2lrnLYncuH4p3QX25RUCNa8aU1QSCC5"; 
        String accessToken = "2809571326-fyBz1ITFXf4yjuqZvHKgGyy0QcQfNVr8y2OGYq6"; 
        String accessTokenSecret = "MAnEtUccHXheXf0z2pauV75oj2XOm6ag4hiLvbUOh6n6B";
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
        		"swag", "snack", "drink", "alcohol", "peace", "swift", "maroon", "xbox", "surface"};        
        
        
        TopologyBuilder builder = new TopologyBuilder();
        Date date = new Date();
        
        builder.setSpout("twitter", new TwitterSampleSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keyWords));
        builder.setBolt("print", new SmartPrinterBolt(  "/users/zhaoyu/tweets/nokeyword-" + new SimpleDateFormat("MMddHHmmss").format(date) + ".txt"))
                .shuffleGrouping("twitter");
                
                
        Config conf = new Config();
        conf.setDebug(true);
        
        final LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology("test");
				cluster.shutdown();
			}
		});
        //cluster.shutdown();
    }
}
