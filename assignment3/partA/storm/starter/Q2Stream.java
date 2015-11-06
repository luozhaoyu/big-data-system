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

import storm.starter.bolt.Q2FilterTweetBolt;
import storm.starter.bolt.Q2PrintTweetBolt;
import storm.starter.spout.Q2FetchTweetSpout;
import storm.starter.spout.Q2RandomFriendsCountSpout;
import storm.starter.spout.Q2RandomHashtagSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class Q2Stream {        
    public static void main(String[] args) {
        String consumerKey = "42NRnxnkuqrghnolDWSqbiFyv"; 
        String consumerSecret = "zmwC0g6z1FOBBQigW8w2lrnLYncuH4p3QX25RUCNa8aU1QSCC5"; 
        String accessToken = "2809571326-fyBz1ITFXf4yjuqZvHKgGyy0QcQfNVr8y2OGYq6"; 
        String accessTokenSecret = "MAnEtUccHXheXf0z2pauV75oj2XOm6ag4hiLvbUOh6n6B";
        int interval = 100;
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("friendsCount", new Q2RandomFriendsCountSpout(interval));
        builder.setSpout("hashtags", new Q2RandomHashtagSpout(interval));
        builder.setSpout("tweets",  new Q2FetchTweetSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, interval));
        builder.setBolt("filter", new Q2FilterTweetBolt())
        	.shuffleGrouping("friendsCount")
        	.shuffleGrouping("hashtags")
        	.shuffleGrouping("tweets");
        builder.setBolt("printer", new Q2PrintTweetBolt())
        	.shuffleGrouping("filter", "filterStream");
                
                
        Config conf = new Config();
        conf.setDebug(true);
        
        final LocalCluster cluster = new LocalCluster();
        StormTopology topo = builder.createTopology();
        cluster.submitTopology("test", conf, topo);
        
        //Utils.sleep(interval * 100);
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
