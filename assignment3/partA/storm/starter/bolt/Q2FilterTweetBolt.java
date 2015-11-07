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
package storm.starter.bolt;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import storm.starter.util.MyLog;
import storm.starter.util.TweetInfo;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Q2FilterTweetBolt extends BaseBasicBolt {

	public Q2FilterTweetBolt() {

	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		if (tuple.size() <= 0) {
			MyLog.MyPrint("WRONG\tFilterTweetBolt\t" + tuple.toString(), "/u/y/i/yiran/install/apache-storm-0.9.5/examples/storm-starter/Q2pickedtweets.txt");
		}
		List<String> intervalPickedTweets = new ArrayList<String>();

		int fc = tuple.getIntegerByField("friendsCount");
		List<String> htlist = (List<String>) tuple.getValueByField("hashtags");
		List<TweetInfo> twlist = (List<TweetInfo>) tuple
				.getValueByField("tweetinfolist");
		Set<String> hashtagSet = new HashSet<String>(htlist);

		for (TweetInfo twInfo : twlist) {
			if (twInfo.getFriendsCount() < fc) {
				for (String twHashtag : twInfo.getHashtags()) {
					if (hashtagSet.contains(twHashtag)) {
						intervalPickedTweets.add(twInfo.getTweet());
						break;
					}
				}
			}
		}
		
		String msg;
		if (!intervalPickedTweets.isEmpty()) {
			collector.emit(new Values(intervalPickedTweets));
			for (String t : intervalPickedTweets){
				MyLog.MyPrint("[" + t + "]", "/u/y/i/yiran/install/apache-storm-0.9.5/examples/storm-starter/Q2pickedtweets.txt");
			}
//			msg = intervalPickedTweets.toString();
//			MyLog.MyPrint(msg, "/u/y/i/yiran/install/apache-storm-0.9.5/examples/storm-starter/Q2pickedtweets.txt");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("pickedtweets"));
	}

}
