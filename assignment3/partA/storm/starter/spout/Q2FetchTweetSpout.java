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

package storm.starter.spout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import storm.starter.util.TweetInfo;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@SuppressWarnings("serial")
public class Q2FetchTweetSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	int interval = 100;
	int sequenceNumber = 0;
	long timestamp = 0;
	ArrayList<TweetInfo> tweetInfoList;
	
	public Q2FetchTweetSpout(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret, int interval) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.interval = interval;
		this.sequenceNumber = 0;
		Date date = new Date();
		timestamp = date.getTime();
		tweetInfoList = new ArrayList<TweetInfo>();
	}
	
	public Q2FetchTweetSpout() {
		// TODO Auto-generated constructor stub
		Date date = new Date();
		timestamp = date.getTime();
		tweetInfoList = new ArrayList<TweetInfo>();
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;

		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
			
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception ex) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

		};

		TwitterStream twitterStream = new TwitterStreamFactory(
				new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		twitterStream.addListener(listener);
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);
		
		twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		//Utils.sleep(interval);
		Status ret = queue.poll();
		if (ret == null){
			Utils.sleep(10);
		}else {
			System.out.println("DEBUG\tFetchTweetSpout\t" + ret.getText());
			if (ret.getLang().equals("en")){
				Date date = new Date();
				long currTime = date.getTime();
				ArrayList<String> hss = new ArrayList<String>();
				for (HashtagEntity htentity : ret.getHashtagEntities()){
					hss.add(htentity.getText());
				}
				tweetInfoList.add(new TweetInfo(ret.getUser().getFriendsCount(), hss, ret.getText()));
								
				if ((currTime - timestamp) > interval){
					timestamp = currTime;
					sequenceNumber ++;
					_collector.emit(new Values(sequenceNumber, tweetInfoList));
					tweetInfoList.clear();
				}
			}
		}
	}

	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("seq", "tweets-friendsCount","tweets-hashtags","tweets-tweetText"));
		declarer.declare(new Fields("seq", "tweetinfolist"));
	}

}
