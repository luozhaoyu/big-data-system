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
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class Q2RandomHashtagSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;
  int sequenceNumber = 0;

  String[] hashtags = new String[]{ "EMABiggestFans1D","ALDubEBTamangPanahon","PushAwardsKathNiels","AMAs","gameinsight","android",
  		"RT","ALDUBBahayNiLOLA","androidgames","OTRASheffield3","PushAwardsLizQuens","Halloween","UKVOTY1D","nowplaying",
  		"job","PSYBulag","GOT7","FOLLOW","voteonedirection","jobs","PSYBwelta","AMAs1D","GetWeird","bencarsonwikipedia",
  		"hsswi","GreysAnatomy","TPP","letschattymatty","SmackDown","NationalDoughnutDay","hero","nfl","Warcraft","USA","america","scandal",
  		"FridayFeeling","BlizzCon","KeystoneXL","Google","Junction2015","VeteransDay", "Xmas", "Christmas", "Thanksgiving", "ISIS",
  		"somoshis15","thefeeling","ikkijkk3","bakeoffitalia","k3zoekk3","falsetesnomtvhits","shrek","y100jingleball","adona060",
  		"savetherest","vzwbuzz","askdemi","runforacure","contantotuitero","tedxsbu","queronotvz","socialsignals","eastenders",
  		"ivotedjosh","ivotedtyler","topdebate","rawrawardskathniels","lontanadame","quiero","gthb20bin","barisaruc","inhumaneseige",
  		"unam","rollersfifthharmony","tvog","nominacionpolemica","changeisgood","liveamp","tvoh","lions","rectorunam","legion","favoriteartistlatin",
  		"kimanioffair","rawgist","lrds","martino","lamafia","hikerchat","echoage","travelskills","ykann","lapelotasiempreal10","buenosmuchachos",
  		"hello_nigeriadotcom","holidaysconfamilia","askpausini","sanayainjdj8onmbc","chinawarnsindia","monchytaseguro","tbs","fafner","anime_k","bambam",
  		"messi","shinmaimaou","coralie","dukenation","hablacfk","thejlawquiz","liveonsc","chanyeol","happy21stleenamday","mansionelan","5daysfor_flipside",
  		"grindhouse","jessickarabid","seguimeytesigoya","unprettyrapstar2","cienciaparasiempre","canalbr800k","kriswufantastic","vedalam","mitam",
  		"hbdkamalbyvijayfans","1dhistory","pushawardslizquens","pushawardskathniels","7daysuntilmitam","gameinsight","ff","rt","amas","android","staracarabia",
  		"job","androidgames","jobs","k3zoektk3","oneweekuntilpurpose","ss9","bizoldukbile","purpose","keystonexl","fansawards2015",
  		"history","nowplaying","news","losmaspopulares2015","madeintheam","periscope","quote","follow","yaassdemi","win","hiring","trecru",
  		"eurekamag","somoshis15","messi","delirium","mgwv","np","dvlzx","brianlanzelotta", "eurovision", "asmsg","cat", "car", "Stockholm", "snow", "data", "system", "Trump", "Palantir", "blue", "badger",
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
		"autograph", "selfie", "fans"
  };
  int interval = 100;
  int len = 10;
  
  public Q2RandomHashtagSpout(int interval){
	  this.interval = interval;
	  this.sequenceNumber = 0;
  }
  
  public Q2RandomHashtagSpout(int interval, int randSize){
	  this.interval = interval;
	  this.len = randSize;
	  this.sequenceNumber = 0;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
    Utils.sleep(interval); 
    String[] sample = new String[len];
    for (int i = 0; i < len; i++){
    	sample[i] = hashtags[_rand.nextInt(hashtags.length)];
    }
    ArrayList<String> sampleList = new ArrayList<String>(Arrays.asList(sample));
    sequenceNumber ++;
    _collector.emit(new Values(sequenceNumber, sampleList));
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	  declarer.declare(new Fields("seq", "hashtags"));
  }

}