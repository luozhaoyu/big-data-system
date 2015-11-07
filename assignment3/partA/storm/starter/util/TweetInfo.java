package storm.starter.util;

import java.util.List;

import twitter4j.HashtagEntity;

public class TweetInfo {
	List<String> hashtags;
	int friendsCount;
	String tweet;
	
	public List<String> getHashtags() {
		return hashtags;
	}

	public void setHashtags(List<String> hashtags) {
		this.hashtags = hashtags;
	}

	public int getFriendsCount() {
		return friendsCount;
	}

	public void setFriendsCount(int friendsCount) {
		this.friendsCount = friendsCount;
	}

	public String getTweet() {
		return tweet;
	}

	public void setTweet(String tweet) {
		this.tweet = tweet;
	}

	public TweetInfo(int friendsCount, List<String> hashtags, String tweet){
		this.friendsCount = friendsCount;
		this.hashtags = hashtags;
		this.tweet = tweet;
	}
	
	@Override
	public String toString(){
//		return "{" + tweet + "|" + Integer.toString(friendsCount) + "|" + hashtags.toString() + "}";
		return "{" + tweet +  "}";
	}
}
