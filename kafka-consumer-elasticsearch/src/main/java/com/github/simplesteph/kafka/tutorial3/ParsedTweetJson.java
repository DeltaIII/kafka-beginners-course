package com.github.simplesteph.kafka.tutorial3;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ParsedTweetJson {

	private static final String tweet_id = "id";
	private static final String tweet_entities = "entities";
	private static final String tweet_hashtags = "hashtags";
	private static JsonParser jsonParser = new JsonParser();

	static ParsedTweetJson parse(final String unparsedJson) {
		return new ParsedTweetJson(jsonParser.parse(unparsedJson).getAsJsonObject());
	}

	private final JsonObject tweetJson;

	private ParsedTweetJson(final JsonObject tweetJson) {
		this.tweetJson = tweetJson;
	}

	List<String> getHashTags() {

		final JsonObject entities = this.tweetJson.get(tweet_entities).getAsJsonObject();
		final JsonArray hashtagsJson = entities.get(tweet_hashtags).getAsJsonArray();

		final List<String> hashTags = new LinkedList<>();

		final Iterator<JsonElement> iterator = hashtagsJson.iterator();
		while (iterator.hasNext()) {
			final JsonObject hashtagJson = iterator.next().getAsJsonObject();
			hashTags.add(hashtagJson.get("text").getAsString());
		}
		return hashTags;
	}

	int getTweetId() {
		return this.tweetJson.get(tweet_id).getAsInt();
	}

}
