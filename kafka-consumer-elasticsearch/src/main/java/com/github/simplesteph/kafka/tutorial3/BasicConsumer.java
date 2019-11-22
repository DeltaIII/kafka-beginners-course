package com.github.simplesteph.kafka.tutorial3;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafka.constants.NetworkConstants;
import org.kafka.constants.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicConsumer {

	private static final String CONSUMER_GROUP_ID = "java-consumer-group";

	public static KafkaConsumer<String, String> createConsumer(final String topic) {

		final String bootstrapServers = NetworkConstants.BOOTSTRAP_SERVER;
		final String groupId = CONSUMER_GROUP_ID;

		// create consumer configs
		final Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit of offsets

		// create consumer
		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(topic));

		return consumer;

	}

	public static void main(final String[] args) throws IOException {
		final Logger logger = LoggerFactory.getLogger(BasicConsumer.class.getName());

		final KafkaConsumer<String, String> consumer = createConsumer(Topics.TWITTER);
		try {
			while (true) {
				final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka
																										// 2.0.0

				final int recordCount = records.count();
				logger.info("Received " + recordCount + " records");

				for (final ConsumerRecord<String, String> record : records) {

					final String tweetJsonUnparsed = record.value();
					final ParsedTweetJson tweetJson = ParsedTweetJson.parse(tweetJsonUnparsed);

					final int id = tweetJson.getTweetId();
					final List<String> hashTags = tweetJson.getHashTags();

					logger.info("Tweet: " + id + " : " + hashTags.toString());
				}

			}
		} finally {
			consumer.close();
		}

	}
}
