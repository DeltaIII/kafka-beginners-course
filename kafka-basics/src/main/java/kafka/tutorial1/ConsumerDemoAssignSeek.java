package kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafka.constants.NetworkConstants;
import org.kafka.constants.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {

	public static void main(final String[] args) {

		final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

		final String bootstrapServers = NetworkConstants.BOOTSTRAP_SERVER;
		final String topic = Topics.FIRST;

		// create consumer configs
		final Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		try (
				// create consumer
				KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

			// assign and seek are mostly used to replay data or fetch a specific message

			// assign
			final TopicPartition partitionToReadFrom = new TopicPartition(topic, Integer.parseInt(args[0]));
			final long offsetToReadFrom = Long.parseLong(args[1]);
			consumer.assign(Arrays.asList(partitionToReadFrom));

			// seek
			consumer.seek(partitionToReadFrom, offsetToReadFrom);

			final int numberOfMessagesToRead = 5;
			boolean keepOnReading = true;
			int numberOfMessagesReadSoFar = 0;

			// poll for new data
			while (keepOnReading) {
				final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka
																										// 2.0.0

				for (final ConsumerRecord<String, String> record : records) {
					numberOfMessagesReadSoFar += 1;
					logger.info("Key: " + record.key() + ", Value: " + record.value());
					logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
					if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
						keepOnReading = false; // to exit the while loop
						break; // to exit the for loop
					}
				}
			}

			logger.info("Exiting the application");
		}
	}
}
