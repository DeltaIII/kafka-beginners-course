package kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafka.constants.Groups;
import org.kafka.constants.NetworkConstants;
import org.kafka.constants.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(final String[] args) {

		final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

		final String bootstrapServers = NetworkConstants.BOOTSTRAP_SERVER;
		final String groupId = Groups.BASICS;
		final String topic = Topics.FIRST;

		// create consumer configs
		final Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// create consumer
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

			// subscribe consumer to our topic(s)
			consumer.subscribe(Arrays.asList(topic));

			// poll for new data
			while (true) {
				final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka
																										// 2.0.0

				for (final ConsumerRecord<String, String> record : records) {
					logger.info("Key: " + record.key() + ", Value: " + record.value());
					logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
				}
			}
		}
	}
}
