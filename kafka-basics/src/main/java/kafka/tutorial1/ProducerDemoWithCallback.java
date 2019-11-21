package kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kafka.constants.NetworkConstants;
import org.kafka.constants.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

	public static void main(final String[] args) {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

		final String bootstrapServers = NetworkConstants.BOOTSTRAP_SERVER;

		// create Producer properties
		final Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the producer
		final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		for (int i = 0; i < 10; i++) {
			// create a producer record
			final ProducerRecord<String, String> record = new ProducerRecord<>(Topics.FIRST,
					"hello world " + Integer.toString(i));

			// send data - asynchronous
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(final RecordMetadata recordMetadata, final Exception e) {
					// executes every time a record is successfully sent or an exception is thrown
					if (e == null) {
						// the record was successfully sent
						logger.info("Received new metadata. \n" + "Topic:" + recordMetadata.topic() + "\n"
								+ "Partition: " + recordMetadata.partition() + "\n" + "Offset: "
								+ recordMetadata.offset() + "\n" + "Timestamp: " + recordMetadata.timestamp());
					} else {
						logger.error("Error while producing", e);
					}
				}
			});
		}

		// flush data
		producer.flush();
		// flush and close producer
		producer.close();

	}
}
