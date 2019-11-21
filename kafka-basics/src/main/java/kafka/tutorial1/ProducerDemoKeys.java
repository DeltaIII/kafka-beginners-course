package kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

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

public class ProducerDemoKeys {

	public static void main(final String[] args) throws ExecutionException, InterruptedException {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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

			final String topic = Topics.FIRST;
			final String value = "hello world " + Integer.toString(i);
			final String key = "id_" + Integer.toString(i);

			final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

			logger.info("Key: " + key); // log the key
			// id_0 is going to partition 1
			// id_1 partition 0
			// id_2 partition 2
			// id_3 partition 0
			// id_4 partition 2
			// id_5 partition 2
			// id_6 partition 0
			// id_7 partition 2
			// id_8 partition 1
			// id_9 partition 2

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
			}).get(); // block the .send() to make it synchronous - don't do this in production!
		}

		// flush data
		producer.flush();
		// flush and close producer
		producer.close();

	}
}
