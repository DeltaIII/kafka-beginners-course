package kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafka.constants.NetworkConstants;
import org.kafka.constants.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

	public static class ConsumerRunnable implements Runnable {

		private final CountDownLatch latch;
		private final KafkaConsumer<String, String> consumer;
		private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

		public ConsumerRunnable(final String bootstrapServers, final String groupId, final String topic,
				final CountDownLatch latch) {
			this.latch = latch;

			// create consumer configs
			final Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			// create consumer
			this.consumer = new KafkaConsumer<>(properties);
			// subscribe consumer to our topic(s)
			this.consumer.subscribe(Arrays.asList(topic));
		}

		@Override
		public void run() {
			// poll for new data
			try {
				while (true) {
					final ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));

					for (final ConsumerRecord<String, String> record : records) {
						this.logger.info("Key: " + record.key() + ", Value: " + record.value());
						this.logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
					}
				}
			} catch (final WakeupException e) {
				this.logger.info("Received shutdown signal!");
			} finally {
				this.consumer.close();
				// tell our main code we're done with the consumer
				this.latch.countDown();
			}
		}

		public void shutdown() {
			// the wakeup() method is a special method to interrupt consumer.poll()
			// it will throw the exception WakeUpException
			this.consumer.wakeup();
		}
	}

	public static void main(final String[] args) {
		final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

		final String bootstrapServers = NetworkConstants.BOOTSTRAP_SERVER;
		final String groupId = "my-sixth-application";
		final String topic = Topics.FIRST;

		// latch for dealing with multiple threads
		final CountDownLatch latch = new CountDownLatch(1);

		// create the consumer runnable
		logger.info("Creating the consumer thread");
		final Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

		// start the thread
		final Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();

		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Caught shutdown hook");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Application has exited");
		}

		));

		try {
			latch.await();
		} catch (final InterruptedException e) {
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing");
		}
	}

	private ConsumerDemoWithThread() {

	}
}
