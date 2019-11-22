package kafka.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kafka.constants.NetworkConstants;
import org.kafka.constants.Secrets;
import org.kafka.constants.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	private static final List<String> TERMS_TO_FOLLOW = Lists.newArrayList("software", "bjss", "kafka");

	public static void main(final String[] args) {
		new TwitterProducer().run();
	}

	private static KafkaProducer<String, String> createKafkaProducer() {
		final String bootstrapServers = NetworkConstants.BOOTSTRAP_SERVER;
		// create Producer properties
		final Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create safe Producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(NetworkConstants.NUMBER_OF_RETRIES));
		properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, NetworkConstants.DELIVERY_TIMEOUT);
		properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, NetworkConstants.REQUEST_TIMEOUT);
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 so we can
																							// keep this as 5. Use 1
																							// otherwise.

		// high throughput producer (at the expense of a bit of latency and CPU usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB batch size

		// create the producer
		final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		return producer;
	}

	private static Client createTwitterClient(final BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		final Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		final StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		hosebirdEndpoint.trackTerms(TERMS_TO_FOLLOW);

		// These secrets should be read from a config file
		final Authentication hosebirdAuth = new OAuth1(Secrets.API_KEY, Secrets.API_SECRET, Secrets.ACCESS_TOKEN,
				Secrets.ACCESS_SECRET);

		final ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		final Client hosebirdClient = builder.build();
		return hosebirdClient;
	}

	private static void setupShutdownHooks(final Logger logger, final Client client,
			final KafkaProducer<String, String> producer) {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("stopping application...");
			logger.info("shutting down client from twitter...");
			client.stop();
			logger.info("closing producer...");
			producer.close();
			logger.info("done!");
		}));
	}

	private volatile boolean shouldProduce = true;

	private TwitterProducer() {
	}

	private void run() {
		final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

		logger.info("Setup");

		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

		// create a twitter client
		final Client client = createTwitterClient(msgQueue);

		// create a kafka producer
		final KafkaProducer<String, String> producer = TwitterProducer.createKafkaProducer();

		// add a shutdown hook
		setupShutdownHooks(logger, client, producer);

		// Attempts to establish a connection.
		client.connect();

		// loop to send tweets to kafka
		// on a different thread, or multiple different threads....
		try {
			while (!client.isDone() && this.shouldProduce) {

				final String message = msgQueue.poll(5, TimeUnit.SECONDS);

				if (message != null) {
					this.sendMessageToKafka(logger, producer, message);
				}
			}
		} catch (final InterruptedException e) {
			logger.error("Error in polling tweets:\n", e);
		} finally {
			client.stop();
		}

		logger.info("End of application");
	}

	private void sendMessageToKafka(final Logger logger, final KafkaProducer<String, String> producer,
			final String message) {

		final ProducerRecord<String, String> record = new ProducerRecord<>(Topics.TWITTER, null, message);
		producer.send(record, (recordMetadata, e) -> {
			if (e != null) {
				logger.error("Something bad happened", e);
				this.shouldProduce = false;
			} else {
				logger.info(recordMetadata.topic() + " -- partition --> " + recordMetadata.partition());
			}
		});
	}
}
