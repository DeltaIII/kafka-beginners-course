package org.kafka.constants;

/**
 * Networking constants for connecting to different kafka hosts in the swarm
 */
public final class NetworkConstants {

	public static final String BROKER_HOST_NAME = "kafka_kafka";
	public static final String BROKER_PORT = "9092";
	public static final String BOOTSTRAP_SERVER = BROKER_HOST_NAME + ":" + BROKER_PORT;

	public static final String ZOOKEEPER_HOST_NAME = "kafka_zookeeper";
	public static final String ZOOKEEPER_PORT = "2181";
	public static final String ZOOKEEPER_SERVER = ZOOKEEPER_HOST_NAME + ":" + ZOOKEEPER_PORT;

	public static final int NUMBER_OF_RETRIES = 5;

}
