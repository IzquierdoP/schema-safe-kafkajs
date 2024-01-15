import {KafkaConfig, Message, ProducerRecord} from "kafkajs";
import {SchemaRegistryAPIClientArgs} from "@kafkajs/confluent-schema-registry/dist/api";

/**
 * Kafka client configuration
 */
export interface KafkaClientProps {
	/**
	 * Kafka cluster configuration
	 */
	cluster: KafkaConfig,

	/**
	 * Schema registry configuration
	 */
	schemaRegistry: SchemaRegistryAPIClientArgs
}

/**
 * Message with it's corresponding schema id
 */
export interface MessageWithSchema extends Message{
	/**
	 * The schema id
	 */
	schemaId: number,
}

/**
 * Publish configuration, this includes the messages to publish and other publish options
 */
export interface PublishProps extends ProducerRecord{
	/**
	 * The messages to publish
	 */
	messages: MessageWithSchema[]
}
