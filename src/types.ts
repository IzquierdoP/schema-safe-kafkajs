import {KafkaConfig, Message, ProducerRecord, Producer as KafkaJsProducer, IHeaders, CompressionTypes} from "kafkajs";
import {SchemaRegistryAPIClientArgs} from "@kafkajs/confluent-schema-registry/dist/api";

export type Producer = KafkaJsProducer
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
export interface MessageWithSchema {
	key?: Buffer | string | null
	schemaId: number,
	value: object,
	headers?: IHeaders,
	timestamp?: string
	partition?: number

}

/**
 * Publish configuration, this includes the messages to publish and other publish options
 */
export interface PublishProps{
	topic: string
	messages: MessageWithSchema[]
	acks?: number
	timeout?: number
	compression?: CompressionTypes
}

