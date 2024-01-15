import {Consumer, ConsumerConfig, Kafka, Message, Producer, ProducerConfig, RecordMetadata} from "kafkajs";
import {SchemaRegistry} from "@kafkajs/confluent-schema-registry";
import {KafkaClientProps, PublishProps} from "./types";
import {AvroSchema, Schema} from "@kafkajs/confluent-schema-registry/dist/@types";

/**
 * Kafka client. This class is a wrapper around the kafkajs library and the kafkajs confluent schema registry library.
 * It provides a simple interface to publish and consume messages from a Kafka cluster using Avro schemas.
 * All the messages are encoded and decoded using the schema registry.
 */
export class KafkaClient {
	private kafka: Kafka
	private schemaRegistry: SchemaRegistry

	/**
	 * Creates a new Kafka client
	 * @param props - Kafka client configuration. This includes the Kafka cluster configuration and the schema registry configuration
	 */
	constructor(props: KafkaClientProps) {
		this.kafka = new Kafka(props.cluster)
		this.schemaRegistry = new SchemaRegistry(props.schemaRegistry)
	}

	/**
	 * Publishes one or more messages to a topic
	 * @param {Producer} producer - The producer instance, must be connected and ready to publish
	 * @param {PublishProps} publishData - The publish configuration, this includes the messages to publish and other publish options
	 */
	async publish(producer: Producer, publishData: PublishProps) : Promise<RecordMetadata[]> {
		const messages: Message[] = []

		for (const message of publishData.messages) {
			const encodedMessage = await this.schemaRegistry.encode(message.schemaId, message.value)
			messages.push({
				key: message.key ? Buffer.from(message.key) : undefined,
				value: encodedMessage,
				headers: message.headers,
				timestamp: message.timestamp,
				partition: message.partition,
			})
		}

		return await producer.send({
			messages: messages,
			acks: publishData.acks,
			topic: publishData.topic,
			timeout: publishData.timeout,
			compression: publishData.compression
		})

	}

	/**
	 * Creates and returns a producer instance using the provided configuration
	 * @param {ProducerConfig} config - Producer configuration
	 */
	producer(config: ProducerConfig): Producer {
		return this.kafka.producer(config)
	}

	/**
	 * Creates and returns a consumer instance using the provided configuration
	 * @param {ConsumerConfig} config - Consumer configuration
	 */
	consumer(config: ConsumerConfig): Consumer {
		return this.kafka.consumer(config)
	}

	/**
	 * Get schema by id
	 * @param {number} schemaId - The schema id
	 */
	async getSchema(schemaId: number): Promise<AvroSchema> {
		return await this.schemaRegistry.getSchema(schemaId) as AvroSchema
	}
	/**
	 * Get the latest registered schema for a specific topic
	 * @param {string} topic - the topic name
	 */
	async getLatestSchema(topic: string): Promise<AvroSchema> {
		const schemaId = await this.schemaRegistry.getLatestSchemaId(topic)
		return await this.schemaRegistry.getSchema(schemaId) as AvroSchema
	}






}
