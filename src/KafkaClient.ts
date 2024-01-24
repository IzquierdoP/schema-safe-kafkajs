import {Consumer, ConsumerConfig, Kafka, Message, ProducerConfig, RecordMetadata} from "kafkajs";
import {SchemaRegistry} from "@kafkajs/confluent-schema-registry";
import {KafkaClientProps, MessageWithSchema, PublishProps, Producer} from "./types";
import {AvroSchema, Schema} from "@kafkajs/confluent-schema-registry/dist/@types";
import {getAvroMessageBuffer, getSchemaIdFromAvroMessage} from "./functions";

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
	async publish(producer: Producer, publishData: PublishProps): Promise<RecordMetadata[]> {

		const encodedMessages = await this._encodeBatch(publishData.messages)

		return await producer.send({
			messages: encodedMessages,
			acks: publishData.acks,
			topic: publishData.topic,
			timeout: publishData.timeout,
			compression: publishData.compression
		})

	}

	/**
	 * Encodes a batch of messages using the schema registry.
	 * @param {MessageWithSchema[]} messages - the messages array to encode, each message hast the schemaId that'll be used.
	 * @private
	 */
	private async _encodeBatch(messages: MessageWithSchema[]): Promise<Message[]> {
		const encodePromises = []

		for (const message of messages) {
			const encodedMessagePromise = this.schemaRegistry.encode(message.schemaId, message.value)
			encodePromises.push(encodedMessagePromise)
		}

		const encodedMessages = await Promise.all(encodePromises)

		const producerMessages: Message[] = encodedMessages.map((encodedMessage, index) => {

			return {
				value: encodedMessage,
				headers: messages[index].headers,
				timestamp: messages[index].timestamp,
				key: messages[index].key,
				partition: messages[index].partition
			}
		})

		return producerMessages
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
	 * Get the latest registered schema for a specific subject
	 * @param {string} subject - the topsubjectic name
	 */
	async getLatestSchema(subject: string): Promise<AvroSchema> {
		const schemaId = await this.schemaRegistry.getLatestSchemaId(subject)
		return await this.schemaRegistry.getSchema(schemaId) as AvroSchema
	}

	/**
	 * Get the latest registered schema id for a specific subject
	 * @param subject
	 */
	async getLatestSchemaId(subject: string) {
		return await this.schemaRegistry.getLatestSchemaId(subject)
	}

	/**
	 * Decodes an avro encoded message, the schemaId is derived from the message itself.
	 * @param message
	 */
	async decodeMessage(message: Message): Promise<any> {
		const decoded = await this.schemaRegistry.decode(getAvroMessageBuffer(message))
		return decoded
	}

}
