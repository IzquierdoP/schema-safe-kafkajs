import {Consumer, ConsumerConfig, Kafka, Message, Producer, ProducerConfig, RecordMetadata} from "kafkajs";
import {SchemaRegistry} from "@kafkajs/confluent-schema-registry";
import {KafkaClientProps, PublishProps} from "./types";
import {AvroSchema, Schema} from "@kafkajs/confluent-schema-registry/dist/@types";


export class KafkaClient {
	private kafka: Kafka
	private schemaRegistry: SchemaRegistry

	constructor(props: KafkaClientProps) {
		this.kafka = new Kafka(props.cluster)
		this.schemaRegistry = new SchemaRegistry(props.schemaRegistry)
	}

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

	producer(config: ProducerConfig): Producer {
		return this.kafka.producer(config)
	}

	consumer(config: ConsumerConfig): Consumer {
		return this.kafka.consumer(config)
	}

	async getSchema(schemaId: number): Promise<AvroSchema> {
		return await this.schemaRegistry.getSchema(schemaId) as AvroSchema
	}

	async getLatestSchema(topic: string): Promise<AvroSchema> {
		const schemaId = await this.schemaRegistry.getLatestSchemaId(topic)
		return await this.schemaRegistry.getSchema(schemaId) as AvroSchema
	}






}
