import {Consumer, ConsumerConfig, Kafka, Message, ProducerConfig, RecordMetadata} from "kafkajs";
import {SchemaRegistry} from "@kafkajs/confluent-schema-registry";
import {
  KafkaClientProps,
  MessageWithSchema,
  PublishProps,
  Producer,
  SchemaSafeError,
  RecordFailedToEncode
} from "./types";
import {AvroSchema, Schema} from "@kafkajs/confluent-schema-registry/dist/@types";
import {getAvroMessageBuffer} from "./functions";
import {ConfluentSchemaRegistryValidationError} from "@kafkajs/confluent-schema-registry/dist/errors";

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
  async publish(producer: Producer, publishData: PublishProps): Promise<{
    publishBatchMetadata: RecordMetadata[],
    errorMessages?: RecordFailedToEncode[]
  }> {

    const {encodedMessages, errorMessages} = await this._encodeBatch(publishData.messages)

    const publishBatchMetadata = await producer.send({
      messages: encodedMessages,
      acks: publishData.acks,
      topic: publishData.topic,
      timeout: publishData.timeout,
      compression: publishData.compression
    })

    return {
      publishBatchMetadata,
      errorMessages
    }
  }

  /**
   * Encodes a batch of messages using the schema registry.
   * @param {MessageWithSchema[]} messages - the messages array to encode, each message hast the schemaId that'll be used.
   * @private
   */
  private async _encodeBatch(messages: MessageWithSchema[]): Promise<{
    encodedMessages: Message[],
    errorMessages?: RecordFailedToEncode[]
  }> {

    const encodedValues = []
    const errorMessages: RecordFailedToEncode[] = []

    for (const message of messages) {
      try {
        const encodedMessage = await this._encode(message)
        encodedValues.push(encodedMessage)
      } catch (e: any) {
        errorMessages.push({
          schemaId: message.schemaId,
          key: Buffer.isBuffer(message.key) ? message.key.toString() : message.key ?? null,
          paths: e instanceof SchemaSafeError ? e.paths : [],
          error: e.message
        })
      }
    }

    const encodedMessages: Message[] = encodedValues.map((encodedMessage, index) => {
      return {
        value: encodedMessage,
        headers: messages[index].headers,
        timestamp: messages[index].timestamp,
        key: messages[index].key,
        partition: messages[index].partition
      }
    })

    return {
      encodedMessages: encodedMessages,
      errorMessages: errorMessages.length > 0 ? errorMessages : undefined
    }
  }


  private async _encode(message: MessageWithSchema): Promise<Buffer | null> {
    try {
      if (message.value === null) {
        return null
      }

      const encodedWithMainSchema = await this.schemaRegistry.encode(message.schemaId, message.value)
      // If the message has to be validated with a secondarySchema first, we encode it first and throw
      // an error if the encoding fails.

      const encodedWithPublishingSchema = message.publishingSchemaId
        ? await this.schemaRegistry.encode(message.publishingSchemaId, message.value)
        : undefined


      return encodedWithPublishingSchema ?? encodedWithMainSchema
    } catch (e) {
      if (e instanceof ConfluentSchemaRegistryValidationError) {
        throw new SchemaSafeError(e, message.schemaId, message.key ?? null)
      }
      throw e
    }
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
  async decodeMessage<T>(message: Message): Promise<T | null> {
    if (message.value === null) {
      return null
    }
    const decoded = await this.schemaRegistry.decode(getAvroMessageBuffer(message))
    return decoded
  }

}
