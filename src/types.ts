import {KafkaConfig, Message, ProducerRecord, Producer as KafkaJsProducer, IHeaders, CompressionTypes} from "kafkajs";
import {SchemaRegistryAPIClientArgs} from "@kafkajs/confluent-schema-registry/dist/api";
import {ConfluentSchemaRegistryValidationError} from "@kafkajs/confluent-schema-registry/dist/errors";

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
  publishingSchemaId?: number
  value: object | null,
  headers?: IHeaders,
  timestamp?: string
  partition?: number

}

/**
 * Publish configuration, this includes the messages to publish and other publish options
 */
export interface PublishProps {
  topic: string
  messages: MessageWithSchema[]
  acks?: number
  timeout?: number
  compression?: CompressionTypes
}

export class SchemaSafeError extends Error {
  public schemaId: number
  public messageKey: string | Buffer | null
  public paths: string[][]

  constructor(error: ConfluentSchemaRegistryValidationError, schemaId: number, messageKey: string | Buffer | null) {
    super(error.message)
    this.name = this.constructor.name
    this.schemaId = schemaId
    this.messageKey = messageKey
    this.paths = error.paths
  }
}

export interface RecordFailedToEncode {
  schemaId: number
  key: string | null
  paths?: string[][]
  error: string
}
