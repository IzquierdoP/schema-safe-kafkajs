import {KafkaConfig, Message, ProducerRecord} from "kafkajs";
import {SchemaRegistryAPIClientArgs} from "@kafkajs/confluent-schema-registry/dist/api";

export interface KafkaClientProps {
	cluster: KafkaConfig,
	schemaRegistry: SchemaRegistryAPIClientArgs
}
export interface MessageWithSchema extends Message{
	schemaId: number,
}

export interface PublishProps extends ProducerRecord{
	messages: MessageWithSchema[]
}
