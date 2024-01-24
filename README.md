# Schema Safe Kafka Producer

This is a simple Kafka producer that uses the [Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) to ensure that the data being sent to Kafka is compatible with the schema that is registered for the topic.

## Usage

```typescript
import {KafkaClient, Partitioners} from "schema-safe-kafka";

const client = new KafkaClient({
	cluster: {
		clientId: "CLIENT_ID",
		brokers: ["BOOTSTRAP_SERVER"],
		ssl: true,
		sasl: {
			mechanism: "plain",
			username: "API_KEY",
			password: "API_SECRET"
		}
	},
	schemaRegistry: {
		host: "SCHEMA_REGISTRY_URL",
		auth: {
			username: "SCHEMA_REGISTRY_API_KEY",
			password: "SCHEMA_REGISTRY_API_SECRET"
		}
	}
})

const producer = client.producer({
	createPartitioner: Partitioners.LegacyPartitioner,
	allowAutoTopicCreation: false,
	idempotent: true,
})

async function run() {
	await producer.connect()
	await client.publish(producer, {
		topic: "topic-name",
		messages: [{
			key: "key",
			value: {
				"foo": "bar",
				"baz": 1
			},
			headers: {
				"meta": "data"
			},
			schemaId: 1
		}]
	})

	await producer.disconnect()

}

run().catch(console.error)

```
