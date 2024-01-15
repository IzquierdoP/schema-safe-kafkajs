import {Message} from "kafkajs";

/**
 * Function to get the schema id from an avro encoded message.
 * @param {string} message - The avro encoded message (as a base64 string)
 * @returns {number} - The schema id
 */
export function getSchemaIdFromAvroMessage(message: Message): number {

	const messageBuffer = getAvroMessageBuffer(message)
	const schemaIdBuffer = messageBuffer.subarray(2, 5)
	return schemaIdBuffer.readUintBE(0, schemaIdBuffer.length)

}

export function getAvroMessageBuffer(message: Message): Buffer {
	const value = message.value
	if (!value) {
		throw Error("Message value is null")
	}
	return typeof value === "string" ? Buffer.from(value) : value
}
