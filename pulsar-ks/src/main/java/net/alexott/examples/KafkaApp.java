package net.alexott.examples;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaApp {

	public static void main(String[] args) {
		String topic = "persistent://sample/standalone/ns1/test1";

		Properties props = new Properties();
		// Point to a Pulsar service
		props.put("bootstrap.servers", "pulsar://localhost:6650");
		props.put("group.id", "my-subscription-name");
		props.put("enable.auto.commit", "false");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());

		try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Collections.singletonList(topic));

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				records.forEach(record -> {
					System.out.println("Received record: " + record);
				});

				// Commit last offset
				consumer.commitSync();
			}
		}
	}

}
