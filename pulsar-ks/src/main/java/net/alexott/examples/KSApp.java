package net.alexott.examples;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class KSApp {
	public static void main(String[] args) {
		final Serde<String> stringSerde = Serdes.String();

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KSApp2");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pulsar://localhost:6650");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());		
		props.put("group.id", "test1");
//		props.put("enable.auto.commit", "false");

		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "persistent://sample/standalone/ns1/test1");

		final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

		final KTable<String, String> wordCounts = textLines
				.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase()))).groupBy((key, word) -> word)
				.count("test").mapValues(value -> Long.toString(value));

		// Write the `KTable<String, Long>` to the output topic.
		wordCounts.toStream().to(stringSerde, stringSerde, "persistent://sample/standalone/ns1/test-wc");
		
		final KafkaStreams streams = new KafkaStreams(builder, props);
		streams.cleanUp();
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
