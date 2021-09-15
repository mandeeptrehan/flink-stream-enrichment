package com.cloudera.flink.enrichments;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.cloudera.flink.lookup.ProcessingTimeJoin;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class StreamEnrichmentJob {

	private static final Gson gson = new GsonBuilder().create();

	public static void main(String[] args) throws Exception {

		String brokers = args[0];
		String inputTopic = args[1];
		String lookupTopic = args[2];
		String outTopic = args[3];
		String streamGroupId = args[4];
		String lookupGroupId = args[5];
		String streamKey = args[6];
		String lookupKey = args[7];

		// For local execution enable this property
		//System.setProperty("java.security.auth.login.config", "./jaas.conf");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(30000);
		
		Properties producerProperties = new Properties();
		producerProperties.setProperty("bootstrap.servers", brokers);
		producerProperties.setProperty("client.id", "flink-stream-encrichment");
		producerProperties.setProperty("security.protocol", "SASL_SSL");
		producerProperties.setProperty("sasl.mechanism", "GSSAPI");
		producerProperties.setProperty("sasl.kerberos.service.name", "kafka");
		
		// For local execution enable this property
		// producerProperties.setProperty("ssl.truststore.location", "./src/main/resources/truststore.jks");
		
		// Disable these properties while running on local
		producerProperties.setProperty("ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts");


		FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(outTopic,
				new KafkaSerializationSchema<String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
						return new ProducerRecord<byte[], byte[]>(outTopic, element.getBytes(StandardCharsets.UTF_8));
					}
				}, producerProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

		Properties consumerProperties = new Properties();
		consumerProperties.setProperty("bootstrap.servers", brokers);
		consumerProperties.setProperty("group.id", streamGroupId);
		consumerProperties.setProperty("security.protocol", "SASL_SSL");
		consumerProperties.setProperty("sasl.mechanism", "GSSAPI");
		consumerProperties.setProperty("sasl.kerberos.service.name", "kafka");
		
		Properties consumerProperties1 = new Properties();
		consumerProperties1.setProperty("bootstrap.servers", brokers);
		consumerProperties1.setProperty("group.id", lookupGroupId);
		consumerProperties1.setProperty("security.protocol", "SASL_SSL");
		consumerProperties1.setProperty("sasl.mechanism", "GSSAPI");
		consumerProperties1.setProperty("sasl.kerberos.service.name", "kafka");
		

		// For local execution enable this property
		// consumerProperties.setProperty("ssl.truststore.location","./src/main/resources/truststore.jks");
		
		// Disable these properties while running on local
		consumerProperties.setProperty("ssl.truststore.location", "/usr/lib/jvm/java-1.8.0/jre/lib/security/cacerts");
			
		FlinkKafkaConsumer<String> streamConsumer = new FlinkKafkaConsumer<>(inputTopic, new SimpleStringSchema(), consumerProperties);
		streamConsumer.setStartFromLatest();
		
		FlinkKafkaConsumer<String> lookUpConsumer = new FlinkKafkaConsumer<>(lookupTopic, new SimpleStringSchema(), consumerProperties1);
		lookUpConsumer.setStartFromLatest();

		DataStream<String> stream = env.addSource(streamConsumer).name("Stream");
		
		DataStream<String> lookupStream = env.addSource(lookUpConsumer).name("Lookup");

		DataStream<String> enrichedMeasurements = stream
				.map(jsonString -> gson.fromJson(jsonString, JsonObject.class)).name("StreamString2ObjectMap")
				.keyBy(jsonObject -> jsonObject.get(streamKey).getAsString())
				.connect(lookupStream.map(jsonString -> gson.fromJson(jsonString, JsonObject.class)).name("LookupString2ObjectMap").keyBy(jsonObject -> jsonObject.get(lookupKey).getAsString()))
				.process(new ProcessingTimeJoin()).name("EnrichedJsonObject").map(jsonObject -> {
					if (jsonObject.has(lookupKey)) {
						jsonObject.remove(lookupKey);
					}
					return jsonObject.toString();
					}).name("EnrichedJson2String");
			
		enrichedMeasurements.addSink(producer).name("EnrichedTopic");
		
		env.execute();

	}

}
