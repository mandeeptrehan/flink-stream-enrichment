package com.cloudera.flink.enrichments;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import com.cloudera.flink.lookup.ProcessingTimeJoin;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class StreamEnrichmentJobLocal {

	private static final Gson gson = new GsonBuilder().create();

	public static void main(String[] args) throws Exception {

		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(30000);
		
		TextInputFormat inputFormat = new TextInputFormat(new Path("/Users/test/Downloads/input"));
		inputFormat.setCharsetName("UTF-8");
		
		DataStreamSource<String> stream = env.readFile(
				inputFormat, "/Users/test/Downloads/input", FileProcessingMode.PROCESS_CONTINUOUSLY, 60000l, BasicTypeInfo.STRING_TYPE_INFO);
		
		DataStreamSource<String> lookupStream = env.readFile(
			    new TextInputFormat(new Path("/Users/test/Downloads/lookup")), "/Users/test/Downloads/lookup", FileProcessingMode.PROCESS_CONTINUOUSLY, 60000l, BasicTypeInfo.STRING_TYPE_INFO);

		DataStream<String> enrichedMeasurements = stream
				.map(jsonString -> gson.fromJson(jsonString, JsonObject.class)).name("JsonString2ObjectMap")
				.keyBy(jsonObject -> jsonObject.get("host").getAsString())
				.connect(lookupStream.map(jsonString -> gson.fromJson(jsonString, JsonObject.class)).keyBy(jsonObject -> jsonObject.get("host_name").getAsString()))
				.process(new ProcessingTimeJoin()).map(jsonObject -> jsonObject.toString());
			
		enrichedMeasurements.print();
		
		env.execute();

	}

}
