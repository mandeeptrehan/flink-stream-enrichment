package com.cloudera.flink.lookup;

import java.util.Map;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/* 
 * This class combines reference data stored in flink state with the stream data
 */
public class ProcessingTimeJoin extends CoProcessFunction<JsonObject, JsonObject, JsonObject> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private ValueState<JsonObject> referenceDataState = null;


	public ProcessingTimeJoin() {
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(25))
				.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
				.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();

		ValueStateDescriptor<JsonObject> cDescriptor = new ValueStateDescriptor<>(
				"referenceData", TypeInformation.of(JsonObject.class)
		);
		cDescriptor.enableTimeToLive(ttlConfig);
		
		referenceDataState = getRuntimeContext().getState(cDescriptor);
		
	}

	@Override
	public void processElement1(JsonObject stream, Context context,
			Collector<JsonObject> out) throws Exception {
		if (referenceDataState.value()!=null) { 
			for (Map.Entry<String, JsonElement> data: referenceDataState.value().entrySet()) {
				stream.add(data.getKey(), data.getValue());
			}
		}
	
		out.collect(stream);
	}

	@Override
	public void processElement2(JsonObject referenceData, Context context,
			Collector<JsonObject> collector) throws Exception {
		referenceDataState.update(referenceData);
		
	}

}