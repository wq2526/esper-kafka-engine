package com.esper.kafka.adapters;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esperio.kafka.EsperIOKafkaInputProcessor;
import com.espertech.esperio.kafka.EsperIOKafkaInputProcessorContext;

public class EsperKafkaProcessor implements EsperIOKafkaInputProcessor {
	
	private static final Log LOG = LogFactory.getLog(EsperKafkaProcessor.class);
	
	private EPServiceProvider engine;

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void init(EsperIOKafkaInputProcessorContext context) {
		// TODO Auto-generated method stub
		engine = context.getEngine();
		LOG.info("processor init");
	}

	@Override
	public void process(ConsumerRecords<Object, Object> records) {
		// TODO Auto-generated method stub
		
		for(ConsumerRecord<?, ?> record : records){
			if(record.value()!=null){
				String json = record.value().toString();
				JSONObject jsonObj = new JSONObject(json);
				LOG.info("receive message from kafka: " + json);
				Map<String, Object> event = jsonObj.toMap();
				LOG.info("send event to esper engine: " + jsonObj.toString());
				engine.getEPRuntime().sendEvent(event, EsperKafkaAdapters.getEventType());
			}
		}
	}
	
}