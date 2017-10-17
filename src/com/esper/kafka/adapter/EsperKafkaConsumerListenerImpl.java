package com.esper.kafka.adapter;

import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;

import com.esper.client.EsperClient;
import com.esper.kafka.listener.EsperKafkaConsumerListener;
import com.esper.kafka.records.EsperKafkaState;
import com.esper.kafka.records.EsperKafkaStateManager;

public class EsperKafkaConsumerListenerImpl<K, V> 
implements EsperKafkaConsumerListener<K, V> {
	
	private static final Log LOG = LogFactory.getLog(EsperKafkaConsumerListenerImpl.class);
	private Set<String> parents;
	
	public EsperKafkaConsumerListenerImpl(Set<String> parents) {
		this.parents = parents;
	}

	@Override
	public boolean process(ConsumerRecords<K, V> records) {
		// TODO Auto-generated method stub
		
		boolean running = true;
		EsperKafkaStateManager.STATE = EsperKafkaState.RUNNING;
		
		for(ConsumerRecord<?, ?> record : records){
			if(record.value()!=null){
				String json = record.value().toString();
				LOG.info("receive message from kafka: " + json);
				JSONObject jsonObj = new JSONObject(json);
				if(jsonObj.has("quit") && parents.contains(jsonObj.getString("quit"))){
					EsperKafkaAdapter.QUITNUM--;
					LOG.info("The quit num for node " + EsperKafkaAdapter.NODENAME +
							" is " + EsperKafkaAdapter.QUITNUM);
					if(EsperKafkaAdapter.QUITNUM==0){
						running = false;
					}
				}
				String eventType = jsonObj.getString("event_type");
				Map<String, Object> event = jsonObj.toMap();
				LOG.info("send event to esper engine: " + jsonObj.toString());
				EsperClient.engine.getEPRuntime().sendEvent(event, eventType);	
			}
		}
		
		return running;
		
	}

}
