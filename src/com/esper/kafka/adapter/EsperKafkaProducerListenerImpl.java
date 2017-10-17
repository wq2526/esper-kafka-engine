package com.esper.kafka.adapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import com.esper.client.EsperClient;
import com.esper.kafka.listener.EsperKafkaProducerListener;
import com.esper.kafka.records.EsperKafkaState;
import com.esper.kafka.records.EsperKafkaStateManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.util.JSONEventRenderer;
import com.kafka.client.KafkaProducerClient;

public class EsperKafkaProducerListenerImpl<K, V> 
implements EsperKafkaProducerListener<K, V> {
	
	private static final Log LOG = LogFactory.getLog(EsperKafkaProducerListenerImpl.class);
	private KafkaProducerClient<K, V> producer;

	@Override
	public void init(KafkaProducerClient<K, V> producer) {
		// TODO Auto-generated method stub
		this.producer = producer;
		
		// attach to existing statements
        String[] statements = EsperClient.engine.getEPAdministrator().getStatementNames();
        for(String statement : statements){
        	EPStatement stmt = EsperClient.engine.getEPAdministrator().getStatement(statement);
        	stmt.addListener(new KafkaOutputListener(stmt));
        	LOG.info("Added Kafka-Output-Adapter listener to statement" + statement);
        }

	}
	
	private class KafkaOutputListener implements UpdateListener {
		
		private JSONEventRenderer jsonEventRenderer;
		
		public KafkaOutputListener(EPStatement statement) {
			jsonEventRenderer = EsperClient.engine.getEPRuntime().getEventRenderer().
					getJSONRenderer(statement.getEventType());
		}

		@SuppressWarnings("unchecked")
		@Override
		public void update(EventBean[] newEvents, EventBean[] oldEvents) {
			// TODO Auto-generated method stub
			if(newEvents==null)return;
			//for(EventBean event : newEvents){
				String json = jsonEventRenderer.render(newEvents[0]);
				LOG.info("receive event from esper engine: " + json);
				JSONObject out = new JSONObject(json);
				
				if(out.has("quit")){
					if(EsperKafkaAdapter.QUITNUM==0){
						String quitmsg = "{\"event_type\":\"quit\",\"quit\":\"" + EsperKafkaAdapter.NODENAME + "\"}";
						producer.produce(null, (V) quitmsg);
						LOG.info("send quit message to kafka " + quitmsg);
						producer.close();
						EsperKafkaStateManager.STATE = EsperKafkaState.FINISHED;
						LOG.info(EsperKafkaStateManager.STATE);
					}	
				}else{
					if(!out.has("event_type")){
						out.put("event_type", EsperKafkaAdapter.getOutType());
						json = out.toString();
					}
					producer.produce(null, (V) json);
					LOG.info("send message to kafka " + json);
				}
			}
			
		//}
		
	}

}
