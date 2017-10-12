package com.esper.kafka.adapters;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import com.esper.kafka.records.EsperKafkaState;
import com.esper.kafka.records.EsperKafkaStateManager;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EPStatementState;
import com.espertech.esper.client.EPStatementStateListener;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.util.JSONEventRenderer;
import com.espertech.esperio.kafka.EsperIOKafkaConfig;
import com.espertech.esperio.kafka.EsperIOKafkaOutputFlowController;
import com.espertech.esperio.kafka.EsperIOKafkaOutputFlowControllerContext;

public class EsperKafkaFlowController implements EsperIOKafkaOutputFlowController {
	
	private static final Log LOG = LogFactory.getLog(EsperKafkaFlowController.class);
	
	private EPServiceProvider engine;
	private Producer<String, String> producer;
	private Set<String> topics;
	

	@Override
	public void close() {
		// TODO Auto-generated method stub
		LOG.info("close the kafka producer");
		producer.close();

	}

	@Override
	public void initialize(EsperIOKafkaOutputFlowControllerContext context) {
		// TODO Auto-generated method stub
		
		LOG.info("flowcontroller init");
		
		engine = context.getEngine();
		
		//obtain producer
		producer = new KafkaProducer<String, String>(context.getProperties());
		
		// determine topics
		topics = new HashSet<String>();
		String topicsCSV = context.getProperties().getProperty(EsperIOKafkaConfig.TOPICS_CONFIG);
        String[] topicNames = topicsCSV.split(",");
        for (String topicName : topicNames) {
            if (topicName.trim().length() > 0) {
                topics.add(topicName.trim());
            }
        }
        
        LOG.info("get output topics " + topics.toString());
        
        // attach to existing statements
        String[] statements = engine.getEPAdministrator().getStatementNames();
        for(String statement : statements){
        	EPStatement stmt = engine.getEPAdministrator().getStatement(statement);
        	stmt.addListener(new KafkaOutputListener(stmt));
        	LOG.info("Added Kafka-Output-Adapter listener to statement" + statement);
        }
        
        // attach listener to receive newly-created statements
        engine.addStatementStateListener(new KafkaStatementListener());
        

	}
	
	private class KafkaOutputListener implements UpdateListener {
		
		private JSONEventRenderer jsonEventRenderer;
		
		public KafkaOutputListener(EPStatement statement) {
			jsonEventRenderer = engine.getEPRuntime().getEventRenderer().
					getJSONRenderer(statement.getEventType());
		}

		@Override
		public void update(EventBean[] newEvents, EventBean[] oldEvents) {
			// TODO Auto-generated method stub
			if(newEvents==null)return;
			for(EventBean event : newEvents){
				String json = jsonEventRenderer.render(event);
				LOG.info("receive event from esper engine: " + json);
				JSONObject out = new JSONObject(json);
				
				if(out.has("quit")){
					EsperKafkaAdapters.QUITNUM--;
					LOG.info("The quit num for node " + EsperKafkaAdapters.NODENAME +
							" is " + EsperKafkaAdapters.QUITNUM);
					if(EsperKafkaAdapters.QUITNUM==0){	
						for(String topic : topics){
							String quitmsg = "{\"event_type\":\"quit\",\"quit\":\"" + EsperKafkaAdapters.NODENAME + "\"}";
							producer.send(new ProducerRecord<String, String>(topic, quitmsg));
							LOG.info("send quit message to kafka " + quitmsg + 
									", to topic " + topic);
						}
						EsperKafkaStateManager.STATE = EsperKafkaState.FINISHED;
						LOG.info(EsperKafkaStateManager.STATE);
					}
				}else{
					if(!out.has("event_type")){
						out.put("event_type", EsperKafkaAdapters.getOutType());
						json = out.toString();
					}
					for(String topic : topics){
						producer.send(new ProducerRecord<String, String>(topic, json));
						LOG.info("send message to kafka " + json + 
								", to topic " + topic);
					}
				}
			}
			
		}
		
	}
	
	private class KafkaStatementListener implements EPStatementStateListener {

		@Override
		public void onStatementCreate(EPServiceProvider arg0, EPStatement arg1) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onStatementStateChange(EPServiceProvider arg0, EPStatement statement) {
			// TODO Auto-generated method stub
			if (statement.getState() == EPStatementState.STARTED) {
				statement.addListener(new KafkaOutputListener(statement));
            } else if (statement.getState() == EPStatementState.STOPPED || statement.getState() == EPStatementState.DESTROYED) {
            	Iterator<UpdateListener> listeners = statement.getUpdateListeners();
                UpdateListener found = null;
                while (listeners.hasNext()) {
                    UpdateListener listener = listeners.next();
                    if (listener instanceof KafkaStatementListener) {
                        found = listener;
                        break;
                    }
                }
                if (found != null) {
                    statement.removeListener(found);
                }
            }
		}
		
	}

}
