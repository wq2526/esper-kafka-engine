package com.esper.client;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esper.client.ConfigurationException;
import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EPServiceDestroyedException;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;

public class EsperClient {
	
	private static final Log LOG = LogFactory.getLog(EsperClient.class);
	public static EPServiceProvider engine = EPServiceProviderManager.getDefaultProvider();
	
	public EsperClient() {
		
	}
	
	public void sendEvent(Map<String, Object> event, String type) {
		try {
			engine.getEPRuntime().sendEvent(event, type);
		} catch (EPException e) {
			// TODO Auto-generated catch block
			LOG.info("send event error", e);
		} catch (EPServiceDestroyedException e) {
			// TODO Auto-generated catch block
			LOG.info("send event error", e);
		}
	}
	
	public String getEngineURI() {
		return engine.getURI();
	}
	
	public void addEventType(String eventType, Map<String, Object> def) {
		try {
			engine.getEPAdministrator().getConfiguration().addEventType(eventType, def);
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			LOG.info("add event type error", e);
		} catch (EPServiceDestroyedException e) {
			// TODO Auto-generated catch block
			LOG.info("add event type error", e);
		}
	}
	
	public void createStmt(String epl) {
		try {
			engine.getEPAdministrator().createEPL(epl);
		} catch (EPException e) {
			// TODO Auto-generated catch block
			LOG.info("create statement error", e);
		} catch (EPServiceDestroyedException e) {
			// TODO Auto-generated catch block
			LOG.info("create statement error", e);
		}
	}

}
