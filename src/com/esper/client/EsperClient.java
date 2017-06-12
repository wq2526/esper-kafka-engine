package com.esper.client;

import java.util.Map;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;

public class EsperClient {
	
	private EPServiceProvider engine;
	
	public EsperClient() {
		this.engine = EPServiceProviderManager.getDefaultProvider();
	}
	
	public String getEngineURI() {
		return engine.getURI();
	}
	
	public void addEventType(String eventType, Map<String, Object> def) {
		engine.getEPAdministrator().getConfiguration().addEventType(eventType, def);
	}
	
	public void createStmt(String epl) {
		engine.getEPAdministrator().createEPL(epl);
	}

}
