package com.esper.kafka.records;

public class EsperKafkaStateManager {
	
	private EsperKafkaState state;
	
	public EsperKafkaStateManager() {
		state = EsperKafkaState.INIT;
	}

	public EsperKafkaState getState() {
		return state;
	}

	public void setState(EsperKafkaState state) {
		this.state = state;
	}
	
	

}
