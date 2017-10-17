package com.esper.kafka.listener;

import com.kafka.client.KafkaProducerClient;

public interface EsperKafkaProducerListener<K, V> {
	
	public void init(KafkaProducerClient<K, V> producer);

}
