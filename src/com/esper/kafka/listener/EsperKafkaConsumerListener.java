package com.esper.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface EsperKafkaConsumerListener<K, V> {
	
	public boolean process(ConsumerRecords<K, V> records);

}
