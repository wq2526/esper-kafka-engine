package com.esper.kafka.adapters;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.espertech.esperio.kafka.EsperIOKafkaConfig;
import com.espertech.esperio.kafka.EsperIOKafkaInputSubscriber;
import com.espertech.esperio.kafka.EsperIOKafkaInputSubscriberContext;

public class EsperKafkaSubscriber implements EsperIOKafkaInputSubscriber {
	
	private static final Log LOG = LogFactory.getLog(EsperKafkaSubscriber.class);
	

	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(EsperIOKafkaInputSubscriberContext context) {
		// TODO Auto-generated method stub
		String topicsCSV = context.getProperties().getProperty(EsperIOKafkaConfig.TOPICS_CONFIG);
        String[] topicNames = topicsCSV.split(",");
        List<String> topics = new ArrayList<>();
        for (String topicName : topicNames) {
            if (topicName.trim().length() > 0) {
                topics.add(topicName.trim());
            }
        }

        LOG.info("subscribe to topics " + topics);
        context.getConsumer().subscribe(topics);
		
	}
	
}
