package com.esper.kafka.adapters;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.esper.client.EsperClient;
import com.espertech.esperio.kafka.EsperIOKafkaConfig;
import com.espertech.esperio.kafka.EsperIOKafkaInputAdapter;
import com.espertech.esperio.kafka.EsperIOKafkaOutputAdapter;

public class EsperKafkaAdapters {
	
	private static final Log LOG = LogFactory.getLog(EsperKafkaAdapters.class);
	
	//The adapter to connect to kafka
	private EsperIOKafkaInputAdapter inputAdapter;
	private EsperIOKafkaOutputAdapter outputAdapter;
	
	private Properties inputProp;
	private Properties outputProp;
	private EsperClient esperClient;
	
	//The information to process a event
	private String kafkaServer;
	private static String eventType;
	private String epl;
	private String groupId;
	private String inputTopic;
	private String outputTopic;
	private Map<String, Object> def;
	
	private Options opts;
	
	public EsperKafkaAdapters() {
		
		inputProp = new Properties();
		outputProp = new Properties();
		
		esperClient = new EsperClient();		
		
		kafkaServer = "";
		eventType = "";
		epl = "";
		groupId = "";
		inputTopic = "";
		outputTopic = "";
		def = new HashMap<String, Object>();
		
		opts = new Options();

	}
	
	public void init(String[] args) throws ParseException {
		
		opts.addOption("kafka_server", true, "The kafka server address");
		opts.addOption("event_type", true, "The event type to be processed");
		opts.addOption("epl", true, "The epl to process the event");
		opts.addOption("group_id", true, "The group id of the consumer");
		opts.addOption("input_topic", true, "The topic to subscribe from kafka");
		opts.addOption("output_topic", true, "The topic to publish to kafka");
		opts.addOption("event_props", true, "The event properties");
		opts.addOption("prop_classes", true, "The classes of the properties");
		
		CommandLine cliParser = new GnuParser().parse(opts, args);
		
		kafkaServer = cliParser.getOptionValue("kafka_server", "10.109.253.127:9092");
		eventType = cliParser.getOptionValue("event_type", "person_event");
		epl = cliParser.getOptionValue("epl", "select * from person_event");
		groupId = cliParser.getOptionValue("group_id", "esper-group-test-id");
		inputTopic = cliParser.getOptionValue("input_topic", "esper-test-input-topic");
		outputTopic = cliParser.getOptionValue("output_topic", "esper-test-output-topic");
		
		LOG.info("prepare to add event type " + eventType + 
				", with statement " + epl + 
				", from topic " + inputTopic + 
				", from kafka " + kafkaServer + 
				", send processed event to " + outputTopic);
		
		String[] eventProps = cliParser.getOptionValue("event_props", "event_type name age").split(" ");
		String[] propClasses = cliParser.getOptionValue("prop_classes", "String String int").split(" ");
		
		if(eventProps.length!=propClasses.length){
			throw new RuntimeException("The event prop num do not equal the prop class num");
		}
		
		//set up the event definition
		def.put("event_type", String.class);
		for(int i=0;i<eventProps.length;i++){
			Object c = new Object();
			
			switch (propClasses[i]){
			case "String" : c = String.class;break;
			case "int" : c = int.class;break;
			case "char" : c = char.class;break;
			case "boolean" : c = boolean.class;break;
			case "short" : c = short.class;break;
			case "long" : c = long.class;break;
			case "float" : c = float.class;break;
			case "double" : c = double.class;break;
			case "byte" : c = byte.class;break;
			default : c = Object.class;break;
			}
			
			def.put(eventProps[i], c);
			
			LOG.info("Add event property " + eventProps[i] + " with class " + c);
		}
		
		//configure the input adapter
		inputProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		inputProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
				org.apache.kafka.common.serialization.StringDeserializer.class.getName());
		inputProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
				org.apache.kafka.common.serialization.StringDeserializer.class.getName());
		inputProp.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		
		inputProp.put(EsperIOKafkaConfig.INPUT_SUBSCRIBER_CONFIG, EsperKafkaSubscriber.class.getName());
		inputProp.put(EsperIOKafkaConfig.INPUT_PROCESSOR_CONFIG, EsperKafkaProcessor.class.getName());
		inputProp.put(EsperIOKafkaConfig.TOPICS_CONFIG, inputTopic);
		
		inputAdapter = new EsperIOKafkaInputAdapter(inputProp, esperClient.getEngineURI());
		
		LOG.info("successfully configure the input adapter");
		
		//configure the output adapter
		outputProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		outputProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
				org.apache.kafka.common.serialization.StringSerializer.class.getName());
		outputProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
				org.apache.kafka.common.serialization.StringSerializer.class.getName());
		
		outputProp.put(EsperIOKafkaConfig.OUTPUT_FLOWCONTROLLER_CONFIG, 
				EsperKafkaFlowController.class.getName());
		outputProp.put(EsperIOKafkaConfig.TOPICS_CONFIG, outputTopic);
		
		outputAdapter = new EsperIOKafkaOutputAdapter(outputProp, esperClient.getEngineURI());
		
		LOG.info("successfully configure the output adapter");
		
		//add the event type
		esperClient.addEventType(eventType, def);
		LOG.info("add event type " + eventType);
		
		//add the statement
		esperClient.createStmt(epl);
		LOG.info("create statement " + epl);
		
	}
	
	public void start() {
		
		LOG.info("start the input adapter");
		inputAdapter.start();
		
		LOG.info("start the output adapter");
		outputAdapter.start();
		
	}
	
	public void close() {
		LOG.info("close the input adapter");
		inputAdapter.destroy();
		
		LOG.info("close the output adapter");
		outputAdapter.destroy();
	}
	
	public String getEngineURI() {
		return esperClient.getEngineURI();
	}
	
	public static String getEventType() {
		return eventType;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		EsperKafkaAdapters adapters = new EsperKafkaAdapters();
		
		try {
			LOG.info("Initializing esper adapters");
			adapters.init(args);
			
			LOG.info("Start adapters");
			adapters.start();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//adapters.close();

	}

}
