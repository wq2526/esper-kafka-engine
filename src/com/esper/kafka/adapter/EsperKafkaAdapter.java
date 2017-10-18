package com.esper.kafka.adapter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.esper.client.EsperClient;
import com.esper.kafka.listener.EsperKafkaConsumerListener;
import com.esper.kafka.listener.EsperKafkaProducerListener;
import com.esper.kafka.records.EsperKafkaState;
import com.esper.kafka.records.EsperKafkaStateManager;
import com.kafka.client.KafkaConsumerClient;
import com.kafka.client.KafkaProducerClient;

public class EsperKafkaAdapter {
	
	private static final Log LOG = LogFactory.getLog(EsperKafkaAdapter.class);
	private ExecutorService exec;
	
	//The kafka client
	private KafkaProducerClient<String, String> producer;
	private KafkaConsumerClient<String, String> consumer;
	
	//esper client
	private EsperClient esperClient;
	
	//The information to process a event
	private String kafkaServer;
	private static String eventType;
	private String epl;
	private static String outType;
	private String groupId;
	private String inputTopic;
	private String outputTopic;
	private Set<String> parents;
	
	public static String NODENAME = "";
	
	private Options opts;
	
	public EsperKafkaAdapter() {
		exec = Executors.newCachedThreadPool();
		
		esperClient = new EsperClient();		
		
		kafkaServer = "";
		eventType = "";
		epl = "";
		outType = "";
		groupId = "";
		inputTopic = "";
		outputTopic = "";
		parents = new HashSet<String>();
		
		opts = new Options();
	}
	
	public void init(String[] args) throws ParseException {
		opts.addOption("kafka_server", true, "The kafka server address");
		opts.addOption("event_type", true, "The event type to be processed");
		opts.addOption("epl", true, "The epl to process the event");
		opts.addOption("out_type", true, "The output event type");
		opts.addOption("group_id", true, "The group id of the consumer");
		opts.addOption("input_topic", true, "The topic to subscribe from kafka");
		opts.addOption("output_topic", true, "The topic to publish to kafka");
		opts.addOption("parents", true, "The parents of the node");
		opts.addOption("node_name", true, "The name of the node");
		
		CommandLine cliParser = new GnuParser().parse(opts, args);
		
		kafkaServer = cliParser.getOptionValue("kafka_server", "10.109.253.127:9092");
		eventType = cliParser.getOptionValue("event_type", "air_quality");
		eventType = eventType.replaceAll("%", "\"");
		epl = cliParser.getOptionValue("epl", "select * from air_quality (parameter=$pm25$)");
		epl = epl.replaceAll("$", "'").replaceAll("%", "\"");
		outType = cliParser.getOptionValue("out_type", "air_quality");
		groupId = cliParser.getOptionValue("group_id", "esper-group-test-id");
		inputTopic = cliParser.getOptionValue("input_topic", "topic-0");
		outputTopic = cliParser.getOptionValue("output_topic", "topic-1");
		String parentsstr = cliParser.getOptionValue("parents", "").replaceAll("%", "\"");
		NODENAME = cliParser.getOptionValue("node_name", "");
		
		Logger.getLogger(EsperKafkaAdapter.class);
		FileAppender appender = (FileAppender) Logger.getRootLogger().getAppender("file");
		appender.setFile("/usr/esper/logs/esper-logs-" + NODENAME + ".log");
		appender.activateOptions();
		
		LOG.info("prepare to add event type " + eventType + 
				", with statement " + epl + 
				", from topic " + inputTopic + 
				", from kafka " + kafkaServer + 
				", send processed event to " + outputTopic + 
				", with out type " + outType + 
				", and parents " + parentsstr + 
				", for node " + NODENAME);
		
		JSONArray events = new JSONArray(eventType);
		JSONArray epls = new JSONArray(epl);
		
		for(int i=0;i<events.length();i++) {
			Map<String, Object> def = new HashMap<String, Object>();
			JSONObject event = events.getJSONObject(i);
			String eventName = event.getString("event_type");
			JSONArray eventProps = event.getJSONArray("event_props");
			JSONArray propClasses = event.getJSONArray("event_classes");
			
			if(eventProps.length()!=propClasses.length()){
				throw new RuntimeException("The event prop num do not equal the prop class num");
			}
			
			//set up the event definition
			for(int j=0;j<eventProps.length();j++){
				Object c = new Object();
				
				switch (propClasses.getString(j)){
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
				
				def.put(eventProps.getString(j), c);
				
				LOG.info("Add event property " + 
				eventProps.getString(j) + 
				" with class " + c + 
				" for event type " + eventName);
			}
			
			//add the event type to esper engine
			esperClient.addEventType(eventName, def);
			LOG.info("add event type " + eventName + " for node " + EsperKafkaAdapter.NODENAME);
		}
		
		//add epl
		for(int i=0;i<epls.length();i++) {
			//add the statement
			String stmt = epls.getString(i);
			esperClient.createStmt(stmt);
			LOG.info("create statement " + stmt + " for node " + EsperKafkaAdapter.NODENAME);
		}
		
		//add quit event
		Map<String, Object> quit = new HashMap<String, Object>();
		quit.put("event_type", String.class);
		quit.put("quit", String.class);
		esperClient.addEventType("quit", quit);
		
		JSONArray parentsJson = new JSONArray(parentsstr);
		for(int i=0;i<parentsJson.length();i++){
			parents.add(parentsJson.getString(i));
			String quitStmt = "select * from quit where quit=";
			quitStmt = quitStmt + "\'" + parentsJson.getString(i) + "\'";
			esperClient.createStmt(quitStmt);
			LOG.info("create quit stmt: " + quitStmt);
		}
		LOG.info("The num of parents of the node " + 
				EsperKafkaAdapter.NODENAME + " is " + parents.size());
		
		//setup kafka client 
		producer = new KafkaProducerClient<String, String>(kafkaServer);
		producer.addTopic(outputTopic);
		consumer = new KafkaConsumerClient<String, String>(kafkaServer, groupId);
		consumer.addTopic(inputTopic);
	}
	
	public KafkaProducerClient<String, String> getProducer() {
		return producer;
	}
	
	public KafkaConsumerClient<String, String> getConsumer() {
		return consumer;
	}
	
	public static String getOutType() {
		return outType;
	}
	
	public void start() {
		exec.execute(new KafkaConsumerRunnable());
		exec.execute(new KafkaProducerRunnable());
	}
	
	private class KafkaConsumerRunnable implements Runnable {
		
		private EsperKafkaConsumerListener<String, String> listener;
		
		public KafkaConsumerRunnable() {
			listener = new EsperKafkaConsumerListenerImpl<String, String>(parents);
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			consumer.consume(listener);
		}
		
	}
	
	private class KafkaProducerRunnable implements Runnable {
		
		private EsperKafkaProducerListener<String, String> listener;
		
		public KafkaProducerRunnable() {
			listener = new EsperKafkaProducerListenerImpl<String, String>(parents);
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			listener.init(producer);
		}
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		EsperKafkaAdapter adapter = new EsperKafkaAdapter();
		
		try{
			adapter.init(args);
			adapter.start();
		}catch (ParseException e){
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		while(EsperKafkaStateManager.STATE!=EsperKafkaState.FINISHED
				|| adapter.getProducer().getRunning() 
				|| adapter.getConsumer().getRunning()){
			
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			
		}
		
		LOG.info("esper engine finish for node " + EsperKafkaAdapter.NODENAME);
		System.exit(0);

	}

}
