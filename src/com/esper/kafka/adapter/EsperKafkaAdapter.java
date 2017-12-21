package com.esper.kafka.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
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
	private Set<String> parents;
	
	public static String VERTEXNAME = "";
	
	private Options opts;
	
	public EsperKafkaAdapter() {
		exec = Executors.newCachedThreadPool();
		
		esperClient = new EsperClient();		
		
		eventType = "";
		epl = "";
		outType = "";
		groupId = "";
		parents = new HashSet<String>();
		
		opts = new Options();
		
		Properties prop = new Properties();
		InputStream input = EsperKafkaAdapter.class.
				getClassLoader().getResourceAsStream("cep.properties");
		try {
			prop.load(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("read cep properties error", e);
		}
		
		String host = prop.getProperty("kafka.host");
		String port = prop.getProperty("kafka.port");
		kafkaServer = host + ":" + port;
	}
	
	public void init(String[] args) throws ParseException {
		
		opts.addOption("event_type", true, "The event type to be processed");
		opts.addOption("epl", true, "The epl to process the event");
		opts.addOption("out_type", true, "The output event type");
		opts.addOption("children", true, "The children of the node");
		opts.addOption("parents", true, "The parents of the node");
		opts.addOption("vertex_name", true, "The name of the node");
		
		CommandLine cliParser = new GnuParser().parse(opts, args);
		
		eventType = cliParser.getOptionValue("event_type", "air_quality");
		eventType = eventType.replaceAll("%", "\"");
		epl = cliParser.getOptionValue("epl", "select * from air_quality (parameter=$pm25$)");
		epl = epl.replaceAll("$", "'").replaceAll("%", "\"");
		outType = cliParser.getOptionValue("out_type", "air_quality");

		String childrenstr = cliParser.getOptionValue("children", "").replaceAll("%", "\"");
		String parentsstr = cliParser.getOptionValue("parents", "").replaceAll("%", "\"");
		VERTEXNAME = cliParser.getOptionValue("vertex_name", "");
		groupId = VERTEXNAME + "-group-id";
		
		Logger.getLogger(EsperKafkaAdapter.class);
		FileAppender appender = (FileAppender) Logger.getRootLogger().getAppender("file");
		appender.setFile("/usr/esper/logs/esper-logs-" + VERTEXNAME + ".log");
		appender.activateOptions();
		
		//setup kafka client 
		producer = new KafkaProducerClient<String, String>(kafkaServer);
		consumer = new KafkaConsumerClient<String, String>(kafkaServer, groupId);	
		consumer.addTopic(VERTEXNAME + "-topic");
		
		LOG.info("prepare to add event type " + eventType + 
				", with statement " + epl +  
				", from kafka " + kafkaServer + 
				", with out type " + outType + 
				", and children " + childrenstr + 
				", and parents " + parentsstr + 
				", for node " + VERTEXNAME);
		
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
			LOG.info("add event type " + eventName + " for node " + EsperKafkaAdapter.VERTEXNAME);
		}
		
		//add epl
		for(int i=0;i<epls.length();i++) {
			//add the statement
			String stmt = epls.getString(i);
			esperClient.createStmt(stmt);
			LOG.info("create statement " + stmt + " for node " + EsperKafkaAdapter.VERTEXNAME);
		}
		
		//add quit event
		Map<String, Object> quit = new HashMap<String, Object>();
		quit.put("event_type", String.class);
		quit.put("quit", String.class);
		esperClient.addEventType("quit", quit);
		
		JSONArray childrenJson = new JSONArray(childrenstr);
		for(int i=0;i<childrenJson.length();i++){
			String cNode = childrenJson.getString(i);
			producer.addTopic(cNode + "-topic");
		}
		LOG.info("The num of children of the node " + 
				EsperKafkaAdapter.VERTEXNAME + " is " + childrenJson.length());
		
		JSONArray parentsJson = new JSONArray(parentsstr);
		for(int i=0;i<parentsJson.length();i++){
			String pNode = parentsJson.getString(i);
			parents.add(pNode);
			String quitStmt = "select * from quit where quit=";
			quitStmt = quitStmt + "\'" + parentsJson.getString(i) + "\'";
			esperClient.createStmt(quitStmt);
			LOG.info("create quit stmt: " + quitStmt);
		}
		LOG.info("The num of parents of the node " + 
				EsperKafkaAdapter.VERTEXNAME + " is " + parents.size());
		
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
			listener = new EsperKafkaConsumerListenerImpl<String, String>(parents, esperClient);
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
		
		LOG.info("esper engine finish for node " + EsperKafkaAdapter.VERTEXNAME);
		System.exit(0);

	}

}
