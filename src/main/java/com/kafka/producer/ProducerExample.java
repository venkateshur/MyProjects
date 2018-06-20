package com.kafka.producer;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

/**
 * yashwant
 */
public class ProducerExample {
   public static void main(String[] args) throws Exception{
	   if(args.length != 2){
			  System.out.println("provide the required 2 arguments ...");
			  System.exit(-1);
		  }
          String topicName = args[0];
          String broakerList = args[1];
          
//properties for producer
      Properties props = new Properties();
      props.put("bootstrap.servers", broakerList);
      props.put("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      
//properties for producer
      Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
      
 //send messages to my-topic
      for(int i = 0; i < 100; i++) {
          ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>(topicName, i, "Test Message #" + Integer.toString(i));
      try {
          producer.send(producerRecord, new MyProducerCallback());
          } catch (Exception e) {
            e.printStackTrace();
      }
      System.out.println("AsynchronousProducer call completed");
      producer.close();
    }
  }
}

       
	   