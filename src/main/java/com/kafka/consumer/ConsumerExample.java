package com.kafka.consumer;

import org.apache.kafka.clients.consumer.*;


import java.util.Arrays;
import java.util.Properties;

/**
 * author yashwant.
 */
public class ConsumerExample {

  public static void main(String[] args) {
	  if(args.length != 2){
		  System.out.println("provide the required 2 arguments ...");
		  System.exit(-1);
	  }
	  String topicName = args[0];
      String bootStrapServers = args[1];
      
    Properties configs = new Properties();
    configs.put("bootstrap.servers", bootStrapServers);
    configs.put("session.timeout.ms", "10000");
    configs.put("group.id", "test");
    configs.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(configs);
    consumer.subscribe(Arrays.asList(topicName));
    while (true) {
      ConsumerRecords<Integer, String> records = consumer.poll(500);
      for (ConsumerRecord<Integer, String> record : records) {
    	  System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());;
        }
      }
    }
  }
