package com.kafkastream.avro.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.kafkastream.avro.model.User;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class UsersTopology {

	@Value("${topic.name.input}")
	private String inputTopic;

	@Value("${topic.name.output}")
	private String outputTopic;
	
	/**
	 * Streaming from input topic, grouping by first name, storing in counts store and sending counts to output topic
	 * @param streamsBuilder
	 */
	@Autowired
	public void process(StreamsBuilder streamsBuilder) {
		KStream<String, User> messageStream = streamsBuilder.stream(inputTopic);
		
//		messageStream.peek((key, record) -> log
//			            .info("Processing entry with the key " + key + " and value " + record));
//		
		 KTable<String, Long> users = messageStream
			    .groupBy((key,value) -> value.getFirstName().toString())
			    .count(Materialized.as("counts"));

//		 users.toStream().peek((key, val) -> log
//		            .info("Processing entry with the key " + key + " and value " + val));
		 
		 users.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));


	}

}