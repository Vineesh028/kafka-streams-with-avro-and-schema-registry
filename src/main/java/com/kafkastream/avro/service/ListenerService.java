package com.kafkastream.avro.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ListenerService {

	/**
	 * Listening to output topic for user and count
	 * @param consumerRecord
	 */
	@KafkaListener(topics = "${topic.name.output}", properties = { ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
			+ "=org.apache.kafka.common.serialization.LongDeserializer" }, groupId = "${spring.kafka.consumer.group-id}")
	public void consume(ConsumerRecord<String, Long> consumerRecord) {

		System.out.println("user :" + consumerRecord.key() + " count : " + consumerRecord.value());

	}

}
