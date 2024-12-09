package com.kafkastream.avro.service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.kafkastream.avro.model.User;

@Service
public class SenderService {

	@Autowired
	private KafkaTemplate<String, User> kafkaTemplate;

	@Value("${topic.name.input}")
	private String inputTopic;

	/**
	 * Sending same user 2000 times to input topic
	 * 
	 * @param user
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public String send(User user) throws InterruptedException, ExecutionException {

		for (int i = 0; i < 2000; i++) {

			ProducerRecord<String, User> record = new ProducerRecord<>(inputTopic, UUID.randomUUID().toString(), user);
			kafkaTemplate.send(record);

		}

		return "message sent";

	}

}
