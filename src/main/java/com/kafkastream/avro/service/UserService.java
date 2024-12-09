package com.kafkastream.avro.service;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
public class UserService {

	@Autowired
	private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	/**
	 * Returns the number of users with same first name
	 * @param name
	 * @return
	 */
	public Long usersCount(String name) {
		
		 KafkaStreams kafkaStreams =  streamsBuilderFactoryBean.getKafkaStreams();
	        ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams
	            .store(StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore()));
	        return counts.get(name);
	}

}
