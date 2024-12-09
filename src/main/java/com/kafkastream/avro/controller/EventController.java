package com.kafkastream.avro.controller;

import java.util.concurrent.ExecutionException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.kafkastream.avro.model.User;
import com.kafkastream.avro.service.SenderService;
import com.kafkastream.avro.service.UserService;

import lombok.AllArgsConstructor;




@RestController
@AllArgsConstructor
public class EventController {

	private final SenderService senderService;
	
	private final UserService userService;

    /**
     * Send users details to topic
     * @param user
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @PostMapping("/event")
    public ResponseEntity<String> sendMessage(@RequestBody User user) throws InterruptedException, ExecutionException {
    	
    	String response = senderService.send(user);
    	return ResponseEntity.status(HttpStatus.OK).body(response);
      
    }
    
    
    
    /**
     * Returns number of users with the same first name
     * @param firstName
     * @return
     */
    @GetMapping("/count/{firstName}")
    public Long ordersCount(@PathVariable String firstName) {
      return userService.usersCount(firstName);
    }

}