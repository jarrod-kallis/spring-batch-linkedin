package com.linkedin.batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//Causes several beans to be registered within Spring's IOC container: 
//JobRepository, JobLauncher, JobRegistry & a transaction manager
@EnableBatchProcessing
public class LinkedinBatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(LinkedinBatchApplication.class, args);
	}
}
