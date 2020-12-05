package com.linkedin.batch;

import java.util.Random;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

public class ItemCorrectnessDecider implements JobExecutionDecider {

	@Override
	public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
		int random = new Random().nextInt(10);
		
		String result = random >= 3 ? "ITEM_CORRECT" : "ITEM_INCORRECT";
		
		System.out.println("Item decider: " + result + " because random was " + random);
		
		return new FlowExecutionStatus(result);
	}

}
