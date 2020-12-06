package com.linkedin.batch.job.billing;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BillingJob {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public Step billCustomerJobStep() {
		return this.stepBuilderFactory.get("billCustomerJobStep")
				.job(billCustomerJob())
				.build();
	}

	@Bean
	public Job billCustomerJob() {
		return this.jobBuilderFactory.get("billCustomerJob")
				.start(sendInvoiceStep())
				.build();
	}
	
	@Bean
	public Step sendInvoiceStep() {
		return this.stepBuilderFactory.get("invoiceStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Invoice is sent to the customer");
				return RepeatStatus.FINISHED; 
			}
		}).build();

	}
}
