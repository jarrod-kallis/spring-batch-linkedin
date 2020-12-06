package com.linkedin.batch.job.billing;

import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BillingFlow {

	@Autowired
	public BillingJob billingJob;
	
	@Bean
	public Flow billCustomerFlow() {
		return new FlowBuilder<SimpleFlow>("billCustomerFlow")
				.start(this.billingJob.sendInvoiceStep())
				.build();
	}
}
