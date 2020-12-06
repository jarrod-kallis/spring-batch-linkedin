package com.linkedin.batch.job.flow;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.linkedin.batch.job.delivery.DeliveryDecider;
import com.linkedin.batch.job.delivery.ItemCorrectnessDecider;

@Configuration
public class CommonFlows {
	
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public JobExecutionDecider deliveryDecider() {
		return new DeliveryDecider();
	}
	
	@Bean
	public JobExecutionDecider itemDecider() {
		return new ItemCorrectnessDecider();
	}

	@Bean
	public Flow deliveryFlow() {
		return new FlowBuilder<SimpleFlow>("deliveryFlow")
			.start(driveToAddressStep())
				.on("FAILED") // Check EXIT_STATUS of driveToAddressStep - we got lost
//				.to(storePackageStep()) // If we got lost store the package
//				.stop() // Stop the job execution: BATCH_STATUS = STOPPED (Allows restarting)
				.fail() // Fail the job execution: BATCH_STATUS = FAILED (Allows restarting)
			.from(driveToAddressStep()) // else if
				.on("*") // Any other EXIT_STATUS
				.to(deliveryDecider())
					.on("PRESENT") // Customer was there to receive the package
					.to(givePackageToCustomerStep())
						.next(itemDecider()).on("ITEM_CORRECT").to(thankCustomerStep())
						.from(itemDecider()).on("ITEM_INCORRECT").to(giveRefundStep())
				.from(deliveryDecider()) // else if
					.on("NOT_PRESENT") // Customer was not there
					.to(leavePackageAtDoorStep())
			.build();
	}
	
	@Bean
	public Step driveToAddressStep() {
		boolean gotLost = false;

		return this.stepBuilderFactory.get("driveToAddressStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				if (gotLost) {
					throw new RuntimeException("Got Lost driving to the address");
				}

				System.out.println("Successfully arrived at the address.");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Step givePackageToCustomerStep() {
		return this.stepBuilderFactory.get("givePackageToCustomerStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Given the package to the customer.");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Step storePackageStep() {
		return this.stepBuilderFactory.get("storePackageStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Unable to deliver package. Storing it.");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step leavePackageAtDoorStep() {
		return this.stepBuilderFactory.get("leavePackageAtDoorStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Customer not home. Leaving the package at the door.");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step thankCustomerStep() {
		return this.stepBuilderFactory.get("thankCustomerStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Item was correct. Thank customer.");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step giveRefundStep() {
		return this.stepBuilderFactory.get("giveRefundStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Item was incorrect. Refund customer.");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
}
