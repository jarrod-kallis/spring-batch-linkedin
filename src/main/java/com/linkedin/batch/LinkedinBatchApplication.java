package com.linkedin.batch;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
// Causes several beans to be registered within Spring's IOC container: 
// JobRepository, JobLauncher, JobRegistry & a transaction manager
@EnableBatchProcessing
public class LinkedinBatchApplication {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

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

	public static void main(String[] args) {
		SpringApplication.run(LinkedinBatchApplication.class, args);
	}

	@Bean
	public Job deliverPackageJob() {
		return this.jobBuilderFactory
				.get("deliverPackageJob")
				.start(packageItemStep())
				.next(driveToAddressStep())	
					.on("FAILED") // Check EXIT_STATUS of driveToAddressStep - we got lost
//					.to(storePackageStep()) // If we got lost store the package
//					.stop() // Stop the job execution: BATCH_STATUS = STOPPED (Allows restarting)
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
				.end()
				.build();
	}

	@Bean
	public Step packageItemStep() {
		return this.stepBuilderFactory.get("packageItemStep").tasklet(new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				// java -jar target/.jar "item=shoes" "run.date(date)=2020/01/31"

				Map<String, Object> jobParameters = chunkContext.getStepContext().getJobParameters();
				String item = jobParameters.get("item").toString();
				String date = jobParameters.get("run.date").toString();

				System.out.println(String.format("The %s have been packaged on %s", item, date));
				return RepeatStatus.FINISHED;
			}
		}).build();
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
