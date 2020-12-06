package com.linkedin.batch.job.delivery;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.linkedin.batch.job.flow.CommonFlows;

@Configuration
//Causes several beans to be registered within Spring's IOC container: 
//JobRepository, JobLauncher, JobRegistry & a transaction manager
@EnableBatchProcessing
public class DeliveryJob {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public CommonFlows commonFlows; 
	
	@Bean
	public Job deliverPackageJob() {
		return this.jobBuilderFactory
				.get("deliverPackageJob")
				.start(packageItemStep())
					.on("*").to(commonFlows.deliveryFlow())
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
}
