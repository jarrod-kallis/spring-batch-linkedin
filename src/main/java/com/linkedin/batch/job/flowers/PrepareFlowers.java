package com.linkedin.batch.job.flowers;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class PrepareFlowers {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public StepExecutionListener selectedFlowerListener() {
		return new FlowerSelectionStepExecutionListener();
	}
	
	@Bean
	public Job prepareFlowersJob() {
        return this.jobBuilderFactory.get("prepareFlowersJob")
        		.start(selectFlowersStep())
        			.on("TRIM_REQUIRED").to(removeThornsStep()).next(arrangeFlowersStep())
        		.from(selectFlowersStep())
        			.on("NO_TRIM_REQUIRED").to(arrangeFlowersStep())
        		.end()
        		.build();
    }

	@Bean
    public Step selectFlowersStep() {
        return this.stepBuilderFactory.get("selectFlowersStep").tasklet(new Tasklet() {

            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Gathering flowers for order.");
                return RepeatStatus.FINISHED; 
            }
            
        })
        .listener(selectedFlowerListener())
        .build();
    }

	@Bean
    public Step arrangeFlowersStep() {
        return this.stepBuilderFactory.get("arrangeFlowersStep").tasklet(new Tasklet() {

            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Arranging flowers for order.");
                return RepeatStatus.FINISHED; 
            }
            
        }).build();
    }

	@Bean
    public Step removeThornsStep() {
        return this.stepBuilderFactory.get("removeThornsStep").tasklet(new Tasklet() {

            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Remove thorns from roses.");
                return RepeatStatus.FINISHED; 
            }
            
        }).build();
    }

}
