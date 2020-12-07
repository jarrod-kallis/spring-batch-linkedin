package com.linkedin.batch.job.chunk;

import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ChunkBasedJob {
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public Job chunkJob() {
		return this.jobBuilderFactory.get("chunkJob")
				.start(chunkStep())
				.build();
	}
	
	@Bean
	public ItemReader<String> itemReader() {
		return new SimpleItemReader();
	}	
	
	@Bean
	public Step chunkStep() {		
		return this.stepBuilderFactory.get("chunkStep")
				.<String, String>chunk(3)
				.reader(itemReader())
				.writer(new ItemWriter<String>() {

					@Override
					public void write(List<? extends String> items) throws Exception {
						System.out.println(String.format("Received %s items", items.size()));
						items.forEach(System.out::println);
					}
					
				})
				.build();
	}
}
