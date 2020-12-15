package com.linkedin.batch.job.json;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.linkedin.batch.job.chunk.Order;
import com.linkedin.batch.job.chunk.OrderRowMapper;

@Configuration
public class JsonJob {

	private static String ORDER_SQL_SELECT = "select order_id, first_name, last_name, "
			+ "email, cost, item_id, item_name, ship_date";
	private static String ORDER_SQL_FROM = "from SHIPPED_ORDER";
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;
	
	@Bean
	public Job jsonJobConfig() throws Exception {
		return this.jobBuilderFactory.get("jsonJobConfig")
				.start(jsonJobStep())
				.build();
	}
	
	@Bean
	public Step jsonJobStep() throws Exception {
		return this.stepBuilderFactory.get("jsonJobStep")
				.<Order, Order>chunk(10)
				.reader(jdbcItemReader())
				.writer(jsonItemWriter())
				.build();
	}
	
	@Bean
	public ItemReader<Order> jdbcItemReader() throws Exception {
		return new JdbcPagingItemReaderBuilder<Order>()
				.dataSource(dataSource)
				.name("jdbcItemReader")
				.queryProvider(jsonJobQueryProvider())
				.rowMapper(new OrderRowMapper())
				.pageSize(10) // Must match the chunk size
				.build();
	}
	
	@Bean
	public PagingQueryProvider jsonJobQueryProvider() throws Exception {
		SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
		
		factory.setSelectClause(ORDER_SQL_SELECT);
		factory.setFromClause(ORDER_SQL_FROM);
		factory.setSortKey("order_id");
		factory.setDataSource(dataSource);
		
		return factory.getObject();
	}
	
	// https://docs-stage.spring.io/spring-batch/docs/current/reference/html/readersAndWriters.html#jsonfileitemwriter
	@Bean
	public JsonFileItemWriter<Order> jsonItemWriter() {
		return new JsonFileItemWriterBuilder<Order>()			
			.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<Order>())
			.resource(new FileSystemResource("src/main/java/com/linkedin/batch/job/json/shipped_orders_output.json"))
			.name("jsonItemWriter")
			.build();
	}
}
