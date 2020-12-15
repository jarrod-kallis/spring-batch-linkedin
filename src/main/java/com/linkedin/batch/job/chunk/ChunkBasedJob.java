package com.linkedin.batch.job.chunk;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.validator.BeanValidatingItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
public class ChunkBasedJob {
	
	private static String[] headerNames = new String[] {"order_id", "first_name", "last_name", "email", "cost", "item_id", "item_name", "ship_date"};
	
	private static String[] orderClassFieldNames = new String[] {"orderId", "firstName", "lastName", "email", "cost", "itemId", "itemName", "shipDate"};
	
	private static String ORDER_SQL_SELECT = "select order_id, first_name, last_name, "
			+ "email, cost, item_id, item_name, ship_date";
	private static String ORDER_SQL_FROM = "from SHIPPED_ORDER";
	
	// Need to specify an "Order By", otherwise it might be a problem if we need to restart a job 
	private static String ORDER_SQL = ORDER_SQL_SELECT + " "
			+ ORDER_SQL_FROM + " "
			+ "order by order_id";
	
	private static String INSERT_ORDER_SQL;
	private static String INSERT_ORDER_SQL_NAMED_PARAMS;
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;
	
	public ChunkBasedJob() {
		// Ordinal Params
		INSERT_ORDER_SQL = "insert into "
				+ "SHIPPED_ORDER_OUTPUT(";
		
		for (int i = 0; i < headerNames.length; i++) {
			String headerName = headerNames[i];
			
			INSERT_ORDER_SQL += headerName;
			
			if (i < headerNames.length - 1) {
				INSERT_ORDER_SQL += ", ";
			}
		}
		
		INSERT_ORDER_SQL += ") values (?,?,?,?,?,?,?,?)";
		
		System.out.println(INSERT_ORDER_SQL);

		// Named Params
		INSERT_ORDER_SQL_NAMED_PARAMS = "insert into "
				+ "SHIPPED_ORDER_OUTPUT(";
		
		for (int i = 0; i < headerNames.length; i++) {
			String headerName = headerNames[i];
			
			INSERT_ORDER_SQL_NAMED_PARAMS += headerName;
			
			if (i < headerNames.length - 1) {
				INSERT_ORDER_SQL_NAMED_PARAMS += ", ";
			}
		}
		
		INSERT_ORDER_SQL_NAMED_PARAMS += ") values (";
		
		for (int i = 0; i < orderClassFieldNames.length; i++) {
			String fieldName = orderClassFieldNames[i];
			
			INSERT_ORDER_SQL_NAMED_PARAMS += ":" + fieldName;
			
			if (i < orderClassFieldNames.length - 1) {
				INSERT_ORDER_SQL_NAMED_PARAMS += ", ";
			}
		}
		
		INSERT_ORDER_SQL_NAMED_PARAMS += ")";
		
		System.out.println(INSERT_ORDER_SQL_NAMED_PARAMS);
	}

	@Bean
	public Job chunkJob() throws Exception {
		return this.jobBuilderFactory.get("chunkJob")
				.start(chunkStep())
				.build();
	}
	
	@Bean
	public ItemReader<Order> flatFileItemReader() {
		FlatFileItemReader<Order> itemReader = new FlatFileItemReader<Order>();
		
		itemReader.setLinesToSkip(1);
		itemReader.setResource(new FileSystemResource("src/main/java/com/linkedin/batch/job/chunk/shipped_orders.csv"));
		
		DefaultLineMapper<Order> lineMapper = new DefaultLineMapper<Order>();
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames(headerNames);
		
		lineMapper.setLineTokenizer(tokenizer);
		
		lineMapper.setFieldSetMapper(new OrderFieldSetMapper());

		itemReader.setLineMapper(lineMapper);
		
		return itemReader;
	}

	// JDBC Cursor is not thread safe
	@Bean
	public ItemReader<Order> jdbcCursorItemReader() {
		return new JdbcCursorItemReaderBuilder<Order>()
				.dataSource(dataSource)
				.name("jdbcCursorItemReader")
				.sql(ORDER_SQL)
				.rowMapper(new OrderRowMapper())
				.build();
	}
	
	@Bean
	public ItemReader<Order> jdbcPagingItemReader() throws Exception {
		return new JdbcPagingItemReaderBuilder<Order>()
				.dataSource(dataSource)
				.name("jdbcCursorItemReader")
				.queryProvider(queryProvider())
				.rowMapper(new OrderRowMapper())
				.pageSize(10) // Must match the chunk size
				.build();
	}
	
	@Bean
	public PagingQueryProvider queryProvider() throws Exception {
		SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
		
		factory.setSelectClause(ORDER_SQL_SELECT);
		factory.setFromClause(ORDER_SQL_FROM);
		factory.setSortKey("order_id");
		factory.setDataSource(dataSource);
		
		return factory.getObject();
	}

	@Bean
	public Step chunkStep() throws Exception {
		return this.stepBuilderFactory.get("chunkStep")
				.<Order, Order>chunk(10)
//				.reader(flatFileItemReader())
//				.reader(jdbcCursorItemReader())
				.reader(jdbcPagingItemReader())
				.processor(orderValidatingItemProcessor())
//				.writer(new ItemWriter<Order>() {
//
//					@Override
//					public void write(List<? extends Order> items) throws Exception {
//						System.out.println(String.format("Received %s items", items.size()));
//						items.forEach(System.out::println);
//					}
//					
//				})
//				.writer(flatFileItemWriter())
				.writer(jdbcBatchItemWriter())
				.build();
	}

	@Bean
	public ItemProcessor<Order, Order> orderValidatingItemProcessor() {
		// This validation item processor uses JSR-380 annotations on the bean to validate it
		BeanValidatingItemProcessor<Order> itemProcessor = new BeanValidatingItemProcessor<Order>();
		
		// If filter is false the item processor will throw an error if there is a validation exception
		itemProcessor.setFilter(true);
		
		return itemProcessor;
	}

	@Bean
	public ItemWriter<Order> flatFileItemWriter() {
		FlatFileItemWriter<Order> itemWriter = new FlatFileItemWriter<Order>();
		
		itemWriter.setResource(new FileSystemResource("src/main/java/com/linkedin/batch/job/chunk/shipped_orders_output.csv"));
		
		DelimitedLineAggregator<Order> aggregator = new DelimitedLineAggregator<Order>();
		aggregator.setDelimiter(",");
		
		BeanWrapperFieldExtractor<Order> fieldExtractor = new BeanWrapperFieldExtractor<Order>();
		fieldExtractor.setNames(orderClassFieldNames);
		
		aggregator.setFieldExtractor(fieldExtractor);
		
		itemWriter.setLineAggregator(aggregator);
		
		return itemWriter;
	}
	
	@Bean
	public ItemWriter<Order> jdbcBatchItemWriter() {
		return new JdbcBatchItemWriterBuilder<Order>()
				.dataSource(dataSource)
//				.sql(INSERT_ORDER_SQL)
//				.itemPreparedStatementSetter(new OrderItemPreparedStatementSetter())
				.sql(INSERT_ORDER_SQL_NAMED_PARAMS)
				.beanMapped() // Maps the names of the fields in the Order class to the named params in the SQL
				.build();
	}
}
