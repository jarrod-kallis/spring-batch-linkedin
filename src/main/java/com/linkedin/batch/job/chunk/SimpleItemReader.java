package com.linkedin.batch.job.chunk;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

public class SimpleItemReader implements ItemReader<String> {

	private List<String> data = new ArrayList<>();
	
	private Iterator<String> iterator;
	
	public SimpleItemReader() {
		super();
		
		for (int i = 1; i <= 5; i++) {
			this.data.add("" + i);			
		}
		
		this.iterator = this.data.iterator();
	}

	@Override
	public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		return this.iterator.hasNext() ? this.iterator.next() : null;
	}

}
