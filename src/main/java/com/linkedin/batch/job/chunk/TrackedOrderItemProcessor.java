package com.linkedin.batch.job.chunk;

import java.util.UUID;

import org.springframework.batch.item.ItemProcessor;

public class TrackedOrderItemProcessor implements ItemProcessor<Order, TrackedOrder> {

	@Override
	public TrackedOrder process(Order item) throws Exception {
		TrackedOrder trackedOrder = new TrackedOrder(item);
		
		trackedOrder.setTrackingNumber(UUID.randomUUID().toString());
		trackedOrder.setFreeShipping(item.getCost().floatValue() > 50);
		
		return trackedOrder;
	}

}
