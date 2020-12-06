package com.linkedin.batch.job.flowers;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

public class FlowerSelectionStepExecutionListener implements StepExecutionListener {

	@Override
	public void beforeStep(StepExecution stepExecution) {
		System.out.println("Executing before flower selection step logic");
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		System.out.println("Executing after flower selection step logic");
		String flowerType = stepExecution.getJobParameters().getString("type");
		
		return flowerType.equalsIgnoreCase("roses") ? new ExitStatus("TRIM_REQUIRED") : new ExitStatus("NO_TRIM_REQUIRED");
	}

}
