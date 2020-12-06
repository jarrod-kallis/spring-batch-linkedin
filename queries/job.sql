select job_execution.JOB_EXECUTION_ID, job_instance.JOB_INSTANCE_ID, job_execution.status as job_status, step_execution.step_name, step_execution.status as step_status,
  job_execution.CREATE_TIME
  from batch_job_instance job_instance
    join batch_job_execution job_execution on job_execution.job_instance_id = job_instance.job_instance_id
    left outer join batch_step_execution step_execution on step_execution.job_execution_id = job_execution.job_execution_id
where job_execution.JOB_INSTANCE_ID = (select max(job_instance_id) from batch_job_instance);