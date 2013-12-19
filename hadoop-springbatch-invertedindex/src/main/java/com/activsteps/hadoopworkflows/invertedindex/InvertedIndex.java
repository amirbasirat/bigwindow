package com.activsteps.hadoopworkflows.invertedindex;

import java.util.Date;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class InvertedIndex {

	private static final Log log = LogFactory.getLog(InvertedIndex.class);

	public static void main(String[] args) throws Exception {
		AbstractApplicationContext context = new ClassPathXmlApplicationContext(
				"classpath:/META-INF/spring/*-context.xml");
		log.info("Batch Inverted Index Application Running");
		context.registerShutdownHook();

		JobLauncher jobLauncher = context.getBean(JobLauncher.class);
		Job job = context.getBean(Job.class);
		jobLauncher.run(
				job,
				new JobParametersBuilder()
						.addString("mr.input", "/user/activsteps/springbatch/invertedindex/input")
						.addString("mr.output", "/user/activsteps/springbatch/invertedindex/output")
						.addString("localData",
								"./data/urlwords.txt")
						.addDate("date", new Date()).toJobParameters());
		// startJobs(context);

	}

	public static void startJobs(ApplicationContext ctx) {
		JobLauncher launcher = ctx.getBean(JobLauncher.class);
		Map<String, Job> jobs = ctx.getBeansOfType(Job.class);

		for (Map.Entry<String, Job> entry : jobs.entrySet()) {
			System.out.println("Executing job " + entry.getKey());
			try {
				if (launcher.run(entry.getValue(), new JobParameters())
						.getStatus().equals(BatchStatus.FAILED)) {
					throw new BeanInitializationException(
							"Failed executing job " + entry.getKey());
				}
			} catch (Exception ex) {
				throw new BeanInitializationException("Cannot execute job "
						+ entry.getKey(), ex);
			}
		}
	}
}
