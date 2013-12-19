package com.activsteps.hadoopworkflows.invertedindex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class LocalToHadoop implements Tasklet {

	private static final Log log = LogFactory.getLog(LocalToHadoop.class);

	private String localFile;
	private String localDirectory;
	private String hadoopFile;
	private String hadoopDirectory;
	private String confCoreSite;
	private String confHdfsSite;
	private Configuration hadoopConf;

	public void setLocalFile(String localFile) {
		this.localFile = localFile;
	}

	public void setLocalDirectory(String localDirectory) {
		this.localDirectory = localDirectory;
	}

	public void setHadoopFile(String hadoopFile) {
		this.hadoopFile = hadoopFile;
	}

	public void setHadoopDirectory(String hadoopDirectory) {
		this.hadoopDirectory = hadoopDirectory;
	}

	public void setConfCoreSite(String confCoreSite) {
		this.confCoreSite = confCoreSite;
	}

	public void setConfHdfsSite(String confHdfsSite) {
		this.confHdfsSite = confHdfsSite;
	}

	public void setHadoopConf(Configuration hadoopConf) {
		this.hadoopConf = hadoopConf;
	}

	/*
	 * Assumptions all output directories needs to be deleted /user/activsteps/springbatch/invertedindex/input needs to
	 * be created
	 */
	public RepeatStatus execute(StepContribution contribution,
			ChunkContext chunkContext) throws Exception {

		hadoopConf
				.addResource(new Path(
						"/home/qvantel/developer/hadoop/hadoop-1.1.2/conf/core-site.xml"));
		hadoopConf
				.addResource(new Path(
						"/home/qvantel/developer/hadoop/hadoop-1.1.2/conf/mapred-site.xml"));
		hadoopConf
				.addResource(new Path(
						"/home/qvantel/developer/hadoop/hadoop-1.1.2/conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(hadoopConf);
		Path sourcePath = new Path(
				"/home/qvantel/datascience-labs/bigwindow/hadoop-springbatch-invertedindex/src/main/resources/data/urlwords.txt");
		
		Path destPath = new Path("/user/activsteps/springbatch/invertedindex/input");
		if (!(fs.exists(destPath))) {
			System.out.println("No Such destination exists :" + destPath);
			return RepeatStatus.CONTINUABLE;
		}
		
		fs.copyFromLocalFile(sourcePath, destPath);

		return RepeatStatus.FINISHED;
	}

	public static void main(String[] argv) {
		LocalToHadoop localToHadoop = new LocalToHadoop();
		//copyTask.copyFileToHadoop();
	}

}
