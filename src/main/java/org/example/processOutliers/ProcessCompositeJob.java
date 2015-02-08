package org.example.processOutliers;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.example.processOutliers.job.FilteringJob;
import org.example.processOutliers.job.MeanComputeJob;
import org.example.processOutliers.job.StandardDeviationJob;
import org.example.processOutliers.job.UserCountJob;
import org.example.processOutliers.util.JobUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.example.processOutliers.util.JobUtils.*;

public class ProcessCompositeJob {

	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {

		if ( args == null || args.length < 2 ){
			System.out.println("Mandatory arguments are input file and output folder");
			System.exit(0);
		}
		Configuration conf = new Configuration();
		String tmpDir = System.getProperty("java.io.tmpdir");

		Path countsPath = UserCountJob.countUsers(new Path(args[0]), new Path(tmpDir, COUNTS), conf);
		double mean = MeanComputeJob.computeMean(countsPath, new Path(tmpDir, MEAN), conf);
		double stDev = StandardDeviationJob.computeStandardDeviation(countsPath, new Path(tmpDir, ST_DEV), conf, mean);
		Path outputPath = FilteringJob.filter(new Path(args[0]), new Path(tmpDir, FILTERED), conf, mean, stDev);


		//export the result to a location
		File output = new File(args[1], "filtered.csv");
		if(!output.exists()) {
			output.createNewFile();
		}
		CSVWriter csvWriter = new CSVWriter(new FileWriter(output), '\t', CSVWriter.NO_QUOTE_CHARACTER);

		FileSystem fs = FileSystem.get(conf);
		JobUtils.outputToCsv(fs, outputPath, csvWriter, JobUtils.TAB_SEPARATOR);
		csvWriter.close();
		// remove folder
		fs.delete(outputPath, true);

	}



}
