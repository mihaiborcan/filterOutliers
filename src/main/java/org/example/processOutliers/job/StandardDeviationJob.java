package org.example.processOutliers.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.processOutliers.util.JobUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class StandardDeviationJob {

	public static final String MEAN = "mean";

	public static double computeStandardDeviation(Path input, Path stDevPath, Configuration conf, Double mean) throws IOException, ClassNotFoundException, InterruptedException {

		conf.set(MEAN, mean.toString());
		Job job = new Job(conf, "Calculate standard deviation");
		job.setJarByClass(StandardDeviationJob.class);

		job.setMapperClass(StdMapper.class);
		job.setReducerClass(StdReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, stDevPath);

		if (FileSystem.get(conf).isDirectory(stDevPath)) {
			FileSystem.get(conf).delete(stDevPath, true);
		}
		job.waitForCompletion(true);

		return getStandardDevFromPath(conf, stDevPath);
	}

	public static class StdMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		Text word = new Text("std");
		DoubleWritable val = new DoubleWritable();
		private Double mean;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mean = Double.valueOf(context.getConfiguration().get(MEAN));
		}

		@Override
		public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
			int userCount = Integer.parseInt(value.toString().split(JobUtils.TAB_SEPARATOR)[1]);
			double diff = (userCount - mean) * (userCount - mean);
			val.set(diff);
			context.write(word, val);
		}
	}

	public static class StdReducer extends Reducer<Text, DoubleWritable, NullWritable, DoubleWritable> {
		NullWritable out = NullWritable.get();
		DoubleWritable standardDev = new DoubleWritable();
		int count = 0;

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double variance;
			double sum = 0;

			for (DoubleWritable val : values) {
				sum += val.get();
				count += 1;
			}
			variance = sum / count;
			standardDev.set(Math.sqrt(variance));
			context.write(out, standardDev);
		}
	}

	private static double getStandardDevFromPath(Configuration conf, Path path) throws IOException {

		FileSystem fs = FileSystem.get(conf);
		String stDevStr = new BufferedReader(new InputStreamReader(fs.open(new Path(path, "part-r-00000")))).readLine();
		Double stDev = null;
		try {
			stDev = Double.valueOf(stDevStr);
		} catch (NumberFormatException ex) {
			System.out.println("For some reason the result of the standard deviation isn't a double");

		}
		return stDev;
	}
}
