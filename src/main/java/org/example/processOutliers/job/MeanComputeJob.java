package org.example.processOutliers.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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


public class MeanComputeJob {

	public static double computeMean(Path input, Path meanPath, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(conf, "Calculate mean");
		job.setJarByClass(MeanComputeJob.class);

		job.setMapperClass(MeanMapper.class);
		job.setReducerClass(MeanReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, meanPath);

		if (FileSystem.get(conf).isDirectory(meanPath)) {
			FileSystem.get(conf).delete(meanPath, true);
		}
		job.waitForCompletion(true);

		return getAverageFromPath(conf, meanPath);
	}

	public static class MeanMapper extends Mapper<Object, Text, Text, IntWritable> {
		Text word = new Text("k");
		IntWritable val = new IntWritable();

		@Override
		public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
			val.set(Integer.parseInt(value.toString().split(JobUtils.TAB_SEPARATOR)[1]));
			context.write(word, val);
		}
	}

	public static class MeanReducer extends Reducer<Text, IntWritable, NullWritable, DoubleWritable> {
		NullWritable out = NullWritable.get();
		DoubleWritable meanResult = new DoubleWritable();
		int count = 0;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			double mean;
			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
				count ++;
			}
			mean = (double)sum / count;
			meanResult.set(mean);
			context.write(out, meanResult);
		}
	}

	private static double getAverageFromPath(Configuration conf, Path path) throws IOException {

		FileSystem fs = FileSystem.get(conf);
		String averageStr = new BufferedReader(new InputStreamReader(fs.open(new Path(path, "part-r-00000")))).readLine();
		Double average = null;
		try {
			average = Double.valueOf(averageStr);
		} catch (NumberFormatException ex) {
			System.out.println("For some reason the result of the average isn't a double");

		}
		return average;
	}
}
