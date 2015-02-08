package org.example.processOutliers.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.processOutliers.util.JobUtils;
import org.example.processOutliers.writables.UUIDWritable;

import java.io.IOException;
import java.util.UUID;

public class FilteringJob {

	public static final String MEAN = "mean";
	public static final String STDEV = "stDev";

	public static Path filter(Path inputPath, Path outputPath, Configuration conf, Double mean, Double stDev) throws IOException, ClassNotFoundException, InterruptedException {

		conf.set(MEAN, mean.toString());
		conf.set(STDEV, stDev.toString());

		Job job = new Job(conf, "Filtering");
		job.setJarByClass(FilteringJob.class);

		job.setMapperClass(FilteringMapper.class);
		job.setReducerClass(FilteringReducer.class);

		job.setMapOutputKeyClass(UUIDWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		job.waitForCompletion(true);
		return outputPath;
	}

	public static class FilteringMapper extends Mapper<LongWritable, Text, UUIDWritable, IntWritable> {

		private final UUIDWritable uuidWritable = new UUIDWritable();
		private final IntWritable intWritable = new IntWritable();
		private Configuration conf;
		private double mean;
		private double stDev;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			mean = Double.valueOf(conf.get(MEAN));
			stDev = Double.valueOf(conf.get(STDEV));
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String[] tokens = value.toString().split("\t");
				UUID trackId = UUID.fromString(tokens[0]);
				Integer count = Integer.valueOf(tokens[1]);

				//if the count of the current user fits into the allowed values (is not outlier)
				if (!JobUtils.isOutlier(count, mean, stDev)){
					uuidWritable.set(trackId);
					intWritable.set(count);
					context.write(uuidWritable, intWritable);
				}
		}
	}

	public static class FilteringReducer extends Reducer<UUIDWritable, IntWritable, Text, NullWritable> {

		private Text text = new Text();

		@Override
		public void reduce(UUIDWritable inputKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			for (IntWritable val : values) {
				text.set(inputKey.toString() + JobUtils.TAB_SEPARATOR + val.toString());
				context.write(text, NullWritable.get());
			}
		}
	}



}
