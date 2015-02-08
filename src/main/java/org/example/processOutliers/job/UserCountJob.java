package org.example.processOutliers.job;

import org.example.processOutliers.util.JobUtils;
import org.example.processOutliers.writables.UUIDWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by mihai on 30/01/15.
 */
public class UserCountJob {

	public static Path countUsers(Path input, Path output, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(conf, "First step is counting the events per user");
		job.setJarByClass(UserCountJob.class);

		job.setMapperClass(UserCountMapper.class);
		job.setReducerClass(UserCountReducer.class);

		job.setMapOutputKeyClass(UUIDWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		if (FileSystem.get(conf).isDirectory(output)) {
			FileSystem.get(conf).delete(output, true);
		}
		job.waitForCompletion(true);
		return output;
	}

	public static class UserCountMapper extends Mapper<Object, Text, UUIDWritable, IntWritable> {
		UUIDWritable user = new UUIDWritable();
		IntWritable count = new IntWritable();

		@Override
		public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
			String[] tokens = value.toString().split(JobUtils.TAB_SEPARATOR);

			user.set(UUID.fromString(tokens[0]));
			count.set(Integer.valueOf(tokens[1]));
			context.write(user, count);
		}
	}

	public static class UserCountReducer extends Reducer<UUIDWritable, IntWritable, Text, NullWritable> {
		Text output = new Text();
		NullWritable empty = NullWritable.get();

		@Override
		public void reduce(UUIDWritable key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable count : counts) {
				sum += count.get();
			}
			if (sum > 1) {
				output.set(key.get() + JobUtils.TAB_SEPARATOR + sum);
				context.write(output, empty);
			}
		}

	}
}
