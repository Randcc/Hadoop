package cn.itcast.bigdata.mr.hufen;

import java.io.IOException;

import org.apache.commons.net.ftp.parser.MacOsPeterFTPEntryParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SharedFirendsStepOne {
	static class SharedFirendsStepOneMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
			
			Text text1 = new Text(); 
			@Override
			protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
				String line = value.toString();
				String[] person_friends = line.split(":");
				String person = person_friends[0];
				String[] friends = person_friends[1].split(",");
				for(int i = 0; i < friends.length; i++) {
					String friend = friends[i];
					if(person.compareTo(friend) < 0) {
						text1.set(person + "-" + friend);
					}else {
						text1.set(friend + "-" + person);
					}
					context.write(text1, NullWritable.get());
				}
			}
			
		}

	static class SharedFirendsStepOneReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text text, Iterable<NullWritable> counts, Context context) throws IOException, InterruptedException {
			int temp = 0;
			for(NullWritable count:counts) {
				temp++;
			}
			if(temp >= 2)
				context.write(text, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SharedFirendsStepOne.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Users/rand/Exciting/DemoData/friends.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/rand/Exciting/DemoData/friendhufen.txt"));
		job.setMapperClass(SharedFirendsStepOneMapper.class);
		job.setReducerClass(SharedFirendsStepOneReducer.class);
		job.waitForCompletion(true);
	}
}









