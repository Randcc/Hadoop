package cn.itcast.bigdata.mr.fensi;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SharedFirendsStepOne {
	static class SharedFirendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//A:B,C,D,F,E,O
			String line = value.toString();
			String[] person_friend = line.split(":");
			String person = person_friend[0];
			String friends = person_friend[1];
			for(String friend:person_friend[1].split(",")) {
				//输出<好友，人>
				 context.write(new Text(friend), new Text(person));
				 
				 
			}
		}
	}
	static class SharedFirendsStepOneReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			
			for(Text person : persons) {
				sb.append(person).append(",");
			}
			context.write(friend, new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SharedFirendsStepOne.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Users/rand/Exciting/DemoData/friends.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/rand/Exciting/DemoData/friendsOut.txt"));
		job.setMapperClass(SharedFirendsStepOneMapper.class);
		job.setReducerClass(SharedFirendsStepOneReducer.class);
		job.waitForCompletion(true);
	}
}









