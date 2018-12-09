package cn.itcast.bigdata.mr.fensi;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;


public class SharedFirendsStepTwo {
	static class SharedFirendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		//A	I,K,C,B,G,F,H,O,D, 
		//友 人，人，人
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] friend_persons = line.split("\t");
			String friend = friend_persons[0];
			String[] persons = friend_persons[1].split(",");
			Arrays.sort(persons);
			for(int i = 0; i < persons.length - 2; i++)
				for(int j = i + 1; j < persons.length - 1; j++) {
					//发出<人->人， 好友> 
					context.write(new Text(persons[i] + "-"+ persons[j]), new Text(friend));
					
				}
			
			
		}
	}
	static class SharedFirendsStepTwoReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text person_person, Iterable<Text> friends, Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			
			for(Text friend : friends) {
				sb.append(friend).append(" ");
			}
			context.write(new Text(person_person), new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SharedFirendsStepTwo.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Users/rand/Exciting/DemoData/friendsOut.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/rand/Exciting/DemoData/friendsOut2.txt"));
		job.setMapperClass(SharedFirendsStepTwoMapper.class);
		job.setReducerClass(SharedFirendsStepTwoReducer.class);
		job.waitForCompletion(true);
	}
}









