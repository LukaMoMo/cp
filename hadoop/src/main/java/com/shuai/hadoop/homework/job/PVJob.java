package com.shuai.hadoop.homework.job;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.shuai.hadoop.homework.job.cstype.IDWriteable;
import com.shuai.hadoop.homework.mr.PVMR;
import com.shuai.hadoop.homework.mr.PVMR.PVMapper.PVReducer;
import com.shuai.hadoop.homework.mr.PVMR.*;

public class PVJob {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		JobConf conf= new JobConf();
		String[] userArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(userArgs.length < 2) {
			System.err.println("input <in> <out> args!");
			return ;
		}
		
        //conf.addResource("classpath:/hadoop/core-site.xml");
        //conf.addResource("classpath:/hadoop/hdfs-site.xml");
        //conf.addResource("classpath:/hadoop/mapred-site.xml");
        //conf.set("mapred.skip.map.skip.records", "1000000");
		
		Job job = new Job(conf,"mySecondJob");
		job.setJarByClass(PVMR.class);
		job.setMapperClass(PVMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IDWriteable.class);
		job.setReducerClass(PVReducer.class);
		job.setNumReduceTasks(4);
		System.out.println(userArgs[0]);
		System.out.println(userArgs[1]);
		
		FileInputFormat.addInputPath(job, new Path(userArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(userArgs[1]));
		
		System.out.println(job.waitForCompletion(true) ? 0 :1);

	}

}
