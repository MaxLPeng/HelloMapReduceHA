package com.pl;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
			
			final IntWritable oneWord = new IntWritable(1); 
			String lineInfo = value.toString();
			System.out.println("WordMapper:map()------------------");
			System.out.println("Input:key(LongWritable)=" + key + " value(Text)=" + value);
			String[] words = lineInfo.split(" ");
			for(String word : words) {
				System.out.println("output:key(Text)=" + word + " value(IntWritable)=" + oneWord);
	            context.write(new Text(word), oneWord);
	        }
	}

	
	
	
}
