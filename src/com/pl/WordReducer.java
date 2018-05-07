package com.pl;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
		Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		String substr = "";
		long count = 0;
		for	(IntWritable v:values) {
				count +=v.get();  
				substr += "," + v.get();
		}
		System.out.println("WordReducer:reduce()------------------");
		System.out.println("Input:key(Text)=" + key + " value(IntWritable[])=" + substr);			
		System.out.println("Output:key(Text)=" + key + " value(LongWritable)=" + count);
		
		context.write(key, new LongWritable(count));
	}

}
