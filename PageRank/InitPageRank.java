package PageRank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InitPageRank {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text first = new Text();
		private Text rest = new Text();

		/*
		 * map function - sends intermediate <Key, Value> as <node, edges>
		 */
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			if(tokenizer.hasMoreTokens()){
				String node = tokenizer.nextToken();
				// appending initial rank of 1.0 1.0 
				node = node + " 1.0 1.0 ";
				String edges = "";
				while(tokenizer.hasMoreTokens()){
					edges = edges + tokenizer.nextToken() + " ";
				}
				first.set(node);
				rest.set(edges);
				// map function sending intermediate <node,edges> pairs
				context.write(first, rest);
			}
		}
	} 

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		/*
		 * reduce function - receives <Key, Value> as <node, count> and gives
		 * output in the form <word, count>
		 */
		public void reduce(Text key, Text values, Context context) 
				throws IOException, InterruptedException {
			// writing the graph with rank included for each node
			context.write(key, values);
		}
	}

	public static void initRank(String inp, String out) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "initialize PageRank");
		job.setJarByClass(InitPageRank.class);

		//setting key & value classes for output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//setting Mapper and Reducer
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		//setting input and output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//file inputs from user
		FileInputFormat.addInputPath(job, new Path(inp));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.waitForCompletion(true);
	}

}