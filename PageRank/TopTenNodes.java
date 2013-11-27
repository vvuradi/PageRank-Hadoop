package PageRank;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopTenNodes {

	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {
		
		// map function - sends intermediate <Key, Value> as <node, value>
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			String node = tokenizer.nextToken();
			String rank = tokenizer.nextToken();
			// map function sending intermediate <'Node',node_rank> pairs
			context.write(new Text("Node"), new Text(node+"_"+rank));
		}
	}

	public static class Reduce extends
			Reducer<Text, Text, Text, Text> {
		/*
		 * reduce function - receives <Key, Value> as <'Node',node_rank> and gives
		 * output in the form <word, count>
		 */
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			SortedMap<Float, String> allNodes = new TreeMap<Float, String>();
			// adding all rank-node pairs to sorted map
			for(Text t : values){
				String[] nodeValPair = t.toString().split("_", 2);
				allNodes.put(Float.parseFloat(nodeValPair[1]), nodeValPair[0]);
			}
			// retrieve top 10 ranked nodes from sorted map
			for(int i=0; i<10; i++){
				Float rank = allNodes.lastKey();
				String node = allNodes.get(rank);
				allNodes.remove(rank);
				// writing node and its rank to output file
				context.write(new Text(node), new Text(rank+""));
			}
		}
	}

	public static void getTopTen(String input, String output) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Top Ten Nodes");
		job.setJarByClass(TopTenNodes.class);

		// setting key & value classes for output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// setting Mapper and Reducer
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// setting input and output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// file inputs from user
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}

}