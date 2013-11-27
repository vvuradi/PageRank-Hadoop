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

public class GraphProperties {

	public static class Map extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
		private Text noOfEdges = new Text();

		/*
		 * map function - sends intermediate <Key, Value> as <node, count>
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			if (tokenizer.hasMoreTokens()) {
				int numOfEdges = tokenizer.countTokens() - 1;
				noOfEdges.set("Total Edges");
				// map function sending intermediate <key,value> pairs
				context.write(noOfEdges, new FloatWritable(numOfEdges));
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {
		/*
		 * reduce function - receives <Key, Value> as <node, count> and gives
		 * output in the form <word, count>
		 */
		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float edges = 0;
			float nodes = 0;
			float minDeg = Integer.MAX_VALUE;
			float maxDeg = Integer.MIN_VALUE;

			for (FloatWritable val : values) {
				float tempVal = val.get();
				edges += tempVal;
				nodes = nodes + 1;
				if (tempVal > maxDeg)
					maxDeg = tempVal;
				if (tempVal < minDeg)
					minDeg = tempVal;
			}
			float avgDeg = edges / nodes;
			// writing total edges in the input graph.
			context.write(key, new FloatWritable(edges));
			// writing total nodes in the input graph.
			context.write(new Text("Total Nodes"), new FloatWritable(nodes));
			// writing min degree in the input graph.
			context.write(new Text("Min Degree"), new FloatWritable(minDeg));
			// writing max degree in the input graph.
			context.write(new Text("Max Degree"), new FloatWritable(maxDeg));
			// writing average degree in the input graph.
			context.write(new Text("Avg Degree"), new FloatWritable(avgDeg));
		}
	}

	public static void Properties(String input, String output) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Graph Properties");
		job.setJarByClass(GraphProperties.class);

		// setting key & value classes for output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		// setting Mapper and Reducer
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// job.setNumReduceTasks(1);
		// setting input and output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// file inputs from user
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}

}