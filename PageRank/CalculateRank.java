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

public class CalculateRank {
	public static enum MATCH_COUNTER {
		converge;
	};

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text oNodes = new Text();

		/*
		 * map function - sends intermediate <Key, Value> as <node, edges>
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);
			if (tokenizer.hasMoreTokens()) {
				// getting num of out bounds. First is node
				// followed by 2 ranks
				int outBounds = tokenizer.countTokens() - 3;
				String node = tokenizer.nextToken();
				String prevRank = tokenizer.nextToken();
				Float rank = Float.parseFloat(prevRank);
				// prev but one rank is neglected
				tokenizer.nextToken();
				// prev rank is included
				String s = prevRank + " ";
				while (tokenizer.hasMoreTokens()) {
					String nxtNode = tokenizer.nextToken();
					s = s + nxtNode + " ";
					oNodes.set(nxtNode);
					String rankValue = rank / outBounds + "R";
					// map func sending <node, rank> pairs
					context.write(oNodes, new Text(rankValue));
				}
				// map function sending intermediate <node,edges> pairs
				// with only one rank
				context.write(new Text(node), new Text(s));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		/*
		 * reduce function - receives <Key, Value> as <node, count> and gives
		 * output in the form <word, count>
		 */
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			float rankSum = 0;
			String sumStr = "";
			String temp = "";
			
			// getting damp factor from config
			Configuration conf = context.getConfiguration();
			Float dFact = Float.parseFloat(conf.get("dampFactor"));
			
			for (Text t : values) {
				String line = t.toString();
				StringTokenizer tokenizer = new StringTokenizer(line);
				if (tokenizer.hasMoreTokens()) {
					String isRank = tokenizer.nextToken();
					int len = isRank.length();
					if (isRank.charAt(len - 1) == 'R') {
						rankSum = rankSum + 
								Float.parseFloat(isRank.substring(0, len - 1));
					} else {
						temp = isRank + " ";
					}
				}
				// appending rest of the tokens
				while (tokenizer.hasMoreTokens()) {
					temp = temp + tokenizer.nextToken() + " ";
				}
			}
			// calculating rank using damp factor
			rankSum = (1 - dFact) + (dFact * rankSum);
			String pRank[] = temp.split(" ", 2);
			Float prevRank = Float.parseFloat(pRank[0]);
			
			// checking for convergence
			if (Math.abs(rankSum - prevRank) > 0.001) {
				context.getCounter(MATCH_COUNTER.converge).increment(1);
			}
			sumStr = rankSum + " " + temp;
			// writing out the new rank while maintaining the graph structure
			context.write(key, new Text(sumStr));
		}
	}

	public static long calculate(String inp, String out, String dFactor)
			throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "CalculateRank");
		job.setJarByClass(CalculateRank.class);

		// setting key & value classes for output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// setting Mapper and Reducer
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// setting input and output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.getConfiguration().set("dampFactor", dFactor);

		// file inputs from user
		FileInputFormat.addInputPath(job, new Path(inp));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.waitForCompletion(true);

		// creating counter to be used for convergence
		Counters counters = job.getCounters();
		Counter c1 = counters.findCounter(MATCH_COUNTER.converge);

		return c1.getValue();
	}

}