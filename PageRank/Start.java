package PageRank;

public class Start {

	public static void main(String[] args) throws Exception {
		String input = args[0]; // input
		String output = args[1]; // output in the form of out0
		String graphProperty = args[2]; // output for graph properties
		String topTen = args[3]; // output for top 10 nodes
		String dampFactor = args[4]; // damping factor

		long t1 = System.currentTimeMillis();
		// calculating grapg properties
		GraphProperties.Properties(input, graphProperty);
		long t2 = System.currentTimeMillis();
		// initializing ranks for the nodes
		InitPageRank.initRank(input, output);
		long t3 = System.currentTimeMillis();

		System.out.println("Time taken to calculate graph prop.: " + "\t"
				+ (t2 - t1));
		System.out.println("Time taken to initialize Rank: " + 
								"\t" + (t3 - t2));
		output = output.substring(0, output.length() - 1);
		int i = 1;
		// looped to 50 so that it doesn't keep on running in AWS
		for (i = 1; i < 50; i++) {
			String in = output + (i - 1) + "";
			String out = output + i + "";

			long t4 = System.currentTimeMillis();
			// calculating rank for this iteration
			long counter = CalculateRank.calculate(in, out, dampFactor);
			long t5 = System.currentTimeMillis();
			System.out.println("Time taken for iteration " + i + ":" + "\t"
					+ (t5 - t4));
			if (counter == 0) {
				break;
			}
		}
		long t6 = System.currentTimeMillis();
		// getting top 10 nodes based on rank.
		TopTenNodes.getTopTen(output + i + "", topTen);
		long t7 = System.currentTimeMillis();
		System.out.println("Time taken for getting Top 10 nodes:" + "\t"
				+ (t7 - t6));
		System.out.println("Converged after " + i + " iterations");
	}
}
