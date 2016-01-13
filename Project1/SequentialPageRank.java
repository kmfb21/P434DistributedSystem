import java.io.*;
import java.security.KeyStore.Entry;
import java.util.*;

import javax.swing.text.html.HTMLDocument.Iterator;

import com.sun.javafx.collections.MappingChange.Map;

public class SequentialPageRank {
    // adjacency matrix read from file
    private HashMap<Integer, ArrayList<Integer>> adjMatrix = new HashMap<Integer, ArrayList<Integer>>();
    // input file name
    private String inputFile = "";
    // output file name
    private String outputFile = "";
    // number of iterations
    private int iterations = 10;
    // damping factor
    private double df = 0.85;
    // number of URLs
    private int size = 0;
    // calculating rank values
    private HashMap<Integer, Double> rankValues = new HashMap<Integer, Double>();

    /**
     * Parse the command line arguments and update the instance variables. Command line arguments are of the form
     * <input_file_name> <output_file_name> <num_iters> <damp_factor>
     *
     * @param args arguments
     */
    public void parseArgs(String[] args) {
    	inputFile = args[0];
    	outputFile = args[1];
    	if (args[2] != null) {
			iterations = Integer.valueOf(args[2]);
		}
    	if (args[3] != null) {
			df = Double.valueOf(args[3]);
		}
    }

    /**
     * Read the input from the file and populate the adjacency matrix
     *
     * The input is of type
     *
     0
     1 2
     2 1
     3 0 1
     4 1 3 5
     5 1 4
     6 1 4
     7 1 4
     8 1 4
     9 4
     10 4
     * The first value in each line is a URL. Each value after the first value is the URLs referred by the first URL.
     * For example the page represented by the 0 URL doesn't refer any other URL. Page
     * represented by 1 refer the URL 2.
     *
     * @throws java.io.IOException if an error occurs
     */
    public void loadInput() throws IOException {
    	File f = new File(inputFile);
    	InputStream is = new FileInputStream(f);
    	DataInputStream dis = new DataInputStream(is);
    	
    	String temp = null;
    	int key;
    	while ((temp = dis.readLine())!=null) {
    		ArrayList<Integer> var = new ArrayList<Integer>();
    		String[] line = temp.split(" ");
			key = Integer.valueOf(line[0]);
			for (int i = 1; i < line.length; i++) {
				var.add(Integer.valueOf(line[i]));
			}
			adjMatrix.put(key, var);
		}
    	
    	dis.close();
    	is.close();
    }

    /**
     * Do fixed number of iterations and calculate the page rank values. You may keep the
     * intermediate page rank values in a hash table.
     */
    public void calculatePageRank() {
    	int num;
    	double var,sum;
    	HashMap<Integer, Double> temprank = new HashMap<Integer, Double>();
    	Set<Integer> s = adjMatrix.keySet();
    	int n = s.size();
    	double init = 1.0 / n;
    	ArrayList<Integer> l;
    	for (int i : s) {
			rankValues.put(i, init);
		}
    	for (int i = 0; i < iterations; i++) {
			for (int key : s) {
				sum = 0;
				for (int father : s) {
					l = adjMatrix.get(father);
					num = l.indexOf(key);
					if (num != -1) {
						sum += rankValues.get(father) / l.size();
					}
				}
				var = (1 - df) / n + df * sum;
				temprank.put(key, var);
			}
			rankValues = temprank;
		}
    }

    /**
     * Print the pagerank values. Before printing you should sort them according to decreasing order.
     * Print all the values to the output file. Print only the first 10 values to console.
     *
     * @throws IOException if an error occurs
     */
    public void printValues() throws IOException {
    	File f = new File(outputFile);
    	OutputStream os = new FileOutputStream(f);
    	DataOutputStream dos = new DataOutputStream(os);
    	
    	List<java.util.Map.Entry<Integer, Double>> rank = new ArrayList<java.util.Map.Entry<Integer, Double>>(rankValues.entrySet());
    	Collections.sort(rank, new Comparator<java.util.Map.Entry<Integer, Double>>() {
    		public int compare(java.util.Map.Entry<Integer, Double> o1, java.util.Map.Entry<Integer, Double> o2) {
    			if (o2.getValue() < o1.getValue()) {
					return -1;
				}else{
					return 1;
				}
    		}
		});
    	
    	int i = 0;
    	System.out.println("Number of Iterations: "+String.valueOf(iterations));
    	for (java.util.Map.Entry<Integer, Double> entry : rank) {
    		i++;
    		if (i<=10) {
				System.out.println(Double.toString(entry.getValue())+" "+Integer.toString(entry.getKey()));
			}
			dos.writeBytes(Double.toString(entry.getValue())+" "+Integer.toString(entry.getKey())+"\n");
		}
    	dos.close();
    	os.close();
    }

    public static void main(String[] args) throws IOException {
        SequentialPageRank sequentialPR = new SequentialPageRank();

        sequentialPR.parseArgs(args);
        sequentialPR.loadInput();
        sequentialPR.calculatePageRank();
        sequentialPR.printValues();
    }
}
