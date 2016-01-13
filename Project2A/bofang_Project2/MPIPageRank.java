import java.io.*;
import java.security.KeyStore.Entry;
import java.util.*;
import mpi.MPI;
import java.lang.Math;

//Shaowen Ren(shaoren)
//Bo Fang(bofang)

public class MPIPageRank {
    private HashMap<Integer, ArrayList<Integer>> adjMatrix = new HashMap<Integer, ArrayList<Integer>>();
    private String inputFile = "";
    private String outputFile = "";
    private int iterations = 10;
    private double df = 0.85,delta = 0.0001;
    private double rankValues[],rank2[],tempRank[];
    
    private int size,rank;
    private int size0,size1;
    private int url_size;
    
    public void parseArgs(String[] args) {
    	//mpjrun.sh -np 8 MPIPageRank <input_file_name> <output_file_name> <delta> <damping factor> <iterations>
    	inputFile = args[3];
    	outputFile = args[4];
    	if (args[5] != null) {
    		delta = Double.valueOf(args[5]);
    	}
    	if (args[6] != null) {
    		df = Double.valueOf(args[6]);
    	}
    	if (args[7] != null) {
    		iterations = Integer.valueOf(args[7]);
    	}
    }

    public void loadInput() throws IOException {
    	Scanner s = new Scanner(new File(inputFile));
    	while (s.hasNextLine()) {
    		Scanner line = new Scanner(s.nextLine());
    		Integer key = line.nextInt();
    		ArrayList<Integer> value = new ArrayList<>();        
    		while (line.hasNext()) {
    			value.add(line.nextInt());
    		}
    		adjMatrix.put(key,value);
    		line.close();
    	}
    	s.close();        
    }

    public void printValues() throws IOException {
    	HashMap<Integer, Double> rankValues1 = new HashMap<Integer, Double>();
    	for (int i = 0; i < rankValues.length; i++) {
			rankValues1.put(i, rankValues[i]);
		}
    	
    	List<java.util.Map.Entry<Integer, Double>> rank = new ArrayList<java.util.Map.Entry<Integer, Double>>(rankValues1.entrySet());
    	Collections.sort(rank, new Comparator<java.util.Map.Entry<Integer, Double>>() {
    		public int compare(java.util.Map.Entry<Integer, Double> o1, java.util.Map.Entry<Integer, Double> o2) {
    			if (o2.getValue() < o1.getValue()) {
					return -1;
				}
				if (o2.getValue() > o1.getValue()) {
					return 1;
				}
    			return 0;
    		}
		});
        File f = new File(outputFile);
        OutputStream os = new FileOutputStream(f);
        DataOutputStream dos = new DataOutputStream(os);	
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

    private int getstartpoint(int rank) {
    	if (rank == 0) {
			return 0;
		} else {
			return size0 + (rank - 1) * size1;
		}
    }
    
    public void mpi(String[] args) throws IOException {
    	//initial
    	MPI.Init(args);
    	rank = MPI.COMM_WORLD.Rank();
        size = MPI.COMM_WORLD.Size();
        //parseArgs and save data
        parseArgs(args);
        loadInput();
        
        //url_size is total size of all urls
        url_size = adjMatrix.size();
        rankValues = new double[url_size];
        //get initial rankValues
        for (int i = 0; i < url_size; i++) {
			rankValues[i] = 1.0 / url_size;
		}
        if (url_size % size == 0) {
        	//when the urls can be divided evenly
        	size0 = url_size / size;
        	size1 = size0;
        } else {
        	//when urls cannot be divided evenly, we are finding a most efficient way of allocating
        	size1 = url_size / size + 1;
        	size0 = url_size - size1 * (size - 1);
        }
        iterloop:
        for (int i = 0; i < iterations; i++) {
			if (rank == 0) {
				//send rankValues in this iteration
				for (int j = 1; j < size; j++) {
					MPI.COMM_WORLD.Send(rankValues, getstartpoint(j), size1, MPI.DOUBLE, j, 1);
				}
			} else {
				//receive rankValues in this iteration
				MPI.COMM_WORLD.Recv(rankValues, getstartpoint(rank), size1, MPI.DOUBLE, 0, 1);
			}
        	//copy an original data before calculating
			rank2 = rankValues.clone();
			//now every process start pageRank
        	for (int j = getstartpoint(rank); j < getstartpoint(rank + 1); j++) {
				double sum = 0;
				for (int j2 = 0; j2 < url_size; j2++) {
					if (adjMatrix.get(j2).indexOf(j) != -1) {
						sum += rankValues[j2] / adjMatrix.get(j2).size();
					}
				}
				rank2[j] = (1 - df) / url_size + df * sum;
			}
        	tempRank = rankValues.clone();
        	rankValues = rank2;
        	if (rank == 0) {
				//receive new ranks
        		for (int j = 1; j < size; j++) {
        			MPI.COMM_WORLD.Recv(rankValues, getstartpoint(j), size1, MPI.DOUBLE, j, 1);
				}
			} else {
				//send new ranks to process 0
				MPI.COMM_WORLD.Send(rankValues, getstartpoint(rank), size1, MPI.DOUBLE, 0, 1);
			}
        	//compare new rank values(rankValues) with previous rank values(tempRank)
        	//if the abs of the difference is less than delta, then break the iteration loop
        	if (rank == 0) {
        		boolean final_pass = true;    
        		innerloop:
        			for (int k = 0; k < url_size; k++){
        				double dif = Math.abs(tempRank[k] - rank2[k]);    
        				if (dif < delta){
        					continue innerloop;
        				} else {
        					final_pass = false;
        					break innerloop;
        				}
        			}
        		if (final_pass) break iterloop;
        	}
        }
        if (rank == 0) {
        	printValues();
        }
        MPI.Finalize();
    }
    
    public static void main(String[] args) throws IOException {
    	MPIPageRank mpiPR = new MPIPageRank();
        mpiPR.mpi(args);
    }
}
