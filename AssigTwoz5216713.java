/*

==============+==============+==========+===============
              | Assignment 2 |          | Oleg Kufirin |
==============+==============+==========+===============

This program uses Breadth-First Search iterated algorithm over a graph.
Frontier advances from origin by one level with each pass.
Iterated passes through MapReduce - map some nodes, result includes additional nodes
which are fed into successive MapReduce passes.

A map task receives a node n as a key and (D, path, points_to) as a value:
- D is the distance to the node from the start
- path is the path to the node from the start
- points_to is a list of nodes reachable from n
- for all p belong to points_to emit(p, D+w_p, path+current_node)

Reduce task gathers possible distances to a given p and selects the minimum one.

This MapReuduce task can advance the known frontier by one hop.
To perform the whole BFS, a non-MapReduce component then feeds the output of this step back 
into the MapReduce task for another iteration.
Note: Mapper emits (n, points-to) as well

High level steps of the program:
1. Read the input and map a node to adjacent node names and distances
2. Aggregate by key to have a value of RDD of a custom class Node()
3. Create new RDD distance_init to initialize the starting point from the start node with distance value 0
4. Perform the first step of MapReduce iteration described above
5. In a for loop limited by number of nodes perform all the other MapReduce iterations
6. Sort output by shortest distance of object Node()
7. Filter out the record of the starting node from the output
8. Export to a text file

The main functions representing MapReduce paradigm are MainMapper and MainReducer.

References:
https://courses.cs.washington.edu/courses/cse490h/08au/lectures/algorithms.pdf
http://www.cse.uoi.gr/~kontog/courses/Algs4World/slides/MapReduce-Algorithms-Models.pdf
https://www.youtube.com/watch?v=BT-piFBP4fE

*/

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AssigTwoz5216713 {
	// class representing a node (vertex) object
	public static class Node implements Serializable {
		
		Integer distance; // current shortest distance from the source node
		String path; // current shortest path from the source node
		ArrayList<Tuple2<String,Integer>> points_to; // list of adjacent nodes and distances to them
		
		public Node() {
			this.distance = 999; // initial shortest distance from the source node is infinity
			this.path = "";
			this.points_to = new ArrayList<Tuple2<String,Integer>>();
		}
		
		public void add_adjacent_node(Tuple2<String, Integer> pair) {
			this.points_to.add(pair);
		}
		
		public void change_distance(Integer new_distance) {
			this.distance = new_distance;
		}
		
		// method for making a string only of adjacent vertices and distances to them
		public String toString_pointsTo() {
			StringBuilder sb = new StringBuilder();
			
			if (this.points_to != null)
				for (Tuple2<String,Integer> n: this.points_to)
					sb.append(n._1 + ":" + n._2 + " ");
			
			return sb.toString().trim();
		}
		
		// final printing method
		public String toString() {
//			StringBuilder sb = new StringBuilder();
			Integer formattedDistance;
			
			// converting infinity (999) to -1 as per spec
			if (this.distance == 999)
				formattedDistance = -1;
			else
				formattedDistance = this.distance;
			
//			if (this.points_to != null)
//				for (Tuple2<String,Integer> n: this.points_to)
//					sb.append(n._1 + ":" + n._2 + " ");
			
			return formattedDistance.toString() + "," + this.path;
//			return this.distance.toString() + "," + this.path + "," + sb.toString().trim();
		}
	}

	
	// zeroValue function for aggregateByKey
	public static class seqFunc implements Function2<Node, Tuple2<String, Integer>, Node> {
		@Override
		public Node call(Node val1, Tuple2<String, Integer> val2)
				throws Exception {
			val1.add_adjacent_node(val2);
			return val1;
		}
	}	

	
	// combiner function for aggregateByKey
	public static class combFunc implements Function2<Node, Node, Node> {
		@Override
		public Node call(Node val1, Node val2)
				throws Exception {
			val1.add_adjacent_node(val2.points_to.get(0));
			return val1;
		}
	}
	
	
	// main Map function that emits distances, paths and graph structure.
	public static class MainMapper implements PairFlatMapFunction<Tuple2<String, Node>, String, String> {
		@Override
		public Iterator<Tuple2<String, String>> call(Tuple2<String, Node> input) throws Exception {
			ArrayList<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
			ArrayList<Tuple2<String,Integer>> neighbours = input._2().points_to;
			
			// pass in neighbors nodes distance and paths via current node
			for(int j=0; j<neighbours.size(); j++) {
				Integer new_distance = neighbours.get(j)._2 + input._2().distance;
				Tuple2<String, String> possible_dist = new Tuple2<String, String>(neighbours.get(j)._1, 
						"VALS " + new_distance.toString() + ":" + input._2.path +"-"+ neighbours.get(j)._1);
				ret.add(possible_dist);
			}
			
			// pass in current node's distance and path
			Integer curr_distance = input._2().distance;
			Tuple2<String, String> possible_dist = new Tuple2<String, String>(input._1, 
					"VALS " + curr_distance.toString() + ":" + input._2.path);
			ret.add(possible_dist);
			
			// pass in the adjacency list of neighboring nodes
			Tuple2<String, String> graph_str = new Tuple2<String, String>(input._1, "NODE " + input._2.toString_pointsTo());
			ret.add(graph_str);
			
			return ret.iterator();
		}
	}
	
	
	// main Reducer function that updates distances, paths and emits graph structure
	public static class MainReducer implements PairFunction<Tuple2<String,Iterable<String>>, String, Node> {
		@Override
		public Tuple2<String, Node> call(Tuple2<String, Iterable<String>> input) throws Exception {
			Integer lowest = 999;
			String lowest_path = "";
			Node node = new Node();
			
			for (String val: input._2) { // parsing the value string
				String[] sp = val.toString().split(" ");
				if (sp[0].equalsIgnoreCase("NODE")) { // recover graph structure
					for (int k=1; k<sp.length; k++) {
						String[] pair = sp[k].split(":");
						Tuple2<String, Integer> pair_tuple = new Tuple2<String, Integer>(pair[0], Integer.parseInt(pair[1]));
						node.add_adjacent_node(pair_tuple);
					}
				}
				else if (sp[0].equalsIgnoreCase("VALS")) { //look for shorter distance and respective paths
					String[] spr = sp[1].toString().split(":");
					Integer distance = Integer.parseInt(spr[0]);
					if (distance < lowest) {
						lowest = distance;
						lowest_path = spr[1];
					}	
				}
			}
			
			node.change_distance(lowest); // update shortest distance
			node.path = node.path + lowest_path; // update shortest path
			
			return new Tuple2<String, Node>(input._1, node);
		}	
	}
	
	
	// swapping key and value forward for sorting by key
	public static class SwapMapper_forward implements PairFunction<Tuple2<String,Node>, Node, String> {
		@Override
		public Tuple2<Node, String> call(Tuple2<String, Node> pair) throws Exception {
			return pair.swap();
		}
	}
	
	
	// swapping key and value backward for output
	public static class SwapMapper_backward implements PairFunction<Tuple2<Node,String>, String, Node> {

		@Override
		public Tuple2<String, Node> call(Tuple2<Node, String> pair) throws Exception {
			return pair.swap();
		}
	}
	
	
	// compare two nodes by their shortest distance from the source node
	public static class NodeComparator implements Comparator<Node>, Serializable {
		@Override
		public int compare(Node o1, Node o2) {
			return o1.distance - o2.distance;
		}
	}
	
	
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Assignment 2").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaRDD<String> input = context.textFile(args[1]); // reading input file

		// first mapping of a node to adjacent node names and distances
		JavaPairRDD<String, Tuple2<String, Integer>> init_first = input.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Tuple2<String, Integer>> call(String line) throws Exception {
				String[] parts = line.split(",");
				String name = parts[0];
				String adjacent_node = parts[1];
				Integer adjacent_distance = Integer.parseInt(parts[2]);
		
				return new Tuple2<String, Tuple2<String, Integer>>(name, new Tuple2<>(adjacent_node, adjacent_distance));
			}
		});

		// aggregation in order to have Node type object as a value
		JavaPairRDD<String, Node> aggregation  = init_first.aggregateByKey(new Node(), new seqFunc(), new combFunc());
		
		// initialize the source node distance as 0
		JavaPairRDD<String, Node> distance_init  = aggregation.mapToPair(new PairFunction<Tuple2<String, Node>, String, Node>() {
			@Override
			public Tuple2<String, Node> call(Tuple2<String, Node> input) throws Exception {
				if(input._1.compareTo(args[0]) == 0) {
					input._2.change_distance(0);
					input._2.path = input._1;				
				}

				return new Tuple2<String, Node>(input._1, input._2);
			}
			
		});
		
		// first iteration of finding shortest path via Map-Reduce
		JavaPairRDD<String, String> mapper = distance_init.flatMapToPair(new MainMapper());
		JavaPairRDD<String, Node> reducer = mapper.groupByKey().mapToPair(new MainReducer());
		
		Integer number_of_nodes = (int) reducer.count(); // getting number of nodes
		
		// iterative BFS via Map-Reduce
		for (int i=1; i<number_of_nodes; i++) {
			mapper = reducer.flatMapToPair(new MainMapper());
			reducer = mapper.groupByKey().mapToPair(new MainReducer());
		}
		
		// swapping key-value for sorting
		JavaPairRDD<Node, String> swapped = reducer.mapToPair(new SwapMapper_forward());
		// sorting nodes by ascending distance
		JavaPairRDD<Node, String> sorted = swapped.sortByKey(new NodeComparator());
		// swapping key-value back
		JavaPairRDD<String, Node> finalSwapped = sorted.mapToPair(new SwapMapper_backward());
		
		// function for filtering out the source node from the final RDD
		Function<Tuple2<String, Node>, Boolean> filterFunction = w -> (w._1.compareTo(args[0]) != 0);
		JavaPairRDD<String, Node> result = finalSwapped.filter(filterFunction);
		
//		result.collect().forEach(System.out::println); // printing on the screen
		result.saveAsTextFile(args[2]); // printing in the output file
		
	}

}
