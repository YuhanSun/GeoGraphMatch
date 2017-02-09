package org.datasyslab.GeoGraphMatch;

import java.util.ArrayList;

public class Query_Graph {
	public ArrayList<Integer> label_list;
	ArrayList<ArrayList<Integer>> graph;
	
	public Query_Graph(int node_count) {
		label_list = new ArrayList<Integer>(node_count);
		graph = new ArrayList<ArrayList<Integer>>(node_count);
	}
}
