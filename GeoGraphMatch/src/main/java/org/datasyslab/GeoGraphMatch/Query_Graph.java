package org.datasyslab.GeoGraphMatch;

import java.util.ArrayList;

public class Query_Graph {
	public int[] label_list;
	ArrayList<ArrayList<Integer>> graph;
	
	public Query_Graph(int node_count) {
		label_list = new int[node_count];
		graph = new ArrayList<ArrayList<Integer>>(node_count);
	}
}
