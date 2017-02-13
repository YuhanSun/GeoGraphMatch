package org.datasyslab.GeoGraphMatch;

import java.util.ArrayList;

public class Core_query_tree_node {
	int parent_node = -1;
	Pair<Integer, Integer> children;  // start_pos, length
	Pair<Integer, Integer> nte;		  // start_pos, length
	ArrayList<Integer> cross_lvl_nte;
}
