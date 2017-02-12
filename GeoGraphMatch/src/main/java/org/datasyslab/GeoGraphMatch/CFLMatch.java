package org.datasyslab.GeoGraphMatch;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

import javax.sound.sampled.Line;

import org.neo4j.register.Register.Int;

public class CFLMatch {
	
	private static final boolean RESULT_ENUMERATION = false;
	
	//data graph
	public HashMap<Integer, Integer> label_cardinality;
	public int cnt_unique_label;
	
	//query graph
	public int[] nodes_label_query;
	public ArrayList<ArrayList<Integer>> graph;
	
	//query related parameters
	public int MAX_DEGREE_QUERY = 0;
	public int cnt_node_query = 0;
	public int[] core_number_query;
	public int[] node_degree_query;
	
	public int residual_tree_match_seq_index;
	public int residual_tree_leaf_node_index;
	public int NEC_mapping_pair_index;
	
	public int[] residual_tree_match_seq;
	public int[] NEC_map;
	public int[] NEC_mapping;
	public NEC_element[] NEC_mapping_pair;
	public int[] NEC_mapping_Actual;
	public NEC_node[] NEC_Node_array;
	
	public CFLMatch(HashMap<Integer, Integer> p_label_cardinality) {
		this.label_cardinality = p_label_cardinality;
		this.cnt_unique_label = p_label_cardinality.size();
		NEC_mapping = new int[(cnt_unique_label + 1) * cnt_node_query];
		NEC_mapping_pair = new NEC_element[(cnt_unique_label + 1) * cnt_node_query];
		NEC_mapping_Actual = new int[(cnt_unique_label + 1) * cnt_node_query];
		NEC_Node_array = new NEC_node [cnt_node_query];
		residual_tree_match_seq = new int[cnt_node_query];

	}
	
	public static void main(String[] args) {
		String datagraph_path = "/home/yuhansun/Documents/GeoGraphMatchData/hprd";
		HashMap<Integer, Integer> p_label_cardinality = Utility.Preprocess_DataGraph(datagraph_path);
		String query_graphs_path = "/home/yuhansun/Documents/GeoGraphMatchData/hprd25d";
    	//        	String query_graphs_path = "/home/yuhansun/Documents/GeoGraphMatchData/test_query_graph";
    	//        	String query_graphs_path = "/home/yuhansun/Documents/GeoGraphMatchData/human10s";
    	ArrayList<Query_Graph> query_Graphs = Utility.ReadQueryGraphs(query_graphs_path, 1);
    	CFLMatch cflMatch = new CFLMatch(p_label_cardinality);
    	cflMatch.SubgraphMatch(query_Graphs.get(0));
	}
	
	public void Initialize_Query_Parameter()
	{
		cnt_node_query = graph.size();
		for(ArrayList<Integer> line : graph)
			if(line.size() > MAX_DEGREE_QUERY)
				MAX_DEGREE_QUERY = line.size();
		OwnMethods.Print(String.format("Parameter Init done."));
		OwnMethods.Print(String.format("MAX_QUERY_NODE:%d\nMAX_DEGREE_QUERY:%d\n", cnt_node_query, MAX_DEGREE_QUERY));
		
	}
	
	
	
	public ArrayList<Int[]> SubgraphMatch(Query_Graph query_Graph)
	{
		this.graph = query_Graph.graph;
		this.nodes_label_query = query_Graph.label_list;
		Initialize_Query_Parameter();
		readQueryGraph();
		boolean isTree = true;
		for (int i = 0; i < cnt_node_query; i ++)
			if (core_number_query[i] >= 2){
				isTree = false;
				break;
			}
		
		if(isTree)
		{
			
		}
		else
		{
			
		}
		return null;
	}
	
	public void readQueryGraph()
	{
		//begin starting the core-decomposition, core number is the degree number
//		int * bin = bin_query;	//	int bin [MAX_DEGREE_QUERY + 1];
//		int * pos = pos_query;	//	int pos [cnt_node_query];
//		int * vert = vert_query;//	int vert [cnt_node_query];
		int [] bin = new int[MAX_DEGREE_QUERY+1];
		int [] pos = new int[cnt_node_query];
		int [] vert = new int[cnt_node_query];
		core_number_query = new int[cnt_node_query];
		node_degree_query = new int[cnt_node_query];

		for (int i = 0; i< cnt_node_query; i++)
		{
			core_number_query[i] = graph.get(i).size();
			node_degree_query[i] = graph.get(i).size();
		}
		
		for (int i = 0; i < cnt_node_query; i ++)
			bin[ core_number_query [i] ] ++;

		int start = 0;
		int num;

		for (int d = 0; d <= MAX_DEGREE_QUERY; d++){
			num = bin[d];
			bin[d] = start;
			start += num;
		}


		for (int i = 0; i < cnt_node_query; i++){
			pos[i] = bin[ core_number_query[i] ];
			vert[ pos[i] ] = i;
			bin[ core_number_query[i] ] ++;
		}

		for (int d = MAX_DEGREE_QUERY; d > 0; d --)
			bin[d] = bin[d-1];
		bin[0] = 0;

		for (int i = 0; i < cnt_node_query; i++){

			int v = vert[i];

			for (int j = 0; j < graph.get(v).size(); j ++){

				int u = graph.get(v).get(j); // nodes_query[v][j];

				if (core_number_query[u] > core_number_query[v]){

					int du = core_number_query[u];
					int pu = pos[u];

					int pw = bin[du];
					int w = vert[pw];

					if (u != w){	//if not the same node, switch the position of the two nodes.
						pos[u] = pw;
						pos[w] = pu;
						vert[pu] = w;
						vert[pw] = u;
					}

					bin[du] ++;
					core_number_query[u]--;
				}
			}
		}
	}

	public void Extract_Residual_Structures()
	{
		residual_tree_match_seq_index = 0;
		residual_tree_leaf_node_index = 0;
		NEC_mapping_pair_index = 0;

		NEC_map = new int[cnt_node_query];
		boolean[] visited = new boolean[cnt_node_query];
		
		for(int i = 0; i<cnt_node_query; i++)
		{
			NEC_map[i] = -1;
			visited[i] = false;
		}

		for (int i = 0; i < cnt_node_query; i++) {//for each node in the query

			if (core_number_query[i] < 2)//not in the two-core
				continue;

			//now i must be a 2-core node => next, we check whether i is a articulation node

			//for all of node i's children
			for (int j = 0; j < graph.get(i).size(); j ++){

				int child = graph.get(i).get(j);

				if (core_number_query[child] < 2){ // the child node is not in the 2-core

					//two cases here, the NEC node or a residual tree

					if (node_degree_query[child] == 1){ //degree is one ==> NEC node
						//============ CASE ONE: ONE-DEGREE NODES => NEC nodes =====================

						int label = nodes_label_query[child];

						if (NEC_mapping[label * cnt_node_query + i] == 0) 
						{
							NEC_mapping_pair[NEC_mapping_pair_index ++] = new NEC_element(label, i, child);// child is the representative node
							NEC_map [child] = child;//NEC map
							NEC_mapping_Actual[label * cnt_node_query + i] = child;

							if(RESULT_ENUMERATION)
							{
								//								NEC_Node_array[child].node = child;
								//								NEC_Node_array[child].nextAddress = NULL;
								NEC_Node_array[child].node = child;
								NEC_Node_array[child].next = -1;
							}
						}
						else 
						{
							NEC_map [child] = NEC_mapping_Actual[label * cnt_node_query + i];//NEC map

							if(RESULT_ENUMERATION)
							{
								int rep =NEC_mapping_Actual[label * cnt_node_query + i];
//								NEC_Node_array[child].node = child;
//								NEC_Node_array[child].nextAddress = NEC_Node_array[ rep ].nextAddress;
//								NEC_Node_array[ rep ].nextAddress = &NEC_Node_array[child];
								NEC_Node_array[child].node = child;
								NEC_Node_array[child].next = NEC_Node_array[ rep ].next;
								NEC_Node_array[ rep ].next = child;
							}
						}

						NEC_mapping[label * cnt_node_query + i] ++; // the label with parent being i, nec_count ++

					} else {
						//============ CASE TWO: NORMAL CASE, THE QUERY TREE ================
						// extract the query tree for extra region candidate extraction, based on DFS
						// also give a DFS-based query sequence at the same time

						int [] dfs_stack = new int[cnt_node_query];
						int dfs_stack_index = 0;

						visited[i] = true; // this is the start node's parent node (a marked node)
						visited[child] = true; // this is the start node

						dfs_stack[dfs_stack_index ++] = child;
						residual_tree_match_seq[residual_tree_match_seq_index ++] = child;

						tree_node_parent[child] = i;

						while (dfs_stack_index != 0) {

							int current_node = dfs_stack[dfs_stack_index - 1];
							dfs_stack_index--;

							int added_child = 0;

							for (int m = query_nodes_array_info[current_node]; m < query_nodes_array_info[current_node + 1]; m ++){

								int child_node = query_nodes_array[m];

								if (!visited[child_node]) {

									visited[child_node] = 1;

									//======== special treatment here: if a node is a leaf (degree being 1), then put it into nec node set
									if (node_degree_query[child_node] == 1){

										int label = nodes_label_query[child_node];

										if (NEC_mapping[label * MAX_QUERY_NODE + current_node] == 0) {
											NEC_mapping_pair[NEC_mapping_pair_index ++] = NEC_element(label, current_node, child_node);// child is the repesentive node
											NEC_map [child_node] = child_node;//NEC map
											NEC_mapping_Actual[label * MAX_QUERY_NODE + current_node] = child_node;
	#ifdef RESULT_ENUMERATION
											NEC_Node_array[child_node].node = child_node;
											NEC_Node_array[child_node].nextAddress = NULL;
	#endif
										}
										else{
											NEC_map [child_node] = NEC_mapping_Actual[label * MAX_QUERY_NODE + current_node];//NEC map
	#ifdef RESULT_ENUMERATION
											int rep = NEC_mapping_Actual[label * MAX_QUERY_NODE + current_node];
											NEC_Node_array[child_node].node = child_node;
											NEC_Node_array[child_node].nextAddress = NEC_Node_array[ rep ].nextAddress;
											NEC_Node_array[ rep ].nextAddress = &NEC_Node_array[child_node];
	#endif
										}
										NEC_mapping[label * MAX_QUERY_NODE + current_node]++; // the label with parent being i, nec_count ++
										continue;
									}
									//===========================================================
									tree_node_parent[child_node] = current_node;
									added_child ++;
									dfs_stack[dfs_stack_index ++] = child_node;
									residual_tree_match_seq[residual_tree_match_seq_index ++] = child_node;
								}

								if (added_child == 0)//this information is recorded for extracting the matching sequence for the tree matching sequence.
									residual_tree_leaf_node[residual_tree_leaf_node_index ++] = make_pair(current_node, 0);
							}
						}
					}
				}
			}
		}


		//================ construct the NEC set by label: each label is with a vector which contains many NECs with this label.=========
		sort(NEC_mapping_pair, NEC_mapping_pair + NEC_mapping_pair_index, sort_by_NEC_label);
		int last_label;
		NEC_set_index = 0;
		NEC_set_by_label_index.clear();
		int sum;
		if (NEC_mapping_pair_index == 1){
			NEC_element & nec_ele = NEC_mapping_pair[0];
			int label = nec_ele.label;
			int parent_id = nec_ele.parent_id;
			int represent_child = nec_ele.represent_node;
			sum = NEC_mapping[label * MAX_QUERY_NODE + parent_id];
			NEC_mapping[label * MAX_QUERY_NODE + parent_id] = 0; //reset it back to 0
			NEC_set_by_label_index.push_back(make_pair(label, NEC_set_index));
			NEC_set_array[NEC_set_index ++] = NEC_set_array_element(parent_id, represent_child, sum);
			NEC_set_by_label_index.push_back(make_pair(-1, NEC_mapping_pair_index)); // redundant element to set the end
		} else {
			for (int i = 0; i < NEC_mapping_pair_index; i++) {

				NEC_element & nec_ele = NEC_mapping_pair[i];

				int label = nec_ele.label;
				int parent_id = nec_ele.parent_id;
				int represent_child = nec_ele.represent_node;
				sum = NEC_mapping[label * MAX_QUERY_NODE + parent_id];
				NEC_mapping[label * MAX_QUERY_NODE + parent_id] = 0; //reset it back to 0

				if (i == 0) {
					NEC_set_by_label_index.push_back(make_pair(label, NEC_set_index));
					NEC_set_array[NEC_set_index ++] = NEC_set_array_element(parent_id, represent_child, sum);
					last_label = label;
					continue;
				} else if (i == NEC_mapping_pair_index - 1) {
					if (label != last_label)
						NEC_set_by_label_index.push_back(make_pair(label, NEC_set_index));
					NEC_set_array[NEC_set_index ++] = NEC_set_array_element(parent_id, represent_child, sum);
					NEC_set_by_label_index.push_back(make_pair(-1, NEC_mapping_pair_index)); // redunant element to set the end
					continue;
				}

				if (label != last_label) {
					NEC_set_by_label_index.push_back(make_pair(label, NEC_set_index));
					last_label = label;
				}

				NEC_set_array[NEC_set_index ++] = NEC_set_array_element(parent_id, represent_child, sum);
			}
		}

	#ifdef RESULT_ENUMERATION
		for (int i = 0; i < cnt_node_query; i++){
			if (node_degree_query[i] != 1)
				continue;
			NEC_Node * next = NEC_Node_array[i].nextAddress;
			if (next == NULL)
				continue;
			while (next != NULL)
				next = next->nextAddress;
		}

	#endif


	#ifdef	OUTPUT_EXTRA_INFO

		int sum_node = 0;

		if (NEC_mapping_pair_index != 0){
			for (int i = 0; i < NEC_set_by_label_index.size() - 1; i++) {
				int label = NEC_set_by_label_index[i].first;
				int start = NEC_set_by_label_index[i].second;
				int end = NEC_set_by_label_index[i + 1].second;

				for (int j = start; j < end; j++) {
					int parent_id = NEC_set_array[j].parent_id;
					int sum = NEC_set_array[j].sum;
					sum_node += sum;
					cerr << "label :" << label << " => parent id " << parent_id << " \t sum => " << sum
							<< "\t representative node is " << NEC_set_array[j].represent_node<< endl;
				}
			}
		}

		cerr << "NEC classes contained: " << NEC_mapping_pair_index << " classes with " << sum_node << " nodes " << endl;
		cerr << "Query trees with sum node: " << residual_tree_match_seq_index
					<< " and tree leaf index is " << residual_tree_leaf_node_index << endl;
	#endif

	}
}
