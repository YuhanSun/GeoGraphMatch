package org.datasyslab.GeoGraphMatch;

import java.lang.reflect.Array;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.sound.sampled.Line;

import org.apache.lucene.search.FieldCache.IntParser;
import org.neo4j.register.Register.Int;

public class CFLMatch {
	
	private static final boolean RESULT_ENUMERATION = false;
	private static final boolean OUTPUT_EXTRA_INFO = false;
	private static final boolean CORE_AND_MAX_NB_FILTER = true;
	private static final int SIZEOF_INT = 32;
	
	//data graph
	public HashMap<Integer, Integer> label_cardinality;
	public int cnt_unique_label;
	public int cnt_node;
	
	public ArrayList<Integer> label_deg_label_pos;
	public ArrayList<Label_Degree_Node> label_degree_nodes;
	
	public int[] core_number_data;
	public int[] MAX_NB_degree_data;
	
	public Pair<Integer, Integer>[] nodes_to_label_info;
	public int[] nodes_data;
	
	public int[] degree_data;
	/**
	 * used and initialized in construct function
	 */
	public int max_label_counter;
	
	//query graph
	public int[] nodes_label_query;
	public ArrayList<ArrayList<Integer>> graph;
	
	//query related parameters
	public int MAX_DEGREE_QUERY = 0;
	public int cnt_node_query = 0;
	public int[] core_number_query;
	public int[] node_degree_query;
	public int sum_degree_cur = 0;
	public int count_global_temp_array_1;
	
	public int residual_tree_match_seq_index;
	public int[] residual_tree_match_seq;
	
	public int residual_tree_leaf_node_index;
	public Pair<Integer, Double> [] residual_tree_leaf_node;
	
	public int NEC_mapping_pair_index;
	public ArrayList<NEC_element> NEC_mapping_pair;
	
	public int NEC_set_index;
	public NEC_set_array_element[] NEC_set_array;
	
	public int[] tree_node_parent;
	public int[] NEC_map;
	public int[] NEC_mapping;
	public int[] NEC_mapping_Actual;
	public NEC_Node[] NEC_Node_array;
	
	public ArrayList<Pair<Integer, Integer>> NEC_set_by_label_index;
	
	public Core_query_tree_node[] core_query_tree;
	
	public int simulation_sequence_index;
	public ArrayList<Integer> simulation_sequence;	//needs to be sorted
	
	public ArrayList<Pair<Integer, Integer>> level_index;
	public int[] BFS_level_query;
	public int[] BFS_parent_query;
	
	public int root_node_id;
	
	//variables in main
	public int nte_array_for_matching_unit_index = 0;
	public int matching_sequence_index =0;
	
	//local variable used in BFS_NORMAL and BFS_TREE
	public int core_tree_node_child_array_index = 0;
	public int[] core_tree_node_child_array;
	
	public int core_tree_node_nte_array_index = 0;
	public int[] core_tree_node_nte_array;
	
	//the two are not used actually
	public int exploreCRSequence_indx = 0;
	public int[] exploreCRSequence;
	
	public NodeIndexUnit[] indexSet;
	public ArrayList<Pair<Integer, Integer>> seq_edge_this_level;
	
	public int NLF_size = -1;
	public int[] NLF_array;	//for query // neighborhood label array
	public int[] NLF_check; //for data
	
	public int[] simulation_check_array;
	
	public int[] array_to_clean;
	public int to_clean_index;
	
	public int[] flag_prelin_char;
	public int count_index_array_for_indexSet;
	public int[] flag_child_cand;
	public int[] index_array_for_indexSet;
	
	public CFLMatch(Data_Graph data_Graph) {
		this.label_cardinality = data_Graph.label_cardinality;
		this.cnt_unique_label = label_cardinality.size();
		
		this.max_label_counter = 0;
		for( int cardinality : label_cardinality.values())
			if(cardinality > max_label_counter)
				max_label_counter = cardinality;
		
		//these variables will be null without explicitly call compute function in Data_Graph
		this.label_deg_label_pos = data_Graph.label_deg_label_pos;
		this.label_degree_nodes = data_Graph.label_degree_nodes;
		this.core_number_data =  data_Graph.core_number_data;
		this.MAX_NB_degree_data = data_Graph.MAX_NB_degree_data;
		
		this.nodes_to_label_info = data_Graph.nodes_to_label_info;
		this.nodes_data = data_Graph.nodes_data;
		
		this.degree_data = data_Graph.degree_data;
	}
	
	public static void main(String[] args) {
		HashMap<Integer, Integer> transfer_table = null;
		Data_Graph data_Graph = null;
		try {
			String datagraph_path = "/home/yuhansun/Documents/GeoGraphMatchData/hprd";
			String transfer_table_path = "/home/yuhansun/Documents/GeoGraphMatchData/transfertable_hprd.txt";
			String query_graphs_path = "/home/yuhansun/Documents/GeoGraphMatchData/hprd25d";
	    	//        	String query_graphs_path = "/home/yuhansun/Documents/GeoGraphMatchData/test_query_graph";
	    	//        	String query_graphs_path = "/home/yuhansun/Documents/GeoGraphMatchData/human10s";
			transfer_table = Utility.Read_Transfer_Table(transfer_table_path);
			if(transfer_table == null)
				throw new Exception("Read transfer_table failed!");
			
			data_Graph = new Data_Graph(datagraph_path, transfer_table);
			data_Graph.Calculate_Label_Cardinality();
			data_Graph.Calculate_Label_Degree_Nodes();
			
//	    	ArrayList<Query_Graph> query_Graphs = Utility.ReadQueryGraphs(query_graphs_path, transfer_table, 1);
//	    	CFLMatch cflMatch = new CFLMatch(p_label_cardinality);
//	    	cflMatch.SubgraphMatch(query_Graphs.get(0));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void Initialize_Query_Parameter()
	{
		cnt_node_query = graph.size();
		for(ArrayList<Integer> line : graph)
		{
			if(line.size() > MAX_DEGREE_QUERY)
				MAX_DEGREE_QUERY = line.size();
			sum_degree_cur += line.size();
		}
		OwnMethods.Print(String.format("Parameter Init done."));
		OwnMethods.Print(String.format("MAX_QUERY_NODE:%d\nMAX_DEGREE_QUERY:%d\n", cnt_node_query, MAX_DEGREE_QUERY));
		
		coreDecompositition_query();
		
		NEC_mapping = new int[(cnt_unique_label + 1) * cnt_node_query];
		NEC_mapping_pair = new ArrayList<NEC_element>((cnt_unique_label + 1) * cnt_node_query);
		NEC_mapping_Actual = new int[(cnt_unique_label + 1) * cnt_node_query];
		NEC_Node_array = new NEC_Node [cnt_node_query];
		residual_tree_match_seq = new int[cnt_node_query];
		tree_node_parent = new int[cnt_node_query];
		residual_tree_leaf_node = new Pair[cnt_node_query];
		NEC_set_by_label_index = new ArrayList<Pair<Integer,Integer>>();
		NEC_set_array = new NEC_set_array_element[(cnt_unique_label + 1) * cnt_node_query];
		core_query_tree =  new Core_query_tree_node[cnt_node_query];
		
		BFS_level_query = new int[cnt_node_query];
		BFS_parent_query = new int[cnt_node_query];
		
		simulation_sequence = new ArrayList<Integer>(Collections.nCopies(cnt_node_query, 0));
		
		for(int i = 0; i<cnt_node_query; i++)
		{
			Pair<Integer, Double> pair = new Pair<Integer, Double>(0, (double) 0);
			residual_tree_leaf_node[i] = pair;
		}
		
		core_tree_node_nte_array = new int[sum_degree_cur];
		core_tree_node_child_array = new int[sum_degree_cur];
		
		NLF_size = (cnt_unique_label + 1) / SIZEOF_INT + 1;
		NLF_array = new int[NLF_size];
		NLF_check = new int[cnt_node * NLF_size];
		
		exploreCRSequence = new int[cnt_node_query];//not useful
		
		indexSet = new NodeIndexUnit[cnt_node_query];
		for ( int i = 0; i < cnt_node_query; i++)
		{
			indexSet[i].candidates = new int[max_label_counter];
			indexSet[i].path = new long [max_label_counter];
			indexSet[i].parent_cand_size = 0;
		}
		
		seq_edge_this_level = new ArrayList<Pair<Integer,Integer>>();
		simulation_check_array =  new int[cnt_node];
		
		array_to_clean = new int[cnt_node * cnt_node_query];
		to_clean_index = 0;
		
		flag_prelin_char = new int[cnt_node];
		flag_child_cand = new int[cnt_node];
		index_array_for_indexSet = new int[cnt_node];
	}
	
	
	
	public ArrayList<Int[]> SubgraphMatch(Query_Graph query_Graph)
	{
		this.graph = query_Graph.graph;
		this.nodes_label_query = query_Graph.label_list;
		Initialize_Query_Parameter();
		
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
			Extract_Residual_Structures();
			root_node_id = start_node_selection_NORMAL();
			BFS_NORMAL();
			nte_array_for_matching_unit_index = 0;
			matching_sequence_index =0;
//			exploreCR_FOR_CORE_STRUCTURE();
//			exploreCR_Residual_NORMAL();
//			simulation_NORMAL();
//			getCORE_sequence_CORE();
//			if (residual_tree_match_seq_index >= 2)
//				getTreeMatchingSequence_NORMAL();
		}
		return null;
	}
	
	/**
	 * initialize core_number_query
	 */
	public void coreDecompositition_query()
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
							NEC_mapping_pair.add(new NEC_element(label, i, child));
							NEC_mapping_pair_index++;
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

							for (int m = 0; m < graph.get(current_node).size(); m ++){

								int child_node = graph.get(current_node).get(m);

								if (!visited[child_node]) {

									visited[child_node] = true;

									//======== special treatment here: if a node is a leaf (degree being 1), then put it into nec node set
									if (node_degree_query[child_node] == 1){

										int label = nodes_label_query[child_node];

										if (NEC_mapping[label * cnt_node_query + current_node] == 0) {
											NEC_mapping_pair.add(new NEC_element(label, current_node, child_node));
											NEC_mapping_pair_index++;
											NEC_map [child_node] = child_node;//NEC map
											NEC_mapping_Actual[label * cnt_node_query + current_node] = child_node;
											if(RESULT_ENUMERATION)
											{
												NEC_Node_array[child_node].node = child_node;
												NEC_Node_array[child_node].next = -1;
											}
										}
										else{
											NEC_map [child_node] = NEC_mapping_Actual[label * cnt_node_query + current_node];//NEC map
											if(RESULT_ENUMERATION)
											{
												int rep = NEC_mapping_Actual[label * cnt_node_query + current_node];
												NEC_Node_array[child_node].node = child_node;
												NEC_Node_array[child_node].next = NEC_Node_array[ rep ].next;
												NEC_Node_array[ rep ].next = child_node;
											}
										}
										NEC_mapping[label * cnt_node_query + current_node]++; // the label with parent being i, nec_count ++
										continue;
									}
									//===========================================================
									tree_node_parent[child_node] = current_node;
									added_child ++;
									dfs_stack[dfs_stack_index ++] = child_node;
									residual_tree_match_seq[residual_tree_match_seq_index ++] = child_node;
								}

								if (added_child == 0)//this information is recorded for extracting the matching sequence for the tree matching sequence.
								{
									residual_tree_leaf_node[residual_tree_leaf_node_index ++] = new Pair<Integer, Double>(current_node, (double) 0);
								}
							}
						}
					}
				}
			}
		}


		//================ construct the NEC set by label: each label is with a vector which contains many NECs with this label.=========
//		sort(NEC_mapping_pair, NEC_mapping_pair + NEC_mapping_pair_index, sort_by_NEC_label);
		Collections.sort(NEC_mapping_pair.subList(0, NEC_mapping_pair_index), new Comparator<NEC_element>() {

			public int compare(NEC_element o1, NEC_element o2) {
				return o1.label.compareTo(o2.label);
			}
			
		});
		int last_label = 0;
		NEC_set_index = 0;
		NEC_set_by_label_index.clear();
		int sum;
		if (NEC_mapping_pair_index == 1){
			NEC_element nec_ele = NEC_mapping_pair.get(0);
			int label = nec_ele.label;
			int parent_id = nec_ele.parent_id;
			int represent_child = nec_ele.represent_node;
			sum = NEC_mapping[label * cnt_node_query + parent_id];
			NEC_mapping[label * cnt_node_query + parent_id] = 0; //reset it back to 0
			NEC_set_by_label_index.add(new Pair<Integer, Integer>(label, NEC_set_index));
			NEC_set_array[NEC_set_index ++] = new NEC_set_array_element(parent_id, represent_child, sum);
			NEC_set_by_label_index.add(new Pair<Integer, Integer>(-1, NEC_mapping_pair_index)); // redundant element to set the end
		} else {
			for (int i = 0; i < NEC_mapping_pair_index; i++) {

				NEC_element  nec_ele = NEC_mapping_pair.get(i);

				int label = nec_ele.label;
				int parent_id = nec_ele.parent_id;
				int represent_child = nec_ele.represent_node;
				sum = NEC_mapping[label * cnt_node_query + parent_id];
				NEC_mapping[label * cnt_node_query + parent_id] = 0; //reset it back to 0

				if (i == 0) {
					NEC_set_by_label_index.add(new Pair<Integer, Integer>(label, NEC_set_index));
					NEC_set_array[NEC_set_index ++] = new NEC_set_array_element(parent_id, represent_child, sum);
					last_label = label;
					continue;
				} else if (i == NEC_mapping_pair_index - 1) {
					if (label != last_label)
						NEC_set_by_label_index.add(new Pair<Integer, Integer>(label, NEC_set_index));
					NEC_set_array[NEC_set_index ++] = new NEC_set_array_element(parent_id, represent_child, sum);
					NEC_set_by_label_index.add(new Pair<Integer, Integer>(-1, NEC_mapping_pair_index)); // redunant element to set the end
					continue;
				}

				if (label != last_label) {
					NEC_set_by_label_index.add(new Pair<Integer, Integer>(label, NEC_set_index));
					last_label = label;
				}

				NEC_set_array[NEC_set_index ++] = new NEC_set_array_element(parent_id, represent_child, sum);
			}
		}

	if(RESULT_ENUMERATION)
	{
		for (int i = 0; i < cnt_node_query; i++){
			if (node_degree_query[i] != 1)
				continue;
			int next_id = NEC_Node_array[i].next;
			if (next_id == -1)
				continue;
			while (next_id != -1)
				next_id = NEC_Node_array[next_id].next;
		}
	}
		
//	if(OUTPUT_EXTRA_INFO)
//	{
//		int sum_node = 0;
//
//		if (NEC_mapping_pair_index != 0){
//			for (int i = 0; i < NEC_set_by_label_index.size() - 1; i++) {
//				int label = NEC_set_by_label_index.get(i).getLeft();
//				int start = NEC_set_by_label_index.get(i).getRight();
//				int end = NEC_set_by_label_index.get(i + 1).getRight();
//
//				for (int j = start; j < end; j++) {
//					int parent_id = NEC_set_array[j].parent_id;
//					int sum_extrainfor = NEC_set_array[j].sum;
//					sum_node += sum_extrainfor;
//					cerr << "label :" << label << " => parent id " << parent_id << " \t sum => " << sum_extrainfor
//							<< "\t representative node is " << NEC_set_array[j].represent_node<< endl;
//				}
//			}
//		}
//
//		cerr << "NEC classes contained: " << NEC_mapping_pair_index << " classes with " << sum_node << " nodes " << endl;
//		cerr << "Query trees with sum node: " << residual_tree_match_seq_index
//					<< " and tree leaf index is " << residual_tree_leaf_node_index << endl;
//	}

	}

	public int start_node_selection_NORMAL(){

		double least_ranking = 0.0;
		int start_node = -1;
		double ranking;
		int label;
		int degree;
		
		int loop_start = 0;
		for (int i = 0; i < cnt_node_query; i++)
		{
			if(core_number_query[i] < 2)
				continue;
			label = nodes_label_query[i];
			degree = node_degree_query[i];
			ranking = (double)(label_cardinality.get(label)) /(double)degree ;
			start_node = i;
			loop_start = i+1;
			break;
		}
		
		for (int i = loop_start; i < cnt_node_query; i++){

			if (core_number_query[i] < 2)	//root node must be selected from the core structure
				continue;

			label = nodes_label_query[i];
			degree = node_degree_query[i];

			//binary search used here
//			int s = label_deg_label_pos[ label - 1 ].second;
//			int end = label_deg_label_pos[ label ].second;
//			vector<int>::iterator pos = lower_bound( degree_array.begin() + s , degree_array.begin() + end, degree);
//			int start = pos - degree_array.begin();

			ranking = (double)(label_cardinality.get(label)) /(double)degree ;

			if (ranking < least_ranking){
				least_ranking = ranking;
				start_node = i;
			}
		}
		return start_node;
	}

	public void BFS_NORMAL()
	{
		resetTreeNodes();

		core_tree_node_child_array_index = 0;
		core_tree_node_nte_array_index = 0;

		exploreCRSequence_indx = 0;

		int[] visited = new int[cnt_node_query];

		int[] queue_array = new int[cnt_node_query];

		queue_array[0] = root_node_id;

		int pointer_this_start = 0;
		int pointer_this_end = 1;
		int pointer_next_end = 1;
		int current_level = 1; //initial level starts at 1

		simulation_sequence_index = 0;
		level_index.clear();
		visited[root_node_id] = 1;
		BFS_level_query[root_node_id] = 1;
		BFS_parent_query[root_node_id] = -1;
		initializeTreeNode(core_query_tree[root_node_id], -1);

		while (true) {

			int start = simulation_sequence_index;

			while (pointer_this_start != pointer_this_end) { // queue not empty

				int current_node = queue_array[pointer_this_start];
				pointer_this_start++;

				for (int i = 0; i < graph.get(current_node).size(); i ++){

					int childNode = graph.get(current_node).get(i);

					if (visited[childNode] != 0) { //this child node has been visited before,

						if (childNode != core_query_tree[current_node].parent_node)
							addNonTreeEdgeToTreeNode (core_query_tree[current_node], childNode);

						if (visited[childNode] > current_level)	//this is a cross level nte
							addCrossLevelNTEToTreeNode (core_query_tree[childNode], current_node); //record its cross level nte parent

					} else {//this child node has not been visited.

						visited[childNode] = current_level + 1; //parent node's level plus one

						queue_array[pointer_next_end] = childNode;
						pointer_next_end++;

						BFS_level_query[childNode] = current_level + 1;
						BFS_parent_query[childNode] = current_node;

						if (core_number_query[childNode] < 2)
							continue;

						initializeTreeNode(core_query_tree[childNode], current_node);
						addChildToTreeNode(core_query_tree[current_node], childNode);
					}
				}

//				simulation_sequence[simulation_sequence_index] = current_node;
				simulation_sequence.set(simulation_sequence_index, current_node);
				simulation_sequence_index ++;
			}

			int end = simulation_sequence_index;

			level_index.add(new Pair<Integer,Integer>(start, end));

			for (int i = end - 1; i >= start; i--){
				int node = simulation_sequence.get(i);
				if (core_number_query[node] < 2)
					continue;
				exploreCRSequence[exploreCRSequence_indx ++] = node;
			}

			if (pointer_next_end == pointer_this_end) //no node has been pushed in
				break;

			pointer_this_start = pointer_this_end;
			pointer_this_end = pointer_next_end;

			current_level++;

		}
	}
	
	public void resetTreeNodes()
	{
		for (int i = 0; i < cnt_node_query; i ++){
			Core_query_tree_node  c = core_query_tree[i];
			c.parent_node = -1;
			c.children = new Pair<Integer, Integer>(0,0);
			c.nte = new Pair<Integer, Integer>(0,0);
			c.cross_lvl_nte = null;
		}
	}
	
	public void initializeTreeNode(Core_query_tree_node treeNode, int parent_node){
		treeNode.parent_node = parent_node;
		treeNode.children = new Pair<Integer, Integer>(0, 0);
		treeNode.nte = new Pair<Integer, Integer>(0, 0);
		treeNode.cross_lvl_nte = null;
	}

	public void addNonTreeEdgeToTreeNode(Core_query_tree_node treeNode, int otherEnd) {
		core_tree_node_nte_array[ core_tree_node_nte_array_index ++] = otherEnd;
		if (treeNode.nte.getRight() == 0)
			treeNode.nte = new Pair<Integer, Integer> (core_tree_node_nte_array_index - 1, 1);
		else
			treeNode.nte.setRight(treeNode.nte.getRight()+1);
	}	

	public void addCrossLevelNTEToTreeNode(Core_query_tree_node  treeNode, int otherEnd) {
		if (treeNode.cross_lvl_nte == null)
			treeNode.cross_lvl_nte = new ArrayList<Integer>();
		treeNode.cross_lvl_nte.add(otherEnd);
	}

	public void addChildToTreeNode(Core_query_tree_node treeNode, int child) {
		core_tree_node_child_array[ core_tree_node_child_array_index ++] = child;
		if (treeNode.children.getRight() == 0)
			treeNode.children = new Pair<Integer, Integer>(core_tree_node_child_array_index - 1, 1);
		else
			treeNode.children.setRight(treeNode.children.getRight()+1);
	}

	public void exploreCR_FOR_CORE_STRUCTURE(){

		/*
		 * This function explores the candidate region and build the top down index from the root node ONLY for the core structure.
		 * It is able to handle core structure input and normal input.
		 * For core structure input, it uses the "simulation_sequence".
		 * For normal input, it uses the "exploreCR_sequence"
		 */

		//================= First step: deal with the ROOT node ===============================
		//visited flag array: introduced solely becasue of the same-level ntes validation
		boolean [] visited = new boolean[cnt_node_query];

		int seq_edge_this_level_index = 0;

		//get its preliminary candidates now
		{
			visited[root_node_id] = true;
			NodeIndexUnit root_node_unit = indexSet[root_node_id];
			int label = nodes_label_query[root_node_id];
			int degree = node_degree_query[root_node_id];

			int max_nb_degree;
			int core;
			if(CORE_AND_MAX_NB_FILTER)
			{
				max_nb_degree = 0;
				core = core_number_query[root_node_id];
			}
			//============== generate the neighborhood label array =======================
			NLF_array = new int[4 * NLF_size];
			for (int j = 0; j < graph.get(root_node_id).size(); j++) {
				int neighborhood = graph.get(root_node_id).get(j);
				int local_label = nodes_label_query[neighborhood];
				int idx = NLF_size - 1 - local_label / SIZEOF_INT;
				int pos = local_label % SIZEOF_INT;

				NLF_array[idx] |= (1 << pos);

				if(CORE_AND_MAX_NB_FILTER)
				{
					int nb_degree = node_degree_query [ neighborhood ];
					if ( nb_degree > max_nb_degree) //find the max neighbor degree
						max_nb_degree = nb_degree;
				}
			}

			//binary search here
			int s = label_deg_label_pos.get(label - 1)+1;
			int end = label_deg_label_pos.get(label);
			int pos = Utility.lower_bound( label_degree_nodes, s, end, degree);

			count_global_temp_array_1 = 0;

			for (int j = pos; j <= end; j++) {

				int can_id = label_degree_nodes.get(j).id;

				if(CORE_AND_MAX_NB_FILTER)
					if (core_number_data[can_id] < core || max_nb_degree > MAX_NB_degree_data[can_id])
						continue;


				boolean flag_add = true;
				for (int pos_local = NLF_size - 1; pos_local >= 0; pos_local--){
					if (NLF_check[can_id * NLF_size + pos_local] != ( NLF_array[pos_local] | NLF_check[can_id * NLF_size + pos_local] )){
						flag_add = false;
						break;
					}
				}

				if (flag_add)// This node id is valid preliminary candidate for the current query node!!!
					root_node_unit.candidates[count_global_temp_array_1 ++] = can_id;

			} // end for

			root_node_unit.size = count_global_temp_array_1;
//			fill(root_node_unit.path, root_node_unit.path + count_global_temp_array_1, 1);
			for ( int i = 0; i < count_global_temp_array_1; i++)
				root_node_unit.path[i] = 1;
		}

		int current_level = 0; //this is root node's level

		//================= then explore nodes in the simulation sequence =======================================
		for (int i = 1; i < simulation_sequence_index; i ++){

			int current_node = simulation_sequence.get(i);
			int BFS_parent = BFS_parent_query[current_node];
			visited[current_node] = true;
			int label_cur = nodes_label_query[current_node];
			int degree_cur = node_degree_query[current_node];
			int level_cur = BFS_level_query[current_node];

			//================== BACKWARD PRUNTING ===================================
			//backward pruning for same-level ntes with one level is finished
			if (level_cur != current_level){ //entry point: change of level detected

				current_level = level_cur;
				seq_edge_this_level_index --;
				while (seq_edge_this_level_index != -1){
					Pair<Integer, Integer> nte = seq_edge_this_level.get(seq_edge_this_level_index);
					int refinee = nte.getLeft();
					NodeIndexUnit refinee_node_unit = indexSet[refinee];
					int label = nodes_label_query[refinee];
					int check_value = 0;

					while (nte.getLeft() == refinee){

						int child = nte.getRight(); // the child is the refiner
						NodeIndexUnit child_node_unit = indexSet[child];
						int can_id;
						Pair<Integer, Integer> result;
						for (int x = 0; x < child_node_unit.size; x++) {
							can_id = child_node_unit.candidates[x];
							if (can_id == -1)
								continue;

							result = nodes_to_label_info[can_id * (cnt_unique_label + 1) + label];
							for (int y = result.getLeft(); y < result.getLeft() + result.getRight(); y++) {
								int temp_node = nodes_data[y];
								if (simulation_check_array [temp_node] == check_value){
									simulation_check_array [temp_node] ++;
									if (check_value == 0)
										array_to_clean[to_clean_index ++] = temp_node;
								}
							}
						}

						check_value++;

						if (seq_edge_this_level_index == 0){
							seq_edge_this_level_index --;
							break;
						}

						seq_edge_this_level_index --;
						nte = seq_edge_this_level.get(seq_edge_this_level_index);

					}//end for j

					if (check_value != 0){ // it has indeed been refined
						for (int j = 0; j <  refinee_node_unit.size; j++) {
							int can_id = refinee_node_unit.candidates[j];
							if (simulation_check_array[can_id] != check_value)
								refinee_node_unit.candidates[j] = -1;
						} // end for

						while(to_clean_index != 0)
							simulation_check_array [ array_to_clean[ --to_clean_index ] ] = 0;
					}
				}
				seq_edge_this_level_index = 0; //reset to 0;
			}
			//========================================================================

			int max_nb_degree = 0;
			int core_cur = 0;
			if(CORE_AND_MAX_NB_FILTER)
			{
				max_nb_degree = 0;
				core_cur = core_number_query[current_node];
			}
			//============== generate the neighborhood label array =======================
//			int first = query_nodes_array_info[current_node];
//			memset(NLF_array, 0, 4 * NLF_size);
			NLF_array = new int[4 * NLF_size];
			for (int neighbor : graph.get(current_node)) 
			{
				int local_label = nodes_label_query[neighbor];
				int idx = NLF_size - 1 - local_label / SIZEOF_INT;
				int pos = local_label % SIZEOF_INT;
				NLF_array[idx] |= (1 << pos);
				if(CORE_AND_MAX_NB_FILTER)
				{
					int nb_degree = node_degree_query [neighbor];
					if ( nb_degree > max_nb_degree) //find the max neighbor degree
						max_nb_degree = nb_degree;
				}
			}
			//=================================================================================


			NodeIndexUnit cur_node_unit = indexSet[current_node];
			NodeIndexUnit parent_unit = indexSet[BFS_parent];

			//make sure it wont "new" array every time
			if (cur_node_unit.parent_cand_size < parent_unit.size){
				cur_node_unit.size_of_index = new int [parent_unit.size];
//				memset(cur_node_unit.size_of_index, 0 , sizeof(int) * parent_unit.size);
				for(i = 0; i < parent_unit.size; i++)
					cur_node_unit.size_of_index[i] = 0;
				cur_node_unit.index = new int[parent_unit.size][];
				cur_node_unit.parent_cand_size = parent_unit.size;
			}

			int check_value = 0; //no candidates yet, so assign 0 to it

			//=========== Check cross-level nte and visited same-level nte =========================
			//============ FORWARD PRUNING FOR NTES ================================================
//			for (int j = query_nodes_array_info[current_node]; j < query_nodes_array_info[current_node + 1]; j ++){
			for(int child : graph.get(current_node)){
//				int child = query_nodes_array[j];
				//only keep children with lower level, or vistted ones in same level
				if (BFS_level_query[child] > level_cur)
					continue;

				if (!visited[child]){// here, the we made sure that child in lower level have been marked visited already
					seq_edge_this_level.add(new Pair<Integer, Integer>(current_node, child));
					seq_edge_this_level_index ++;
					continue;
				}

				//Then, we refine the cross-level ntes and visited same-level ntes together :: we dont differentiate them
				NodeIndexUnit nte_parent_node_unit = indexSet[child];

				for (int y = 0; y < nte_parent_node_unit.size; y ++) {
					int nte_parent_cand = nte_parent_node_unit.candidates[y];
					if (nte_parent_cand == -1)
						continue;
					Pair<Integer, Integer> query_result = nodes_to_label_info[nte_parent_cand * (cnt_unique_label + 1) + label_cur];
					//for each of the result retrieved by querying the edge index
					for (int i_local = query_result.getLeft(); i_local < query_result.getLeft() + query_result.getRight(); i++) {
						int can_id = nodes_data[i];
						if (flag_prelin_char[can_id] == check_value){
							flag_prelin_char[can_id] ++;
							if (check_value == 0)
								array_to_clean[ to_clean_index ++ ] = can_id;
						}
					}
				}
				check_value ++; //update the check value by one
			}
			//======================================================================================

			int child_index = 0;
			//for each cands of its BFS parent
			for (int parent_cand_index = 0; parent_cand_index < parent_unit.size; parent_cand_index++){
				int cand_parent = parent_unit.candidates[parent_cand_index];
				if (cand_parent == -1)
					continue;
				count_index_array_for_indexSet = 0;

				//query edge index
				Pair<Integer, Integer> res_edgeIndex = nodes_to_label_info[cand_parent * (cnt_unique_label + 1) + label_cur];

				for (int x = res_edgeIndex.getLeft(); x < res_edgeIndex.getLeft() + res_edgeIndex.getRight(); x++) {
					int can_id = nodes_data[x];
					//check the flag for cross-level nte and visited same-level nte
					if (flag_prelin_char[can_id] != check_value)
						continue;

					if (flag_child_cand[can_id] != -1){
						//push into the index first
						cur_node_unit.path[child_index] += parent_unit.path[parent_cand_index];
						index_array_for_indexSet[count_index_array_for_indexSet++] = flag_child_cand[can_id];
						continue;
					}

					//check degree, core, and max_neighbor degree together
					if (degree_data[can_id] < degree_cur || core_number_data[can_id] < core_cur || MAX_NB_degree_data[can_id] < max_nb_degree)
						continue;

					//check lightweight NLF
					int flag_add = 1;
					for (int pos = NLF_size - 1; pos >= 0; pos--){
						if (NLF_check[can_id * NLF_size + pos] != ( NLF_array[pos] | NLF_check[can_id * NLF_size + pos] )){
							flag_add = 0;
							break;
						}
					}

					// lightweight NLF OK
					if (flag_add == 1){
						cur_node_unit.candidates[child_index] = can_id;
						cur_node_unit.path[child_index] = parent_unit.path[parent_cand_index];
						flag_child_cand[can_id] = child_index;
						index_array_for_indexSet[count_index_array_for_indexSet ++] = child_index;
						child_index ++;
					}

				}//end for: edge index

				if (cur_node_unit.size_of_index[parent_cand_index] < count_index_array_for_indexSet)
					cur_node_unit.index[parent_cand_index] = new int [count_index_array_for_indexSet];
//				copy(index_array_for_indexSet, index_array_for_indexSet + count_index_array_for_indexSet, cur_node_unit.index[parent_cand_index]);
				for(int i_local = 0; i_local < count_index_array_for_indexSet; i++)
					cur_node_unit.index[parent_cand_index][i] = index_array_for_indexSet[i];
				cur_node_unit.size_of_index[parent_cand_index] = count_index_array_for_indexSet;
			}//end for: candidates of BFS parent

			cur_node_unit.size = child_index;

			for (int x = 0; x < child_index; x++)
				flag_child_cand[ cur_node_unit.candidates [x] ] = -1;

			while(to_clean_index != 0)
				flag_prelin_char [ array_to_clean[ --to_clean_index ] ] = 0;

		} //end for: simulation sequence

	}

}
