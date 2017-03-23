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
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.register.Register.Int;

import commons.OwnMethods;
import commons.Pair;
import commons.Query_Graph;
import commons.*;
public class CFLMatch {

	private static final boolean RESULT_ENUMERATION = true;
	private static final boolean OUTPUT_EXTRA_INFO = false;
	private static final boolean CORE_AND_MAX_NB_FILTER = true;
	private static final int SIZEOF_INT = 32;
	private static final int MAX_NTE_QUERY = 2000;
	private static final int LIMIT = 100;
	private static final boolean COUT_RESULT = true;

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
	public int[] mapping_found = new int[1];
	public boolean isTree;
	public int MAX_DEGREE_QUERY = 0;
	public int cnt_node_query = 0;
	public int[] core_number_query;
	public int[] node_degree_query;
	public int sum_degree_cur = 0;
	public int count_global_temp_array_1;

	public int residual_tree_match_seq_index;
	public int[] residual_tree_match_seq;

	public int residual_tree_leaf_node_index;
	public ArrayList<Pair<Integer, Double>> residual_tree_leaf_node;

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
	
	//getCORE_sequence_CORE
	public int[] sequence_flag_query;
	public int leaf_nodes_index;
	public int temp_array_query_index;
	public int[] sum_nontree_edge;
	public int[] stack_array_query;
	public int[] sum_path;
	public int[] leaf_nodes;
	public int[] temp_array_query;
	public SequenceUnit[] matching_unit;
	public int[] nte_array_for_matching_unit = new int[MAX_NTE_QUERY];
	public double[] path_acuu;
	public double[] path_temp;
	public Pair<Integer, Double>[] leaf_path_info;
	
	public int[] global_temp_array_1;
	
	public boolean[] mapping_flag_data;
	public int[] actual_mapping;
	public int[] self_pos;
	public int[] all_mapping;
	public SearchUnit[] su;
	public int SIZEOF;
	public int SIZE_OF_EDGE_MATRIX;
	public boolean[] data_edge_matrix;
	
	public ArrayList<Pair<Integer, Pair<Integer, Double>>> NEC_set_ranking;
	public int leaf_necs_idx;
	public int[] leaf_necs;
	public ArrayList<Pair<Integer, Integer>> v_nec_count;
	public int nec_count_set_size = -1;
	public int[] nec_count_set;
	public int[] actual_mapping_tree;
	
	public SearchUnit[] su_tree;
	
//	public Pair<Integer, Integer>[] Leaf_cands_info;

	public CFLMatch(Data_Graph data_Graph) {
		this.label_cardinality = data_Graph.label_cardinality;
		this.cnt_unique_label = label_cardinality.size();

		this.max_label_counter = 0;
		for( int cardinality : label_cardinality.values())
		{
			cnt_node += cardinality;
			if(cardinality > max_label_counter)
				max_label_counter = cardinality;
		}
		
		NLF_size = data_Graph.NLF_size;
		NLF_array = data_Graph.NLF_array;
		NLF_check = data_Graph.NLF_check;
		
		//these variables will be null without explicitly call compute function in Data_Graph
		this.label_deg_label_pos = data_Graph.label_deg_label_pos;
		this.label_degree_nodes = data_Graph.label_degree_nodes;
		this.core_number_data =  data_Graph.core_number_data;
		this.MAX_NB_degree_data = data_Graph.MAX_NB_degree_data;

		this.nodes_to_label_info = data_Graph.nodes_to_label_info;
		this.nodes_data = data_Graph.nodes_data;

		this.degree_data = data_Graph.degree_data;
		
		this.SIZEOF = data_Graph.SIZEOF;
		this.SIZE_OF_EDGE_MATRIX = data_Graph.SIZE_OF_EDGE_MATRIX;
		this.data_edge_matrix = data_Graph.data_edge_matrix;
	}

	public static void main(String[] args) {
		HashMap<Integer, Integer> transfer_table = null;
		Data_Graph data_Graph = null;
		try {
			String datagraph_path = "/home/yuhansun/Documents/GeoGraphMatchData/data/DataSet/hprd";
			String transfer_table_path = "/home/yuhansun/Documents/GeoGraphMatchData/data/transfertable_hprd.txt";
//			String query_graphs_path = "/home/yuhansun/Documents/GeoGraphMatchData/data/QuerySet/hprd50s";
			String query_graphs_path = "/home/yuhansun/Documents/GeoGraphMatchData/data/QuerySet/test_query_graph";
			//        	String query_graphs_path = "/home/yuhansun/Documents/GeoGraphMatchData/human10s";
			transfer_table = Utility.Read_Transfer_Table(transfer_table_path);
			if(transfer_table == null)
				throw new Exception("Read transfer_table failed!");

			data_Graph = new Data_Graph(datagraph_path, transfer_table);
			data_Graph.Calculate_All();

			int read_count = 1;
			ArrayList<Query_Graph> query_Graphs = Utility.ReadQueryGraphs(query_graphs_path, transfer_table, read_count);
			CFLMatch cflMatch = new CFLMatch(data_Graph);
			
//			int i = 77;
			for(int i = 0; i < read_count; i++)
			{
				OwnMethods.Print("query id :"+ i);
				cflMatch.SubgraphMatch(query_Graphs.get(i));
			}
				
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
		OwnMethods.Print(String.format("cnt_node_query:%d\nMAX_DEGREE_QUERY:%d\n", cnt_node_query, MAX_DEGREE_QUERY));

		coreDecompositition_query();

		mapping_found[0] = 0;
		NEC_mapping = new int[(cnt_unique_label + 1) * cnt_node_query];
		for( int i = 0; i < (cnt_unique_label + 1) * cnt_node_query; i++)
			NEC_mapping[i] = 0;
		NEC_mapping_pair = new ArrayList<NEC_element>((cnt_unique_label + 1) * cnt_node_query);
		NEC_mapping_Actual = new int[(cnt_unique_label + 1) * cnt_node_query];
		NEC_Node_array = new NEC_Node [cnt_node_query];
		for(int i = 0; i < cnt_node_query; i++)
			NEC_Node_array[i] = new NEC_Node();
		residual_tree_match_seq = new int[cnt_node_query];
		tree_node_parent = new int[cnt_node_query];
		residual_tree_leaf_node = new ArrayList<Pair<Integer,Double>>();
		NEC_set_by_label_index = new ArrayList<Pair<Integer,Integer>>();
		NEC_set_array = new NEC_set_array_element[(cnt_unique_label + 1) * cnt_node_query];
		core_query_tree =  new Core_query_tree_node[cnt_node_query];
		for(int i = 0; i < cnt_node_query; i++)
			core_query_tree[i] = new Core_query_tree_node();

		level_index = new ArrayList<Pair<Integer,Integer>>(); 
		BFS_level_query = new int[cnt_node_query];
		BFS_parent_query = new int[cnt_node_query];

		simulation_sequence = new ArrayList<Integer>(Collections.nCopies(cnt_node_query, 0));

		core_tree_node_nte_array = new int[sum_degree_cur];
		core_tree_node_child_array = new int[sum_degree_cur];

		exploreCRSequence = new int[cnt_node_query];//not useful

		indexSet = new NodeIndexUnit[cnt_node_query];
		for ( int i = 0; i < cnt_node_query; i++)
		{
			indexSet[i] = new NodeIndexUnit();
			indexSet[i].candidates = new int[max_label_counter];
			indexSet[i].path = new long [max_label_counter];
			indexSet[i].parent_cand_size = 0;
		}

		seq_edge_this_level = new ArrayList<Pair<Integer,Integer>>(10000);
		for(int i = 0; i < 10000; i++)
			seq_edge_this_level.add(new Pair<Integer, Integer>(0, 0));
		simulation_check_array =  new int[cnt_node];

		array_to_clean = new int[cnt_node * cnt_node_query];
		to_clean_index = 0;

		flag_prelin_char = new int[cnt_node];
		flag_child_cand = new int[cnt_node];
		for(int i = 0; i < cnt_node; i++)
			flag_child_cand[i] = -1;
		
		index_array_for_indexSet = new int[cnt_node];
		
		//
		sequence_flag_query = new int[cnt_node_query];
		leaf_nodes_index = 0;
		temp_array_query_index = 0;
		sum_nontree_edge = new int[cnt_node_query];
		stack_array_query = new int[cnt_node_query];
		sum_path = new int[cnt_node_query];
		leaf_nodes = new int[cnt_node_query];
		temp_array_query = new int[cnt_node_query];
		matching_unit = new SequenceUnit[cnt_node_query];
		for ( int i = 0; i < cnt_node_query; i++)
			matching_unit[i] = new SequenceUnit();
		path_acuu = new double[cnt_node];
		path_temp = new double[cnt_node];
		leaf_path_info = new Pair[cnt_node_query];
		for(int i = 0; i < cnt_node_query; i++)
			leaf_path_info[i] = new Pair<Integer, Double>(0, 0.0);
		
		global_temp_array_1 = new int[cnt_node];
		
		mapping_flag_data = new boolean[cnt_node];
		actual_mapping = new int[cnt_node_query];
		self_pos = new int[cnt_node_query];
		su = new SearchUnit[cnt_node_query];
		for(int i = 0; i < cnt_node_query; i++)
			su[i] = new SearchUnit();
		all_mapping = new int[cnt_node_query];
		
		NEC_set_ranking = new ArrayList<Pair<Integer,Pair<Integer,Double>>>(cnt_node_query);
		for(int i = 0; i < cnt_node_query; i++)
		{
			Pair<Integer, Pair<Integer, Double>> pair = new Pair<Integer, Pair<Integer,Double>>(0, new Pair<Integer, Double>(0, 0.0));
			NEC_set_ranking.add(pair);
		}
		leaf_necs_idx = 0;
		leaf_necs = new int[cnt_node_query];
		v_nec_count = new ArrayList<Pair<Integer,Integer>>(cnt_node_query);
		for(int i = 0; i < cnt_node_query; i++)
			v_nec_count.add(new Pair<Integer, Integer>(0, 0)); 
		nec_count_set = new int[cnt_node_query];
		actual_mapping_tree = new int[cnt_node_query];
		
		su_tree = new SearchUnit[cnt_node_query];
		for ( int i = 0; i < cnt_node_query; i++)
			su_tree[i] = new SearchUnit();
		
//		Leaf_cands_info = new Pair[(cnt_unique_label + 1) * cnt_node_query];
//		for(int i = 0; i < cnt_node_query; i++)
//			Leaf_cands_info[i] = new Pair<Integer, Integer>(0, 0);
	}



	public ArrayList<Int[]> SubgraphMatch(Query_Graph query_Graph)
	{
		this.graph = query_Graph.graph;
		this.nodes_label_query = query_Graph.label_list;
		Initialize_Query_Parameter();

		isTree = true;
		for (int i = 0; i < cnt_node_query; i ++)
			if (core_number_query[i] >= 2){
				isTree = false;
				break;
			}

		if(isTree)
		{
			root_node_id = start_node_selection_TREE();//choose the start node of the query
			extractResidualStructures_TREE();
			BFS_TREE();
			nte_array_for_matching_unit_index = 0;
			matching_sequence_index = 0;
			exploreCR_FOR_TREE_ONLY_ROOT();
			exploreCR_Residual_TREE();
			simulation_NORMAL();
			getTreeMatchingSequence_TREE();
			mapping_found = getTreeMapping_Enumeration();
		}
		else
		{
			Extract_Residual_Structures();
			root_node_id = start_node_selection_NORMAL();
			BFS_NORMAL();
			nte_array_for_matching_unit_index = 0;
			matching_sequence_index =0;
			exploreCR_FOR_CORE_STRUCTURE();
			exploreCR_Residual_NORMAL();
			simulation_NORMAL();
			getCORE_sequence_CORE();
			if (residual_tree_match_seq_index >= 2)
				getTreeMatchingSequence_NORMAL();
			
			mapping_found = getFullMapping_Enumeration();
		}
		return null;
	}

	/**
	 * initialize core_number_query
	 */
	private void coreDecompositition_query()
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
	
	void extractResidualStructures_TREE(){

		//only for trees.

		residual_tree_match_seq_index = 0;
		residual_tree_leaf_node_index = 0;
		NEC_mapping_pair_index = 0;

//		memset(NEC_map, -1, sizeof(int) * cnt_node_query );
//		char * visited = visited_for_query; //indicate a node is visited or not
//		memset(visited, 0, sizeof(char) * cnt_node_query);
		
		NEC_map = new int[cnt_node_query];
		boolean[] visited = new boolean[cnt_node_query];

		for(int i = 0; i<cnt_node_query; i++)
		{
			NEC_map[i] = -1;
			visited[i] = false;
		}

		{//for each node in the query
			int i = root_node_id;//now i is set to the root node

			//for all of node i's children
//			for (int j = query_nodes_array_info[i]; j < query_nodes_array_info[i + 1]; j ++){
//				int child = query_nodes_array[j];
			for (int j = 0; j < graph.get(i).size(); j ++){

				int child = graph.get(i).get(j);
				if (core_number_query[child] < 2){ // the child node is not in the 2-core (for a tree, no node is in 2-core)
					//two cases here, the NEC node or a residual tree

					if (node_degree_query[child] == 1){ //degree is one ==> NEC node
						//============ CASE ONE: ONE-DEGREE NODES => NEC nodes =====================
						int label = nodes_label_query[child];

						if (NEC_mapping[label * cnt_node_query + i] == 0) {
//							NEC_mapping_pair.get(NEC_mapping_pair_index ++) = NEC_element(label, i, child);// child is the representative node
							NEC_mapping_pair.add(new NEC_element(label, i, child));
							NEC_mapping_pair_index++;
							NEC_map [child] = child;//NEC map
							NEC_mapping_Actual[label * cnt_node_query + i] = child;
							if(RESULT_ENUMERATION)
							{
								NEC_Node_array[child].node = child;
								NEC_Node_array[child].next = -1;
							}
						} else {
							NEC_map [child] = NEC_mapping_Actual[label * cnt_node_query + i];//NEC map
							if(RESULT_ENUMERATION)
							{
								int rep =NEC_mapping_Actual[label * cnt_node_query + i];
								NEC_Node_array[child].node = child;
								NEC_Node_array[child].next = NEC_Node_array[ rep ].next;
								NEC_Node_array[ rep ].next = child;
							}
						}
						NEC_mapping[label * cnt_node_query + i]++; // the label with parent being i, nec_count ++
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
							int added_child = 0;//testing

//							for (int m = query_nodes_array_info[current_node]; m < query_nodes_array_info[current_node + 1]; m ++){
//								int child_node = query_nodes_array[m];
							for (int m = 0; m < graph.get(current_node).size(); m ++){

								int child_node = graph.get(current_node).get(m);
								if (!visited[child_node]) {
									visited[child_node] = true;
									//======== special treatment here: if a node is a leaf (degree being 1), then put it into nec node set
									if (node_degree_query[child_node] == 1){
										int label = nodes_label_query[child_node];
										if (NEC_mapping[label * cnt_node_query + current_node] == 0) {
//											NEC_mapping_pair[NEC_mapping_pair_index ++] = NEC_element(label, current_node, child_node);// child is the repesentive node
											NEC_mapping_pair.add(new NEC_element(label, current_node, child_node));
											NEC_mapping_pair_index++;
											NEC_map [child_node] = child_node;//NEC map
											NEC_mapping_Actual[label * cnt_node_query + current_node] = child_node;
											if(RESULT_ENUMERATION)
											{
												NEC_Node_array[child_node].node = child_node;
												NEC_Node_array[child_node].next = -1;
											}
										} else {
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
									//									residual_tree_leaf_node[residual_tree_leaf_node_index ++] = make_pair(current_node, 0);
								{
									residual_tree_leaf_node.add(new Pair<Integer, Double>(current_node, 0.0));
									residual_tree_leaf_node_index++;
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

				NEC_element nec_ele = NEC_mapping_pair.get(i);

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
		//=============== finish construct the NEC set by label ==========================================================================
		//=============================== END extract the remaining structure =======================
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
//		if(OUTPUT_EXTRA_INFO)
//		{
//			int sum_node = 0;
//			if (NEC_mapping_pair_index != 0){
//				for (int i = 0; i < NEC_set_by_label_index.size() - 1; i++) {
//					int label = NEC_set_by_label_index[i].first;
//					int start = NEC_set_by_label_index[i].second;
//					int end = NEC_set_by_label_index[i + 1].second;
//					for (int j = start; j < end; j++) {
//						int parent_id = NEC_set_array[j].parent_id;
//						int sum = NEC_set_array[j].sum;
//						sum_node += sum;
//						cout << "label :" << label << " => parent id " << parent_id << " \t sum => " << sum
//						<< "\t representative node is " << NEC_set_array[j].represent_node<< endl;
//					}
//				}
//			}
//			cout << "NEC classes contained: " << NEC_mapping_pair_index << " classes with " << sum_node << " nodes " << endl;
//			cout << "Query trees with sum node: " << residual_tree_match_seq_index
//			<< " and tree leaf index is " << residual_tree_leaf_node_index << endl;
//			cout << "Nodes in tree: ";
//			for (int i = 0; i < residual_tree_match_seq_index; i++){
//				cout << residual_tree_match_seq[i] << " ";
//			}
//			cout << endl;
//
//		}
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
								//								NEC_Node_array[child].nextAddress = null;
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
//									residual_tree_leaf_node[residual_tree_leaf_node_index ++] = new Pair<Integer, Double>(current_node, (double) 0);
									residual_tree_leaf_node.add(new Pair<Integer, Double>(current_node, 0.0));
									residual_tree_leaf_node_index++;
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
	
	public int start_node_selection_TREE(){

		double least_ranking = Double.MAX_VALUE;
		int start_node = -1;
		double ranking;
		int label;
		int degree;

		for (int i = 0; i < cnt_node_query; i++){

			label = nodes_label_query[i];
			degree = node_degree_query[i];

			//binary search
			int s = label_deg_label_pos.get(label - 1)+1;
			int end = label_deg_label_pos.get(label);
			int pos = Utility.lower_bound( label_degree_nodes, s, end, degree);
			ranking = (double)(end - pos) /(double)degree ;

			if (ranking < least_ranking){
				least_ranking = ranking;
				start_node = i;
			}
		}

		return start_node;
	}

	public int start_node_selection_NORMAL(){

		double least_ranking = Double.MAX_VALUE;
		int start_node = -1;
		double ranking;
		int label;
		int degree;

		int loop_start = 0;
		for (int i = 0; i < cnt_node_query; i++){

			if (core_number_query[i] < 2)	//root node must be selected from the core structure
				continue;

			label = nodes_label_query[i];
			degree = node_degree_query[i];

			//binary search used here
//			int s = label_deg_label_pos[ label - 1 ].second;
//			int end = label_deg_label_pos[ label ].second;
//			vector<int>::iterator pos = lower_bound( degree_array.begin() + s , degree_array.begin() + end, degree);
//			int start = pos - degree_array.begin();
			int s = label_deg_label_pos.get(label - 1)+1;
			int end = label_deg_label_pos.get(label);
			int pos = Utility.lower_bound( label_degree_nodes, s, end, degree);
			ranking = (double)(end - pos) /(double)degree ;

			if (ranking < least_ranking){
				least_ranking = ranking;
				start_node = i;
			}
		}
		return start_node;
	}
	
	public void BFS_TREE() {

		/*
		 * for the normal input, this function should only consider the core structure nodes for the future top-down indexing
		 * output : true_leaf_nodes, simulation_sequence_array, level_to_sequence which maps a level to a segment in the sequence
		 */

		resetTreeNodes();

		core_tree_node_child_array_index = 0;
		core_tree_node_nte_array_index = 0;
		exploreCRSequence_indx = 0;

		int [] visited = new int[cnt_node_query];
		int [] queue_array = new int[cnt_node_query];
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

//				int start = query_nodes_array_info[current_node];
//				int end = query_nodes_array_info[current_node + 1];
//
//				for (int i = start; i < end; i ++){
//
//					int childNode = query_nodes_array[i];
				for (int i = 0; i < graph.get(current_node).size(); i ++){

					int childNode = graph.get(current_node).get(i);

					if (visited[childNode] != 0) { //this child node has been visited before,
						if (childNode != core_query_tree[current_node].parent_node)
							addNonTreeEdgeToTreeNode (core_query_tree[current_node], childNode);
						if (visited[childNode] > current_level)	//this is a cross level nte
							addCrossLevelNTEToTreeNode (core_query_tree[childNode], current_node); //record its cross level nte parent
					} else {					//this child node has not been visited.
						visited[childNode] = current_level + 1; //parent node's level plus one
						queue_array[pointer_next_end ++] = childNode;
						BFS_level_query[childNode] = current_level + 1;
						BFS_parent_query[childNode] = current_node;

						if (core_number_query[childNode] < 2)
							continue;

						initializeTreeNode(core_query_tree[childNode], current_node);//set current_node as parent
						addChildToTreeNode(core_query_tree[current_node], childNode);//core_tree_node_child_array is set here
					}
				}

//				simulation_sequence[simulation_sequence_index ++] = current_node;
				simulation_sequence.set(simulation_sequence_index, current_node);
				simulation_sequence_index ++;
			}

			int end = simulation_sequence_index;
			level_index.add(new Pair<Integer, Integer>(start, end));//start and end labels each level in simulation_sequence

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

	private void resetTreeNodes()
	{
		for (int i = 0; i < cnt_node_query; i ++){
			Core_query_tree_node  c = core_query_tree[i];
			c.parent_node = -1;
			c.children = new Pair<Integer, Integer>(0,0);
			c.nte = new Pair<Integer, Integer>(0,0);
			c.cross_lvl_nte = null;
		}
	}

	private void initializeTreeNode(Core_query_tree_node treeNode, int parent_node){
		treeNode.parent_node = parent_node;
		treeNode.children = new Pair<Integer, Integer>(0, 0);
		treeNode.nte = new Pair<Integer, Integer>(0, 0);
		treeNode.cross_lvl_nte = null;
	}

	private void addNonTreeEdgeToTreeNode(Core_query_tree_node treeNode, int otherEnd) {
		core_tree_node_nte_array[ core_tree_node_nte_array_index ++] = otherEnd;
		if (treeNode.nte.getRight().equals(0))
			treeNode.nte = new Pair<Integer, Integer> (core_tree_node_nte_array_index - 1, 1);
		else
			treeNode.nte.setRight(treeNode.nte.getRight()+1);
	}	

	private void addCrossLevelNTEToTreeNode(Core_query_tree_node  treeNode, int otherEnd) {
		if (treeNode.cross_lvl_nte == null)
			treeNode.cross_lvl_nte = new ArrayList<Integer>();
		treeNode.cross_lvl_nte.add(otherEnd);
	}

	private void addChildToTreeNode(Core_query_tree_node treeNode, int child) {
		core_tree_node_child_array[ core_tree_node_child_array_index ++] = child;
		if (treeNode.children.getRight().equals(0))
			treeNode.children = new Pair<Integer, Integer>(core_tree_node_child_array_index - 1, 1);
		else
			treeNode.children.setRight(treeNode.children.getRight()+1);
	}
	
	public void exploreCR_FOR_TREE_ONLY_ROOT(){

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
//			int first = query_nodes_array_info[root_node_id];
//			memset(NLF_array, 0, sizeof(int) * NLF_size);
//			for (int j = first; j < first + degree; j++) {
//				int local_label = nodes_label_query[  query_nodes_array[j] ];
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
//			int s = label_deg_label_pos[ label - 1 ].second;
//			int end = label_deg_label_pos[ label ].second;
//			vector<int>::iterator pos = lower_bound( degree_array.begin() + s , degree_array.begin() + end, degree);
//			int start = pos - degree_array.begin();
			int s = label_deg_label_pos.get(label - 1)+1;
			int end = label_deg_label_pos.get(label);
			int pos = Utility.lower_bound( label_degree_nodes, s, end, degree);

			count_global_temp_array_1 = 0;

//			for (int j = start; j < end; j++) {
//
//				int can_id = label_degree_to_node[j];
			for (int j = pos; j <= end; j++) {

				int can_id = label_degree_nodes.get(j).id;
				if(CORE_AND_MAX_NB_FILTER)
				{
					if (core_number_data[can_id] < core || max_nb_degree > MAX_NB_degree_data[can_id])
						continue;
				}

				char flag_add = 1;
				for (int pos_local = NLF_size - 1; pos_local >= 0; pos_local--){
					if (NLF_check[can_id * NLF_size + pos_local] != ( NLF_array[pos_local] | NLF_check[can_id * NLF_size + pos_local] )){
						flag_add = 0;
						break;
					}
				}

				if (flag_add != 0) 				// This node id is valid preliminary candidate for the current query node!!!
					root_node_unit.candidates[count_global_temp_array_1 ++] = can_id;//it is one element in indexSet

			} // end for

			root_node_unit.size = count_global_temp_array_1;
//			fill(root_node_unit.path, root_node_unit.path + count_global_temp_array_1, 1);
			for ( int i = 0; i < count_global_temp_array_1; i++)
				root_node_unit.path[i] = 1;
		}
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

					while (nte.getLeft().equals(refinee)){

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
//							if(can_id == 767)
//								OwnMethods.Print(can_id);//debug
//							else
//								OwnMethods.Print(can_id);
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
				for(int i_local = 0; i_local < parent_unit.size; i_local++)
					cur_node_unit.size_of_index[i_local] = 0;
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
					seq_edge_this_level.set(seq_edge_this_level_index, new Pair<Integer, Integer>(current_node, child));
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
					for (int i_local = query_result.getLeft(); i_local < query_result.getLeft() + query_result.getRight(); i_local++) {
						int can_id = nodes_data[i_local];
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
				for(int i_local = 0; i_local < count_index_array_for_indexSet; i_local++)
					cur_node_unit.index[parent_cand_index][i_local] = index_array_for_indexSet[i_local];
				cur_node_unit.size_of_index[parent_cand_index] = count_index_array_for_indexSet;
			}//end for: candidates of BFS parent

			cur_node_unit.size = child_index;

			for (int x = 0; x < child_index; x++)
				flag_child_cand[ cur_node_unit.candidates [x] ] = -1;

			while(to_clean_index != 0)
				flag_prelin_char [ array_to_clean[ --to_clean_index ] ] = 0;

		} //end for: simulation sequence

	}
	
	public void exploreCR_Residual_TREE(){

		int [] flag_prelin = flag_prelin_char;
//		memset(flag_prelin, 0, sizeof(char) * cnt_node);
//		memset(flag_child_cand, -1, sizeof(int) * cnt_node);
		for(int i = 0; i < cnt_node; i++)
		{
			flag_prelin[i] = 0;
			flag_child_cand[i] = -1;
		}

		// Second case : the tree structures
		for (int i = 0; i < residual_tree_match_seq_index; i++){//query node interator

			int current_node = residual_tree_match_seq[i];
			int BFS_parent = tree_node_parent[current_node];
			int label_cur = nodes_label_query[current_node];
			int degree_cur = node_degree_query[current_node];
			
			int max_nb_degree = 0;
			int core_cur = 0;
			
			if(CORE_AND_MAX_NB_FILTER)
			{
				max_nb_degree = 0;
				core_cur = core_number_query[current_node];//it is not used
			}
			//============== generate the neighborhood label array =======================
//			int first = query_nodes_array_info[current_node];
//			memset(NLF_array, 0, sizeof(int) * NLF_size);
			NLF_array = new int[4 * NLF_size];
//			for (int j = first; j < first + degree_cur; j++) {
			for (int neighbor : graph.get(current_node)){
				//				int local_label = nodes_label_query[  query_nodes_array[j] ];
				int local_label = nodes_label_query[neighbor];
				int idx = NLF_size - 1 - local_label / SIZEOF_INT;
				int pos = local_label % SIZEOF_INT;
				NLF_array[idx] |= (1 << pos);
				if(CORE_AND_MAX_NB_FILTER)
				{
					//				int nb_degree = node_degree_query [ query_nodes_array[j] ];
					//				if ( nb_degree > max_nb_degree) //find the max neighbor degree
					//					max_nb_degree = nb_degree;
					int nb_degree = node_degree_query [ neighbor ];
					if ( nb_degree > max_nb_degree) //find the max neighbor degree
						max_nb_degree = nb_degree;
				}
			}

			NodeIndexUnit cur_node_unit = indexSet[current_node];
			NodeIndexUnit parent_unit = indexSet[BFS_parent];


			if (cur_node_unit.parent_cand_size < parent_unit.size){//make sure it wont "new" array every time
				cur_node_unit.size_of_index = new int [parent_unit.size];
//				memset(cur_node_unit.size_of_index, 0 , sizeof(int) * parent_unit.size);
				for ( int i_local = 0; i_local < parent_unit.size; i_local++)
					cur_node_unit.size_of_index[i_local] = 0;
				cur_node_unit.index = new int [parent_unit.size][];
				cur_node_unit.parent_cand_size = parent_unit.size;
			}

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
					if (flag_child_cand[can_id] != -1){
						//push into the index first
						cur_node_unit.path[child_index] += parent_unit.path[parent_cand_index];
						index_array_for_indexSet[count_index_array_for_indexSet ++] = flag_child_cand[can_id];
						continue;
					}
					//check degree, core, and max_neighbor degree together
					if (degree_data[can_id] < degree_cur || core_number_data[can_id] < core_cur || MAX_NB_degree_data[can_id] < max_nb_degree)
						continue;

					//check lightweight NLF
					char flag_add = 1;
					for (int pos = NLF_size - 1; pos >= 0; pos--){
						if (NLF_check[can_id * NLF_size + pos] != ( NLF_array[pos] | NLF_check[can_id * NLF_size + pos] )){
							flag_add = 0;
							break;
						}
					}

					// lightweight NLF OK
					if (flag_add != 0){
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
				for(int i_local = 0; i_local < count_index_array_for_indexSet; i_local++)
					cur_node_unit.index[parent_cand_index][i_local] = index_array_for_indexSet[i_local];
				cur_node_unit.size_of_index[parent_cand_index] = count_index_array_for_indexSet;

			}//end for: candidates of BFS parent

			cur_node_unit.size = child_index;
			for (int x = 0; x < child_index; x++)
				flag_child_cand[ cur_node_unit.candidates [x] ] = -1;
		}

		// First case : the NEC sets
		if (NEC_mapping_pair_index != 0) {

			for (int i = 0; i < NEC_set_by_label_index.size() - 1; i++) { // access the nec set by label
				int label = NEC_set_by_label_index.get(i).getLeft();
				int start = NEC_set_by_label_index.get(i).getRight();
				int end = NEC_set_by_label_index.get(i + 1).getRight();
				for (int j = start; j < end; j++) { // for each nec node with this label
					int parent_id = NEC_set_array[j].parent_id; //the parent node => "articulation node"
					int represent_node = NEC_set_array[j].represent_node;
					int degree_cur = node_degree_query[represent_node];
					int max_nb_degree = 0;
					int core_cur = 0;
					if(CORE_AND_MAX_NB_FILTER)
					{
						max_nb_degree = 0;
						core_cur = core_number_query[represent_node];
					}
					//============== generate the neighborhood label array =======================
//					int first = query_nodes_array_info[represent_node];
//					memset(NLF_array, 0, sizeof(int) * NLF_size);
					NLF_array = new int[NLF_size];
//					for (int j = first; j < first + degree_cur; j++) {
					for (int neighbor : graph.get(represent_node)){
						int local_label = nodes_label_query[  neighbor ];
						int idx = NLF_size - 1 - local_label / SIZEOF_INT;
						int pos = local_label % SIZEOF_INT;
						NLF_array[idx] |= (1 << pos);
						if(CORE_AND_MAX_NB_FILTER)
						{
							int nb_degree = node_degree_query [ neighbor ];
							if ( nb_degree > max_nb_degree) //find the max neighbor degree
								max_nb_degree = nb_degree;
						}
					}
					//=================================================================================

					NodeIndexUnit cur_node_unit = indexSet[represent_node];
					NodeIndexUnit parent_unit = indexSet[parent_id];

					if (cur_node_unit.parent_cand_size < parent_unit.size){
						cur_node_unit.size_of_index = new int [parent_unit.size];
//						memset(cur_node_unit.size_of_index, 0 , sizeof(int) * parent_unit.size);
						
						for(int i_local = 0; i_local < parent_unit.size; i_local++)
							cur_node_unit.size_of_index[i_local] = 0;
						
						cur_node_unit.index = new int[parent_unit.size][];
						cur_node_unit.parent_cand_size = parent_unit.size;
					}

					int child_index = 0;
					//for each cands of its BFS parent
					for (int parent_cand_index = 0; parent_cand_index < parent_unit.size; parent_cand_index++){
						int cand_parent = parent_unit.candidates[parent_cand_index];
						if (cand_parent == -1)
							continue;
						count_index_array_for_indexSet = 0;
						//query edge index
						Pair<Integer, Integer> res_edgeIndex = nodes_to_label_info[cand_parent * (cnt_unique_label + 1) + label];
						for (int x = res_edgeIndex.getLeft(); x < res_edgeIndex.getLeft() + res_edgeIndex.getRight(); x++) {
							int can_id = nodes_data[x];
							if (flag_child_cand[can_id] != -1){
								//push into the index first
								cur_node_unit.path[child_index] += parent_unit.path[parent_cand_index];
								index_array_for_indexSet[count_index_array_for_indexSet ++] = flag_child_cand[can_id];
								continue;
							}
							//check degree, core, and max_neighbor degree together
							if (degree_data[can_id] < degree_cur || core_number_data[can_id] < core_cur || MAX_NB_degree_data[can_id] < max_nb_degree)
								continue;

							//check lightweight NLF
							char flag_add = 1;
							for (int pos = NLF_size - 1; pos >= 0; pos--){
								if (NLF_check[can_id * NLF_size + pos] != ( NLF_array[pos] | NLF_check[can_id * NLF_size + pos] )){
									flag_add = 0;
									break;
								}
							}

							// lightweight NLF OK
							if (flag_add != 0){
								cur_node_unit.candidates[child_index] = can_id;
								cur_node_unit.path[child_index] = parent_unit.path[parent_cand_index];
								flag_child_cand[can_id] = child_index;
								index_array_for_indexSet[count_index_array_for_indexSet ++] = child_index;
								child_index ++;
							}

						}//end for: edge index

						if (cur_node_unit.size_of_index[parent_cand_index] < count_index_array_for_indexSet)
							cur_node_unit.index[parent_cand_index] = new int [count_index_array_for_indexSet];
//						copy(index_array_for_indexSet, index_array_for_indexSet + count_index_array_for_indexSet, cur_node_unit.index[parent_cand_index]);
						for (int i_local = 0; i_local < count_index_array_for_indexSet; i_local++)
							cur_node_unit.index[parent_cand_index][i_local] = index_array_for_indexSet[i_local];
						cur_node_unit.size_of_index[parent_cand_index] = count_index_array_for_indexSet;

					}//end for: candidates of BFS parent
					cur_node_unit.size = child_index;

					for (int x = 0; x < child_index; x++)
						flag_child_cand[ cur_node_unit.candidates [x] ] = -1;
				}//end for each nec node
			}//end for each nec label
		}
	}
	

	public void exploreCR_Residual_NORMAL()
	{
		int [] flag_prelin = flag_prelin_char;

		//		memset(flag_prelin, 0, sizeof(char) * cnt_node);
		//		memset(flag_child_cand, -1, sizeof(int) * cnt_node);
		for(int i = 0; i < cnt_node; i++)
		{
			flag_prelin[i] = 0;
			flag_child_cand[i] = -1;
		}

		// Second case : the tree structures

		for (int i = 0; i < residual_tree_match_seq_index; i++){

			int current_node = residual_tree_match_seq[i];
			int BFS_parent = tree_node_parent[current_node];

			int label_cur = nodes_label_query[current_node];
			int degree_cur = node_degree_query[current_node];

			int max_nb_degree = 0;
			int core_cur = 0;
			if(CORE_AND_MAX_NB_FILTER)
			{
				max_nb_degree = 0;
				core_cur = core_number_query[current_node];
			}
			//============== generate the neighborhood label array =======================
			//			int first = query_nodes_array_info[current_node];
			//			memset(NLF_array, 0, sizeof(int) * NLF_size);
			NLF_array = new int[NLF_size];
			//			for (int j = first; j < first + degree_cur; j++) {
			for (int neighbor : graph.get(current_node)){
				int local_label = nodes_label_query[neighbor];
				int idx = NLF_size - 1 - local_label / SIZEOF_INT;
				int pos = local_label % SIZEOF_INT;

				NLF_array[idx] |= (1 << pos);

				if(CORE_AND_MAX_NB_FILTER)
				{
					int nb_degree = node_degree_query [ neighbor ];
					if ( nb_degree > max_nb_degree) //find the max neighbor degree
						max_nb_degree = nb_degree;
				}
			}

			NodeIndexUnit cur_node_unit = indexSet[current_node];
			NodeIndexUnit parent_unit = indexSet[BFS_parent];

			//make sure it wont "new" array every time
			if (cur_node_unit.parent_cand_size < parent_unit.size) {
				cur_node_unit.size_of_index = new int [parent_unit.size];
				//				memset(cur_node_unit.size_of_index, 0 , sizeof(int) * parent_unit.size);
				for ( int i_local = 0; i_local < parent_unit.size; i_local++)
					cur_node_unit.size_of_index[i_local] = 0;
				cur_node_unit.index = new int [parent_unit.size][];
				cur_node_unit.parent_cand_size = parent_unit.size;
			}

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
					if (flag_child_cand[can_id] != -1){
						//push into the index first
						cur_node_unit.path[child_index] += parent_unit.path[parent_cand_index];
						index_array_for_indexSet[count_index_array_for_indexSet] = flag_child_cand[can_id];
						count_index_array_for_indexSet ++;
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

				if(cur_node_unit.size_of_index[parent_cand_index] < count_index_array_for_indexSet)
					cur_node_unit.index[parent_cand_index] = new int [count_index_array_for_indexSet];
				//				copy(index_array_for_indexSet, index_array_for_indexSet + count_index_array_for_indexSet, cur_node_unit.index[parent_cand_index]);
				for(int i_local = 0; i_local < count_index_array_for_indexSet; i_local++)
					cur_node_unit.index[parent_cand_index][i_local] = index_array_for_indexSet[i_local];
				cur_node_unit.size_of_index[parent_cand_index] = count_index_array_for_indexSet;
			}//end for: candidates of BFS parent

			cur_node_unit.size = child_index;

			for (int x = 0; x < child_index; x++)
				flag_child_cand[ cur_node_unit.candidates [x] ] = -1;
		}

		// First case : the NEC sets
		if (NEC_mapping_pair_index != 0) {

			for (int i = 0; i < NEC_set_by_label_index.size() - 1; i++) { // access the nec set by label
				int label = NEC_set_by_label_index.get(i).getLeft();
				int start = NEC_set_by_label_index.get(i).getRight();
				int end = NEC_set_by_label_index.get(i + 1).getRight();

				for (int j = start; j < end; j++) { // for each nec node with this label

					int parent_id = NEC_set_array[j].parent_id; //the parent node => "articulation node"
					int represent_node = NEC_set_array[j].represent_node;
					int degree_cur = node_degree_query[represent_node];

					int max_nb_degree = 0;
					int core_cur = 0;
					if(CORE_AND_MAX_NB_FILTER)
					{
						max_nb_degree = 0;
						core_cur = core_number_query[represent_node];
					}
					//============== generate the neighborhood label array =======================
//					int first = query_nodes_array_info[represent_node];
//					memset(NLF_array, 0, sizeof(int) * NLF_size);
					NLF_array = new int[NLF_size];
//					for (int j = first; j < first + degree_cur; j++) {
					for (int neighbor : graph.get(represent_node)){

						int local_label = nodes_label_query[  neighbor ];
						int idx = NLF_size - 1 - local_label / SIZEOF_INT;
						int pos = local_label % SIZEOF_INT;

						NLF_array[idx] |= (1 << pos);

						if(CORE_AND_MAX_NB_FILTER)
						{
							int nb_degree = node_degree_query [ neighbor ];
							if ( nb_degree > max_nb_degree) //find the max neighbor degree
								max_nb_degree = nb_degree;
						}
					}
					//=================================================================================

					NodeIndexUnit cur_node_unit = indexSet[represent_node];
					NodeIndexUnit parent_unit = indexSet[parent_id];

					if (cur_node_unit.parent_cand_size < parent_unit.size) {
						cur_node_unit.size_of_index = new int [parent_unit.size];
//						memset(cur_node_unit.size_of_index, 0 , sizeof(int) * parent_unit.size);
						cur_node_unit.size_of_index = new int[parent_unit.size];
						cur_node_unit.index = new int [parent_unit.size][];
						cur_node_unit.parent_cand_size = parent_unit.size;
					}

					int child_index = 0;
					//for each cands of its BFS parent
					for (int parent_cand_index = 0; parent_cand_index < parent_unit.size; parent_cand_index++){

						int cand_parent = parent_unit.candidates[parent_cand_index];

						if (cand_parent == -1)
							continue;

						count_index_array_for_indexSet = 0;

						//query edge index
						Pair<Integer, Integer> res_edgeIndex = nodes_to_label_info[cand_parent * (cnt_unique_label + 1) + label];
						for (int x = res_edgeIndex.getLeft(); x < res_edgeIndex.getLeft() + res_edgeIndex.getRight(); x++) {
							int can_id = nodes_data[x];
							if (flag_child_cand[can_id] != -1){
								//push into the index first
								cur_node_unit.path[child_index] += parent_unit.path[parent_cand_index];
								index_array_for_indexSet[count_index_array_for_indexSet ++] = flag_child_cand[can_id];
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
								index_array_for_indexSet[count_index_array_for_indexSet] = child_index;
								count_index_array_for_indexSet ++;
								child_index ++;
							}
						}//end for: edge index

						if (cur_node_unit.size_of_index[parent_cand_index] < count_index_array_for_indexSet)
							cur_node_unit.index[parent_cand_index] = new int [count_index_array_for_indexSet];

//						copy(index_array_for_indexSet, index_array_for_indexSet + count_index_array_for_indexSet, cur_node_unit.index[parent_cand_index]);
						for (int i_local = 0; i_local < count_index_array_for_indexSet; i_local++)
							cur_node_unit.index[parent_cand_index][i_local] = index_array_for_indexSet[i_local];
						cur_node_unit.size_of_index[parent_cand_index] = count_index_array_for_indexSet;

					}//end for: candidates of BFS parent
					cur_node_unit.size = child_index;

					for (int x = 0; x < child_index; x++)
						flag_child_cand[ cur_node_unit.candidates [x] ] = -1;

				}//end for each nec node
			}//end for each nec label
		}
	}
	
	public void simulation_NORMAL()
	{
		/*
		 * This simulation is a strictly bottom-up process.
		 */

		//visited flag array for the non-BFS children refining
		boolean[] visited = new boolean[cnt_node_query];
		for (int i = 0; i < cnt_node_query; i++)
			visited[i] = false;
		
		Pair<Integer, Integer> pos;
		int start;
		int end;

		//now we deal with nodes level by level
		for (int level = level_index.size() - 1; level >= 0; level --){// level by level

			int current_level = level + 1;

			pos = level_index.get(level);
			start = pos.getLeft();
			end = pos.getRight();

//			stable_sort (simulation_sequence + start, simulation_sequence + end, sortByDegree_Query_dec);//high-degree node first match
			Collections.sort(simulation_sequence.subList(start, end), new Comparator<Integer>() {

				public int compare(Integer o1, Integer o2) {
					return Utility.Comparator(graph.get(o2).size(), graph.get(o1).size());
				}

			});
			

			//for each node in this level
			for (int seq_index = start; seq_index < end; seq_index ++){ //default sequence now

				int cur_node = simulation_sequence.get(seq_index);

				//if this node is a nec node, then it doesnt need to be refined by simulation.
				if (NEC_map[cur_node] != -1 )
					continue;

				NodeIndexUnit cur_node_unit = indexSet[cur_node];

				int label_cur = nodes_label_query[cur_node];

				//==== FIRST STEP: deal with its BFS children =====================
				Core_query_tree_node tree_node = core_query_tree[cur_node];
				for (int j = tree_node.children.getLeft(); j < tree_node.children.getLeft() + tree_node.children.getRight(); j++){

					int child = core_tree_node_child_array[j];
					visited[child] = true;

					if (NEC_map[child] != -1 && NEC_map[child] != child)
						continue;

					NodeIndexUnit child_node_unit = indexSet[child];

					for (int cand_index = 0; cand_index < cur_node_unit.size; cand_index ++){

						if (cur_node_unit.candidates[cand_index] == -1)
							continue;

						//check the positions one by one
						int[] temp_index = child_node_unit.index[cand_index];
						int temp_size_of_index = child_node_unit.size_of_index[cand_index];

						for (int cand_child_index = 0; cand_child_index < temp_size_of_index; cand_child_index ++){

							int node_pos = temp_index[cand_child_index];
							if (child_node_unit.candidates[node_pos] == -1){
								temp_index[cand_child_index] = temp_index[  temp_size_of_index - 1  ];
								temp_size_of_index --;
								//added because temp_size_of_index is reference & in original
								child_node_unit.size_of_index[cand_index] = child_node_unit.size_of_index[cand_index]-1;
								cand_child_index --;
							}
						}//end for : finished checking the positions

						if (temp_size_of_index == 0)
							cur_node_unit.candidates[cand_index] = -1;

					}
				}
				//=================================================================

				//====== SECOND STEP : deal with non-BFS children with larger level =====================
				int check_value = 0;
				for (int child : graph.get(cur_node)){
					if (BFS_level_query[child] <= current_level || visited[child]) // in a larger level and remain unvisited
						continue;
					//children here must have a larger level than the current level, so it would be a child with a cross-level nte
					if (NEC_map[child] != -1 && NEC_map[child] != child)//a node map to a NEC but not itself
										continue;

					int can_id;
					Pair<Integer, Integer> result;
					NodeIndexUnit child_node_unit = indexSet[child];
					for (int x = 0; x < child_node_unit.size; x++) {
						can_id = child_node_unit.candidates[x];
						if (can_id == -1)
							continue;

						result = nodes_to_label_info[can_id * (cnt_unique_label + 1) + label_cur];

						for (int y = result.getLeft(); y < result.getLeft() + result.getRight(); y++) {
							int temp_node = nodes_data[y];
							if (simulation_check_array[temp_node] == check_value){
								simulation_check_array[temp_node] ++;
								if (check_value == 0)
									array_to_clean[to_clean_index ++] = temp_node;
							}
						}
					}
					check_value ++;
				}

				for (int cand_index = 0; cand_index < cur_node_unit.size; cand_index ++){
					int cand_id = cur_node_unit.candidates[cand_index];
					if (cand_id == -1)
						continue;
					if (simulation_check_array[cand_id] != check_value)
					{
						cand_id = -1;
						cur_node_unit.candidates[cand_index] = -1;//added because cand_id is & reference
					}
				}

				while(to_clean_index != 0)
					simulation_check_array [ array_to_clean[ --to_clean_index ] ] = 0;

				for (int j = tree_node.children.getLeft(); j < tree_node.children.getLeft() + tree_node.children.getRight(); j++)
					visited[ core_tree_node_child_array[j] ] = false;
				//=======================================================================================
			}
		}
	}

	public void getCORE_sequence_CORE()
	{
//		memset(sequence_flag_query, 0, sizeof(int) * cnt_node_query);
//		sequence_flag_query[0] = 0;
		leaf_nodes_index =0;
		temp_array_query_index =0;
		int root = root_node_id;
		double first_path_ranking = Double.MAX_VALUE;
		int first_path_leaf = 0; // the leaf node of the first path

		//include the root node first => this always has nothing to do with the matching option
		addIntoSequence_for_whole_graph(root, true, sequence_flag_query); // need to change this function if used for bcc-ordered matching
		sum_nontree_edge[root] = 0; // must initialize this

		//To find all the leaf nodes and get the sum_nontree edges, we perform a top-down traverse of the query tree here
		int stack_array_index = 0;
		stack_array_query[stack_array_index++] = root;
		int first_leaf_index = 0;

		while (stack_array_index != 0){

			int current_node = stack_array_query[stack_array_index - 1];
			double current_nte_number = (double)sum_nontree_edge[current_node];
			stack_array_index --;
			Core_query_tree_node tree_node = core_query_tree[current_node];

			if (tree_node.children.getRight().equals(0)){ //This is a leaf node

				//compute the sum of the path for the leaf node
				int path_sum = 0;
				for (int x = 0; x < indexSet[current_node].size; x++)
					path_sum += indexSet[current_node].path[x];

				sum_path[current_node] = path_sum;

				//====== compute the sum of non tree edge in this path, stop when reach current bcc's root id
				//====== note that, each leaf node's path is all the way to the core structure's root id
				//       so , must break when reach current bcc's root id
				//===== now compute the ranking value

				current_nte_number += 1; // this is the extra parameter...
				double ranking = (double) path_sum;

				//get the first path to select
				if (ranking < first_path_ranking){
					first_path_ranking = ranking;
					first_path_leaf = current_node;
					first_leaf_index = leaf_nodes_index;
				}
				leaf_nodes[leaf_nodes_index ++] = current_node;

			} else { //This is not a leaf node.
				//put the children nodes into the stack
				for (int i = tree_node.children.getLeft(); i < tree_node.children.getLeft() + tree_node.children.getRight(); i++){
					int child = core_tree_node_child_array[i];
					sum_nontree_edge[child] = (int) (current_nte_number + core_query_tree[child].nte.getRight());
					stack_array_query[stack_array_index ++] = child;
				}
			}
		}

		//now we start to construct the heap to select the pathes
		{ //deal with the first path

//			cerr << "first path leaf is " << first_path_leaf << endl;

			int leaf_id = first_path_leaf;
			temp_array_query_index = 0; //always put to zero before using it
			while ( sequence_flag_query[leaf_id] == 0 ){ // here, actually stop at the bcc root, which is not to be processed here
				temp_array_query[temp_array_query_index ++] = leaf_id;
				leaf_id = core_query_tree[leaf_id].parent_node;//this node's parent
			}

			while (temp_array_query_index != 0){// add the sequence in the temp array into the sequence array
				temp_array_query_index --;
				addIntoSequence_for_whole_graph(temp_array_query[temp_array_query_index], false, sequence_flag_query);	//second
			}

			leaf_nodes[first_leaf_index] = leaf_nodes[leaf_nodes_index - 1];//remove the first leaf
			leaf_nodes_index--;

		} //end dealing with the first path

//		displaySequence();

		if (leaf_nodes_index == 0){//check if there are still leaf nodes leaft
//			displaySequence();
			return;
		}

//		cerr << "remaining leaf node index is " << leaf_nodes_index << endl;

		//bottom-up for each remaining path and record each leaf path's connection node
		{
			int min_leaf_index = 0;
			double min_ranking = Double.MAX_VALUE;

			for (int i = 0; i < leaf_nodes_index; i ++){

				int leaf = leaf_nodes[i];
				double cand_con;
				double path_sum = 0;

				//find its connection node
				int node = core_query_tree[leaf].parent_node;
				int before_con = leaf; //set the node before connection node
				int parent_cand_size = indexSet[ BFS_parent_query[leaf] ].size;

				for (int i_local = 0; i_local < parent_cand_size; i_local++)
					path_acuu[i_local] = indexSet[leaf].size_of_index[i_local];

				while ( sequence_flag_query[node] == 0 ){ // here, stop when reaching the connection node
					int cand_size = indexSet[BFS_parent_query[node]].size;
					for (int x = 0; x < cand_size; x++){
						path_temp[x] = 0;
						for (int y = 0; y < indexSet[node].size_of_index[x]; y++){
							int pos = indexSet[node].index[x][y];
							path_temp[x] += path_acuu[pos];
						}
					}

					for (int x = 0; x < cand_size; x++)
						path_acuu[x] = path_temp[x];

					before_con = node;//set the node as the last node before the connection node
					node = core_query_tree[node].parent_node;//this node's parent
				}

				cand_con = indexSet[node].size;
				for (int i_local = 0; i_local < cand_con; i_local++)
					path_sum += path_acuu[i_local];
				leaf_path_info[leaf].setLeft(before_con);//set the connection node array
				//===================================================

				double ranking = path_sum / cand_con;
				leaf_path_info[leaf].setRight(ranking);
//				cerr << "ranking is " << ranking << endl;
				if (ranking < min_ranking){
					min_ranking = ranking;
					min_leaf_index = i;
				}

			}//end for

//			cerr << " out of for " << endl;
	//
//			cerr << "min_leaf_index " <<min_leaf_index << endl;
	//
//			cerr << "leaf_id " << leaf_nodes[min_leaf_index] << endl;

			int leaf_id = leaf_nodes[min_leaf_index];
			temp_array_query_index = 0; //always put to zero before using it, for safety

			while ( sequence_flag_query[leaf_id] == 0){ // here, actually stop at an already selected node
				temp_array_query[temp_array_query_index ++] = leaf_id;
				leaf_id = core_query_tree[leaf_id].parent_node;//this node's parent
			}

			// add the sequence in the temp array into the sequence array
			while (temp_array_query_index != 0){
				temp_array_query_index --;
				addIntoSequence_for_whole_graph(temp_array_query[temp_array_query_index], false, sequence_flag_query);	//third
			}

			//remove the selected leaf
			leaf_nodes[min_leaf_index] = leaf_nodes[leaf_nodes_index - 1];
			leaf_nodes_index --;
		}

//		cerr << "dasda" << endl;


		{//now we start to select the rest of the paths

			int min_leaf_index = 0;

			while (leaf_nodes_index != 0){

				double min_ranking = Double.MAX_VALUE;

				for (int i = 0; i < leaf_nodes_index; i ++){

					int leaf = leaf_nodes[i];
					if (sequence_flag_query [ leaf_path_info[leaf].getLeft() ] == 0){//the last connection node is still unselected, which means its connection node remains unchanged
						if (leaf_path_info[leaf].getRight() < min_ranking){
							min_ranking = leaf_path_info[leaf].getRight();
							min_leaf_index = i;
						}
						continue;
					}

					double cand_con;
					double path_sum = 0;

					//========== find its connection node
					NodeIndexUnit unit_leaf = indexSet[leaf];
					int node = core_query_tree[leaf].parent_node;
					int before_con = leaf; //set the node before connection node

					for (int i_local = 0; i_local < indexSet[BFS_parent_query[leaf]].size; i_local++)
						path_acuu[i_local] = unit_leaf.size_of_index[i_local];

					while ( sequence_flag_query[node] == 0 ){ // here, stop when reaching the connection node

						NodeIndexUnit unit_node = indexSet[node];
						int cand_size = indexSet[BFS_parent_query[node]].size;

						for (int x = 0; x < cand_size; x++){
							path_temp[x] = 0;
							for (int y = 0; y < unit_node.size_of_index[x]; y++){
								int pos = unit_node.index[x][y];
								path_temp[x] += path_acuu[pos];
							}
						}

						for (int x = 0; x < cand_size; x++)
							path_acuu[x] = path_temp[x];

						before_con = node;//set before connection node
						node = core_query_tree[node].parent_node;//this node's parent
					}

					cand_con = indexSet[node].size;
					leaf_path_info[leaf].setLeft(before_con);//set the connection node array

					for (int i_local = 0; i_local < cand_con; i_local++)
						path_sum += path_acuu[i_local];
					//==============================

					double ranking = path_sum / (cand_con);
					leaf_path_info[leaf].setRight(ranking);

					if (ranking < min_ranking){
						min_ranking = ranking;
						min_leaf_index = i;
					}

				}//end for

				//add this leaf and its path into the matching sequence
				int leaf_id = leaf_nodes[min_leaf_index];
				temp_array_query_index = 0; //always put to zero before using it, for safety
				while ( sequence_flag_query[leaf_id] == 0){ // here, actually stop at an already selected node
					temp_array_query[temp_array_query_index ++] = leaf_id;
					leaf_id = core_query_tree[leaf_id].parent_node;//this node's parent
				}

				while (temp_array_query_index != 0){// add the sequence in the temp array into the sequence array
					temp_array_query_index --;
					addIntoSequence_for_whole_graph(temp_array_query[temp_array_query_index], false, sequence_flag_query);	//fourth
				}
				//=========================================

				//remove the selected leaf
				leaf_nodes[min_leaf_index] = leaf_nodes[leaf_nodes_index - 1];
				leaf_nodes_index --;

			} //end while
		}
//		displaySequence();

	}
	
	private void addIntoSequence_for_whole_graph(int node, boolean isRoot, int[] sequence_flag){

		if (isRoot) {
			assignSequenceUnit(matching_unit[matching_sequence_index], node, -1, 0, 0);
			sequence_flag[ node ] = matching_sequence_index + 1;
			matching_sequence_index ++;
//			cerr << "set " << node << " 's parent to -1." << endl;
		} else {

			Core_query_tree_node  tree_node = core_query_tree[node];

			if (tree_node.nte.getRight().equals(0) == false){

				int begin = nte_array_for_matching_unit_index;

				for (int i = tree_node.nte.getLeft(); i < tree_node.nte.getLeft() + tree_node.nte.getRight(); i++){
					int other_end = core_tree_node_nte_array[i];
					if (sequence_flag[other_end] > 0){
						nte_array_for_matching_unit[nte_array_for_matching_unit_index] = sequence_flag[other_end] - 1;
						nte_array_for_matching_unit_index ++;
					}
				}

				int end = nte_array_for_matching_unit_index;
				assignSequenceUnit(matching_unit[ matching_sequence_index ], node,  sequence_flag[tree_node.parent_node] - 1, end - begin, begin);

			} else
				assignSequenceUnit(matching_unit[ matching_sequence_index ], node, sequence_flag[tree_node.parent_node] - 1, 0, 0);

			sequence_flag[ node ] = matching_sequence_index + 1;
			matching_sequence_index ++;
		}
	}
	
	private void assignSequenceUnit(SequenceUnit unit, int node, int parent_index, int nte_length, int start_pos){
		unit.node = node;
		unit.parent_index = parent_index;
		unit.nte_length = nte_length;
		unit.start_pos = start_pos;
	}
	
	public void getTreeMatchingSequence_TREE(){

		if (residual_tree_match_seq_index == 0){
			residual_tree_match_seq_index = 0;
			residual_tree_match_seq[residual_tree_match_seq_index ++] = root_node_id;	//add the root
			return;
		}

		if (residual_tree_match_seq_index == 1){
			residual_tree_match_seq_index = 0;
			residual_tree_match_seq[residual_tree_match_seq_index ++] = root_node_id;	//add the root
			residual_tree_match_seq[residual_tree_match_seq_index ++] = residual_tree_leaf_node.get(0).getLeft();	//add the root
			return;
		}

		//now we have all the leafs; sort them by the path number
		for (int i = 0; i < residual_tree_leaf_node_index; i++){
			int current_node = residual_tree_leaf_node.get(i).getLeft();
			int sum = 0;
			for (int x = 0; x < indexSet[current_node].size; x++)
				sum += indexSet[current_node].path[x];
			residual_tree_leaf_node.get(i).setRight((double)sum);
		}

//		sort (residual_tree_leaf_node, residual_tree_leaf_node + residual_tree_leaf_node_index, sort_by_second_element);
		Collections.sort(residual_tree_leaf_node.subList(0, residual_tree_leaf_node_index), new Comparator<Pair<Integer, Double>>() {

			public int compare(Pair<Integer, Double> o1, Pair<Integer, Double> o2) {
				return o1.getRight().compareTo(o2.getRight());
			}
		});

		residual_tree_match_seq_index = 0;
//		memset(sequence_flag_query, 0, sizeof(int) * cnt_node_query); //reset all flag
		for(int i = 0; i < cnt_node_query; i++)
			sequence_flag_query[i] = 0;
		
		sequence_flag_query[root_node_id] = 1;	//set the root to 1
		residual_tree_match_seq[residual_tree_match_seq_index ++] = root_node_id;	//add the root

		for (int i = 0; i < residual_tree_leaf_node_index; i++){
			int leaf_id = residual_tree_leaf_node.get(i).getLeft();
			count_global_temp_array_1 = 0;
			while (sequence_flag_query[leaf_id] == 0){
				global_temp_array_1[count_global_temp_array_1 ++] = leaf_id;
				sequence_flag_query[leaf_id] = 1;
				leaf_id = tree_node_parent[leaf_id];
			}
			while (count_global_temp_array_1 != 0){
				count_global_temp_array_1 --;
				residual_tree_match_seq[residual_tree_match_seq_index ++] = global_temp_array_1[count_global_temp_array_1];
			}
		}

	}
	
	public void getTreeMatchingSequence_NORMAL(){

		//now we have all the leafs; sort them by the path number
		for (int i = 0; i < residual_tree_leaf_node_index; i++){
			int current_node = residual_tree_leaf_node.get(i).getLeft();
			int sum = 0;
			for (int x = 0; x < indexSet[current_node].size; x++)
				sum += indexSet[current_node].path[x];
			residual_tree_leaf_node.get(i).setRight((double)sum);
		}

//		sort (residual_tree_leaf_node, residual_tree_leaf_node + residual_tree_leaf_node_index, sort_by_second_element);
		Collections.sort(residual_tree_leaf_node.subList(0, residual_tree_leaf_node_index), new Comparator<Pair<Integer, Double>>() {

			public int compare(Pair<Integer, Double> o1, Pair<Integer, Double> o2) {
				return o1.getRight().compareTo(o2.getRight());
			}
		});
		residual_tree_match_seq_index = 0;

		for (int i = 0; i < residual_tree_leaf_node_index; i++){
			int leaf_id = residual_tree_leaf_node.get(i).getLeft();
			count_global_temp_array_1 = 0;
			while (sequence_flag_query[leaf_id] == 0){
				global_temp_array_1[count_global_temp_array_1 ++] = leaf_id;
				sequence_flag_query[leaf_id] = 1;
				leaf_id = tree_node_parent[leaf_id];
			}
			while (count_global_temp_array_1 != 0){
				count_global_temp_array_1 --;
				residual_tree_match_seq[residual_tree_match_seq_index ++] = global_temp_array_1[count_global_temp_array_1];
			}
		}
	}
	
	public int[] getTreeMapping_Enumeration() {
		SearchUnit temp_su;
		int current;
		int parent;
		NodeIndexUnit index_unit;
		int pos;
		int data_id ;
		int current_tree_query_index = 1;
		int tree_sequence_size = residual_tree_match_seq_index;
		int[] found_mapping_enumeration = new int[1];
		for (int i = 0; i < indexSet[root_node_id].size; i++) {
			int root_cand_id = indexSet[root_node_id].candidates[i];
			if (root_cand_id == -1)//if it is pre-filtered
				continue;
			mapping_flag_data[root_cand_id] = true;//flag for already matched data node
			actual_mapping[0] = root_cand_id;
			self_pos[root_node_id] = i;
			current_tree_query_index = 1;
			while (true) {
				if (current_tree_query_index == 0)//"No MATCH found!"
					break;
				if (current_tree_query_index == tree_sequence_size) { // found a mapping

					if (NEC_mapping_pair_index != 0)
						LeafMappingEnumeration(found_mapping_enumeration);
					else {
						for (int i_local = 0; i_local < residual_tree_match_seq_index; i_local++) //for tree if existed
							all_mapping[ residual_tree_match_seq[i_local] ] = actual_mapping_tree[i_local];

						boolean satisfied = true;
//	if(SPAT_PREDICATE)
//	{
//						for (map<int, MyRect>::iterator it = spat_pred.begin(); it != spat_pred.end(); it++)
//						{
//							int spat_id = (*it).first;
//							MyRect query_rect = (*it).second.copy();
//
//							int map_node_id = all_mapping[spat_id];
//							if(!entities[map_node_id].IsSpatial)
//							{
//								satisfied = false;
//								break;
//							}
//							Location loc = entities[map_node_id].location;
//
//							if (Located_In(query_rect, loc))
//								continue;
//							else
//							{
//								satisfied = false;
//								break;
//							}
//						}
//	}
						if(satisfied)
						{
							found_mapping_enumeration[0] = found_mapping_enumeration[0] + 1;
							if(COUT_RESULT)
							{
								System.out.print(String.format("Mapping %d => ", found_mapping_enumeration[0]));
								for(int i_local = 0; i_local < cnt_node_query; i_local++)
									System.out.print(String.format("%d:%d ", i_local, all_mapping[i_local]));
								System.out.print("\n");
							}
						}
					}
					if (found_mapping_enumeration[0] >= LIMIT){
						while (current_tree_query_index != 0){
							current_tree_query_index --;
							mapping_flag_data[ actual_mapping_tree[current_tree_query_index] ] = false;
							su_tree[current_tree_query_index].address = null;
						}
						mapping_flag_data[root_cand_id] = true;
						return found_mapping_enumeration;
					}
					mapping_flag_data[actual_mapping_tree[tree_sequence_size - 1]] = false;
					current_tree_query_index --;
					continue;
				}
				char back_trace = 0;
				temp_su = su_tree[current_tree_query_index];
				current = residual_tree_match_seq[current_tree_query_index];
				parent = tree_node_parent[current];
				index_unit = indexSet[current];
				if (temp_su.address == null) { // has no value
					int parent_index = self_pos[parent];
					temp_su.address = index_unit.index[parent_index];
					temp_su.address_size = index_unit.size_of_index[parent_index];
					if (temp_su.address_size == 0) {
						temp_su.address = null;//clear the temp_address and it index position
						current_tree_query_index--; // roll back one node in the matching sequence
						if (current_tree_query_index != 0)
							mapping_flag_data[ actual_mapping_tree[current_tree_query_index] ] = false;
						continue;
					}
					temp_su.address_pos = 0;
				} else { // has value
					temp_su.address_pos++; // update the index by one
					if ( temp_su.address_pos == temp_su.address_size) {
						temp_su.address = null;//clear the temp_address and it index position
						current_tree_query_index--;
						if (current_tree_query_index != -1)
							mapping_flag_data[ actual_mapping_tree[ current_tree_query_index ] ] = false;
						continue;
					}
				}
				back_trace = 0; //actually, this line is not necessary, when processed here, the back_trace must be false...
				while (true) {
					//break, until find a mapping for this node
					//or cannot find a mapping after examining all candidates
					pos = index_unit.index[ self_pos[parent] ][ temp_su.address_pos ];
					data_id = index_unit.candidates[pos];
					if (data_id != -1 && mapping_flag_data[data_id] == false) { //first check: this id should have not been mapped before
						actual_mapping_tree[current_tree_query_index] = data_id;
						self_pos[current] = pos;
						mapping_flag_data[data_id] = true;
						break;
					} else { //mapping NOT OK!
						temp_su.address_pos++;//not ok, then we need the next result
						if (temp_su.address_pos == temp_su.address_size) { // no more data id, so cannot find a match for this query node
							back_trace = 1; // indicate that no result is being found, so we need to trace back_trace
							break;
						}
					}
				} //end while
				if (back_trace != 0) { //BACK TRACE
					back_trace = 0;
					temp_su.address = null;//clear the temp_address and it index position
					current_tree_query_index--;
					if (current_tree_query_index != -1)
						mapping_flag_data[ actual_mapping_tree[ current_tree_query_index ] ] = false;
				} else
					current_tree_query_index++;

			}//end while
			mapping_flag_data[root_cand_id] = false;
		}
		return found_mapping_enumeration;
	}
	
	public int[] getFullMapping_Enumeration() 
	{
		int[] found_mapping_enumeration = new int[1];
		found_mapping_enumeration[0] = 0;
		
		SequenceUnit unit;
		SearchUnit temp_su;
		int current;
		int parent;
		NodeIndexUnit index_unit;
		int pos;
		int data_id ;
		char mapping_OK;
		for (int i = 0; i < indexSet[root_node_id].size; i++) {
			int root_cand_id = indexSet[root_node_id].candidates[i];
			if (root_cand_id == -1)
				continue;
			mapping_flag_data[root_cand_id] = true;
			actual_mapping[0] = root_cand_id;
			self_pos[root_node_id] = i;
			boolean back_trace = false;
			int current_query_index = 1; // the current query index of the query sequence, because 0 is the root has already been matched
			while (true) {
				if (current_query_index == 0)//"No MATCH found!"
					break;
				if (current_query_index == matching_sequence_index) { // found a mapping

					if (residual_tree_match_seq_index == 0){
						if (NEC_mapping_pair_index != 0)
							LeafMappingEnumeration(found_mapping_enumeration);
						else {
							found_mapping_enumeration[0] = found_mapping_enumeration[0] +1;
							for (int i_local = 0; i_local < matching_sequence_index; i_local++) //for core
								all_mapping[ matching_unit[i_local].node ] =  actual_mapping[i_local];
						}
					}
					else
						getResidualTreeMapping_Enumeration(found_mapping_enumeration);

					if (found_mapping_enumeration[0] >= LIMIT){
						//need to clean up the two array: mapping_flag_data, su[i].address for next time using
						while (current_query_index != 0){
							current_query_index --;
							mapping_flag_data[ actual_mapping[current_query_index] ] = false;
							su[current_query_index].address = null;
						}
						mapping_flag_data[root_cand_id] = false;
						return found_mapping_enumeration;
					}

					mapping_flag_data[ actual_mapping[matching_sequence_index - 1] ] = false;
					current_query_index --;
					continue;
				}


				unit = matching_unit[current_query_index];
				temp_su = su[current_query_index];
				current = unit.node;
				parent = matching_unit[unit.parent_index].node;
				index_unit = indexSet[current];

				if (temp_su.address == null) { // has no value

					int parent_index = self_pos[parent];

					temp_su.address = index_unit.index[parent_index];
					temp_su.address_size = index_unit.size_of_index[parent_index];

					if (temp_su.address_size == 0) {
						temp_su.address = null;//clear the temp_address and it index position
						current_query_index--; // roll back one node in the matching sequence
						if (current_query_index != 0)
							mapping_flag_data[ actual_mapping[current_query_index] ] = false;
						continue;
					}
					temp_su.address_pos = 0;
				} else { // has value
					temp_su.address_pos++; // update the index by one
					if ( temp_su.address_pos == temp_su.address_size) {
						temp_su.address = null;//clear the temp_address and it index position
						current_query_index--;
						mapping_flag_data[ actual_mapping[ current_query_index ] ] = false;
						continue;
					}
				}

				back_trace = false; //actually, this line is not necessary, when processed here, the back_trace must be false...

				while (true) {
					//break, until find a mapping for this node
					//or cannot find a mapping after examining all candidates

					pos = index_unit.index[ self_pos[parent] ][ temp_su.address_pos ];
					data_id = index_unit.candidates[pos];
					
//					if(data_id == 0)
//						OwnMethods.Print(data_id);	//debug

					mapping_OK = 1;
					if (!mapping_flag_data[data_id]) { //first check: this id should have not been mapped before

						if (unit.nte_length > 0) { //second check: check and validate the nontree edge

							for (int j = unit.start_pos; j < unit.start_pos + unit.nte_length; j++){

								if (SIZEOF == 1){
									if (data_edge_matrix[data_id * SIZE_OF_EDGE_MATRIX + actual_mapping[ nte_array_for_matching_unit[j] ]] == false){
										mapping_OK = 0;
										break;
									}
								} else {
									if ( data_edge_matrix[(data_id % SIZE_OF_EDGE_MATRIX) * SIZE_OF_EDGE_MATRIX + ( actual_mapping[ nte_array_for_matching_unit[j] ] % SIZE_OF_EDGE_MATRIX)] == false){
										mapping_OK = 0;
										break;
									} else {
										//validate the non tree edge by querying the edgeIndex??
//										if ( ! ht[data_id].query( actual_mapping[ nte_array_for_matching_unit[j] ] ) ){
//											mapping_OK = 0;
//											break;
//										}
										
									}
								}

							}										// end for
						}										// end if

					} else
						mapping_OK = 0;

					if (mapping_OK == 1) {
						actual_mapping[current_query_index] = data_id;
						self_pos[current] = pos;
						mapping_flag_data[data_id] = true;
						break;
					} else { //mapping NOT OK!
						temp_su.address_pos++;//not ok, then we need the next result
						if (temp_su.address_pos == temp_su.address_size) { // no more data id, so cannot find a match for this query node
							back_trace = true; // indicate that no result is being found, so we need to trace back_trace
							break;
						}
					}

				} //end while

				if (back_trace) { //BACK TRACE
					back_trace = false;
					temp_su.address = null;//clear the temp_address and it index position
					current_query_index--;
					mapping_flag_data[ actual_mapping[ current_query_index ] ] = false;
				} else
					current_query_index++;

			}
			//===========================================================================================
			mapping_flag_data[root_cand_id] = false;
		}
		return found_mapping_enumeration;
	}
	
	private int[] LeafMappingEnumeration(int[] found_mapping_enumeration) {

		/*
		 * This function only enumerates all the results. Not tuned for performance at all.
		 */

		//step one: get all actual nodes in leaf
		//step two: get the candidates for all nodes.
		//step three: enumerate results until reaching the limit

		//sort the LLC sets
		int nec_set_size = NEC_set_by_label_index.size() - 1;// -1 is because the last element is actually redunant
		for (int i = 0; i < nec_set_size; i++) {
			int cand_sum = 0;
			int node_sum = 0;
			int start = NEC_set_by_label_index.get(i).getRight();
			int end = NEC_set_by_label_index.get(i + 1).getRight();
			for (int j = start; j < end; j++) {
				int parent_id = NEC_set_array[j].parent_id;
				int sum = NEC_set_array[j].sum;
				int represent_node = NEC_set_array[j].represent_node;
				int parent_pos = self_pos[parent_id];
				NodeIndexUnit unit = indexSet[represent_node];
				for (int it = 0; it < unit.size_of_index[parent_pos]; it++){
					int can = unit.candidates[ unit.index[parent_pos][it] ];
					if (!mapping_flag_data[ can ])
						cand_sum ++;
				}
				node_sum += sum; //moved up
			}
			if (cand_sum < node_sum)
				return null;
			NEC_set_ranking.set(i, new Pair(i, new Pair<Integer, Double>(node_sum, (double) cand_sum / (double) node_sum)));
		}
//		sort(NEC_set_ranking, NEC_set_ranking + nec_set_size, sort_by_second_double);
		Collections.sort(NEC_set_ranking.subList(0, nec_set_size), new Comparator<Pair<Integer, Pair<Integer, Double>>>() {

			public int compare(Pair<Integer, Pair<Integer, Double>> o1, Pair<Integer, Pair<Integer, Double>> o2) {
				return o1.getRight().getRight().compareTo(o2.getRight().getRight());
			}
		});
		//=================================================================================

		//============= get all leaf nodes and their candidates
		leaf_necs_idx = 0;
		ArrayList<Integer> leaf_cands = new ArrayList<Integer>();
		ArrayList<Pair<Integer, Integer>> leaf_cands_info =  new ArrayList<Pair<Integer,Integer>>();

		for (int i = 0; i < nec_set_size; i++) {

			int label_index = NEC_set_ranking.get(i).getLeft();//the label of the NEC_node
			int node_sum = NEC_set_ranking.get(i).getRight().getLeft();//number of exact query nodes in this NEC_node
			double count_local_mapping_label = 0;

			if (node_sum == 1) {//==== CASE ONE : there is only one node in this nec set with this label =====

				int start = NEC_set_by_label_index.get(label_index).getRight();
				int represent_node = NEC_set_array[start].represent_node;

				int start_l = leaf_cands.size();

				NodeIndexUnit unit = indexSet[represent_node];
				int parent_id = NEC_set_array[start].parent_id;
				int parent_pos = self_pos[parent_id];//?


				leaf_necs [ leaf_necs_idx ++ ] = represent_node;

				for (int it = 0; it < unit.size_of_index[parent_pos]; it++){
					int can = unit.candidates[ unit.index[parent_pos][it] ];
					if (mapping_flag_data[ can ] == false)
						leaf_cands.add(can);
				}

				int end_l = leaf_cands.size();

				leaf_cands_info.add( new Pair<Integer, Integer>(start_l, end_l) );//offset of leaf_cands

				count_local_mapping_label = NEC_set_ranking.get(i).getRight().getRight(); //we have computed this one in the last step
				if (count_local_mapping_label == 0)
					return null;
			} else {//==== CASE TWO : more than one node, and possible more than one start nodes (nec_units)

				int start = NEC_set_by_label_index.get(label_index).getRight();
				int end = NEC_set_by_label_index.get(label_index + 1).getRight();
				int nec_size = end - start; //number of nec this label has

				for (int j = start, x = 0; j < end; j++, x++)
					v_nec_count.set(x, new Pair<Integer, Integer>(j, NEC_set_array[j].sum));
//				sort(v_nec_count, v_nec_count + nec_size, sort_by_second_element);
				Collections.sort(v_nec_count.subList(0, nec_size), new Comparator<Pair<Integer, Integer>>() {

					public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
						return o1.getRight().compareTo(o2.getRight());
					}
				});
				nec_count_set_size = nec_size;

				for (int j = 0; j < nec_size; j++) {
					int nec_index = v_nec_count.get(j).getLeft();
					int nec_count = v_nec_count.get(j).getRight();
					nec_count_set[j] = nec_count; // the sum of nodes that the nec representative stands for

					int parent_id = NEC_set_array[nec_index].parent_id;
					int represent_node = NEC_set_array[nec_index].represent_node;
					int parent_pos = self_pos[parent_id];

					NodeIndexUnit unit = indexSet[represent_node];
					int start_local = leaf_cands.size();
					for (int it = 0; it < unit.size_of_index[parent_pos]; it++){
						int can = unit.candidates[ unit.index[parent_pos][it] ];
						if (mapping_flag_data[ can ] == false)
							leaf_cands.add(can);
					}
					int end_local = leaf_cands.size();

					if (end_local - start_local < nec_count)
						return null;

					leaf_necs[leaf_necs_idx++] = represent_node;
					leaf_cands_info.add( new Pair<Integer, Integer>(start_local, end_local) );
					NEC_Node next;
					int next_id = NEC_Node_array[represent_node].next;
					while (next_id != -1) {
						next = NEC_Node_array[next_id];
						leaf_necs[leaf_necs_idx++] = next.node;
						leaf_cands_info.add( new Pair<Integer, Integer>(start_local, end_local) );
						next_id = next.next;
					}
				}
			}
		}
		//====================================================================

//		cerr << "before searching " << endl;
//		for (int i = 0; i < leaf_necs_idx; i ++)
//			cerr << leaf_necs[i] << " ";
//		cerr << endl;


		SearchUnit[] su_leaf = new SearchUnit[cnt_node_query];
		for(int i = 0; i < cnt_node_query; i++)
			su_leaf[i] = new SearchUnit();
		int [] actual_mapping_leaf = new int [cnt_node_query];
		for(int i = 0; i < cnt_node_query; i++)
			su_leaf[i].address = null;

		//=========== start to mapping =======================================================
		{
			SearchUnit temp_su;
			int data_id ;
			int current_leaf_query_index = 0;
			int leaf_sequence_size = leaf_necs_idx;

			while (true) {

//				cerr << " current leaf query index is " << current_leaf_query_index << " out of " << leaf_sequence_size << endl;

				if (current_leaf_query_index == -1)//"No MATCH found!"
					break;

				if (current_leaf_query_index == leaf_sequence_size) { // found a mapping

//					cerr << "found mapping " << found_mapping_enumeration << endl;

					if (isTree){
						for (int i = 0; i < residual_tree_match_seq_index; i++) //for tree if existed
							all_mapping[ residual_tree_match_seq[i] ] = actual_mapping_tree[i];
						for (int i = 0; i < leaf_necs_idx; i++) //for leaf if existed
							all_mapping[ leaf_necs[ i ] ] = actual_mapping_leaf[i];
					} else {
						for (int i = 0; i < matching_sequence_index; i++) //for core
							all_mapping[ matching_unit[i].node ] =   actual_mapping[i];
						for (int i = 0; i < residual_tree_match_seq_index; i++) //for tree if existed
							all_mapping[ residual_tree_match_seq[i] ] = actual_mapping_tree[i];
						for (int i = 0; i < leaf_necs_idx; i++) //for leaf if existed
							all_mapping[ leaf_necs[ i ] ] = actual_mapping_leaf[i];
					}

					boolean satisfied = true;
//	if(SPAT_PREDICATE)
//					for (map<int, MyRect>::iterator it = spat_pred.begin(); it != spat_pred.end(); it++)
//					{
//						int spat_id = (*it).first;
//						MyRect query_rect = (*it).second.copy();
//
//						int map_node_id = all_mapping[spat_id];
//						if(!entities[map_node_id].IsSpatial)
//						{
//							satisfied = false;
//							break;
//						}
//						Location loc = entities[map_node_id].location;
//
//						if (Located_In(query_rect, loc))
//							continue;
//						else
//						{
//							satisfied = false;
//							break;
//						}
//					}
//	#endif
					if(satisfied)
					{
						found_mapping_enumeration[0] = found_mapping_enumeration[0] + 1;
						if(COUT_RESULT)
						{
							System.out.print(String.format("Mapping %d => ", found_mapping_enumeration[0]));
							for(int i = 0; i < cnt_node_query; i++)
								System.out.print(String.format("%d:%d ", i, all_mapping[i]));
							System.out.print("\n");
//							cout << "Mapping " << found_mapping_enumeration << " => ";
//							for (int i = 0; i < cnt_node_query; i++)
//								cout << i << ":" << all_mapping[i] << " ";
//							cout << endl;
						}
					}
					if (found_mapping_enumeration[0] >= LIMIT){
						while (current_leaf_query_index != 0){
							current_leaf_query_index --;
							mapping_flag_data[ actual_mapping_leaf[current_leaf_query_index] ] = false;
							su_leaf[current_leaf_query_index].address = null;
						}
						break;
					}

					mapping_flag_data[actual_mapping_leaf[leaf_sequence_size - 1]] = false;
					current_leaf_query_index --;
					continue;
				}

				boolean back_trace = false;
				temp_su =su_leaf[current_leaf_query_index];

				if (temp_su.address == null) { // has no value

					temp_su.address = actual_mapping_leaf;//meaningless. just for giving it a value for code reusing
					temp_su.address_size = leaf_cands_info.get(current_leaf_query_index).getRight();

					if (temp_su.address_size == 0) {
						temp_su.address = null;//clear the temp_address and it index position
						current_leaf_query_index--; // roll back one node in the matching sequence
						if (current_leaf_query_index != 0)
							mapping_flag_data[ actual_mapping_leaf[current_leaf_query_index] ] = false;
						continue;
					}
					temp_su.address_pos = leaf_cands_info.get(current_leaf_query_index).getLeft();
				} else { // has value
					temp_su.address_pos++; // update the index by one
					if ( temp_su.address_pos == temp_su.address_size) {
						temp_su.address = null;//clear the temp_address and it index position
						current_leaf_query_index--;
						if (current_leaf_query_index != -1)
							mapping_flag_data[ actual_mapping_leaf[ current_leaf_query_index ] ] = false;
						continue;
					}
				}

				back_trace = false; //actually, this line is not necessary, when processed here, the back_trace must be false...

				while (true) {
					//break, until find a mapping for this node
					//or cannot find a mapping after examining all candidates

					data_id = leaf_cands.get(temp_su.address_pos);

					if (data_id != -1 && mapping_flag_data[data_id] == false) { //first check: this id should have not been mapped before
						actual_mapping_leaf[current_leaf_query_index] = data_id;
						mapping_flag_data[data_id] = true;
						break;
					} else { //mapping NOT OK!
 						temp_su.address_pos++;//not ok, then we need the next result
						if (temp_su.address_pos == temp_su.address_size) { // no more data id, so cannot find a match for this query node
							back_trace = true; // indicate that no result is being found, so we need to trace back_trace
							break;
						}
					}
				} //end while

				if (back_trace) { //BACK TRACE
					back_trace = false;
					temp_su.address = null;//clear the temp_address and it index position
					current_leaf_query_index--;
					if (current_leaf_query_index != -1)
						mapping_flag_data[ actual_mapping_leaf[ current_leaf_query_index ] ] = false;
				} else
					current_leaf_query_index++;
			}
		}
		return found_mapping_enumeration;
	}
	
	private void getResidualTreeMapping_Enumeration(int[] found_mapping_enumeration) {
		SearchUnit temp_su;
		int current;
		int parent;
		NodeIndexUnit index_unit;
		int pos;
		int data_id ;
		int current_tree_query_index = 0;
		int tree_sequence_size = residual_tree_match_seq_index;
		while (true) {
			if (current_tree_query_index == -1)//"No MATCH found!"
				break;
			if (current_tree_query_index == tree_sequence_size) { // found a mapping
				if (NEC_mapping_pair_index != 0)
					LeafMappingEnumeration(found_mapping_enumeration);
				else{
					for (int i = 0; i < matching_sequence_index; i++) //for core
						all_mapping[ matching_unit[i].node ] =  actual_mapping[i];
					for (int i = 0; i < residual_tree_match_seq_index; i++) //for tree if existed
						all_mapping[ residual_tree_match_seq[i] ] = actual_mapping_tree[i];

					boolean satisfied = true;
					//	if(SPAT_PREDICATE)
					//					for (map<int, MyRect>::iterator it = spat_pred.begin(); it != spat_pred.end(); it++)
					//					{
					//						int spat_id = (*it).first;
					//						MyRect query_rect = (*it).second.copy();
					//
					//						int map_node_id = all_mapping[spat_id];
					//						if(!entities[map_node_id].IsSpatial)
					//						{
					//							satisfied = false;
					//							break;
					//						}
					//						Location loc = entities[map_node_id].location;
					//
					//						if (Located_In(query_rect, loc))
					//							continue;
					//						else
					//						{
					//							satisfied = false;
					//							break;
					//						}
					//					}
					//	#endif
					if(satisfied)
					{
						found_mapping_enumeration[0] = found_mapping_enumeration[0] + 1;
						if(COUT_RESULT)
						{
							System.out.print(String.format("Mapping %f => ", found_mapping_enumeration));
							for(int i = 0; i < cnt_node_query; i++)
								System.out.print(String.format("%d:%d ", i, all_mapping[i]));
							System.out.print("\n");
							//							cout << "Mapping " << found_mapping_enumeration << " => ";
							//							for (int i = 0; i < cnt_node_query; i++)
							//								cout << i << ":" << all_mapping[i] << " ";
							//							cout << endl;
						}
					}			
				}

				if (found_mapping_enumeration[0] >= LIMIT){
					while (current_tree_query_index != 0){
						current_tree_query_index --;
						mapping_flag_data[ actual_mapping_tree[current_tree_query_index] ] = false;
						su_tree[current_tree_query_index].address = null;
					}
					return;
				}

				mapping_flag_data[actual_mapping_tree[tree_sequence_size - 1]] = false;
				current_tree_query_index --;
				continue;
			}
			boolean back_trace = false;
			temp_su = su_tree[current_tree_query_index];
			current = residual_tree_match_seq[current_tree_query_index];
			parent = tree_node_parent[current];
			index_unit = indexSet[current];
			if (temp_su.address == null) { // has no value
				int parent_index = self_pos[parent];
				temp_su.address = index_unit.index[parent_index];
				temp_su.address_size = index_unit.size_of_index[parent_index];
				if (temp_su.address_size == 0) {
					temp_su.address = null;//clear the temp_address and it index position
					current_tree_query_index--; // roll back one node in the matching sequence
					if (current_tree_query_index != 0)
						mapping_flag_data[ actual_mapping_tree[current_tree_query_index] ] = false;
					continue;
				}
				temp_su.address_pos = 0;
			} else { // has value
				temp_su.address_pos++; // update the index by one
				if ( temp_su.address_pos == temp_su.address_size) {
					temp_su.address = null;//clear the temp_address and it index position
					current_tree_query_index--;
					if (current_tree_query_index != -1)
						mapping_flag_data[ actual_mapping_tree[ current_tree_query_index ] ] = false;
					continue;
				}
			}
			back_trace = false; //actually, this line is not necessary, when processed here, the back_trace must be false...
			while (true) {
				
				pos = index_unit.index[ self_pos[parent] ][ temp_su.address_pos ];
				
				data_id = index_unit.candidates[pos];
				if (data_id != -1 && mapping_flag_data[data_id] == false) { //first check: this id should have not been mapped before
					actual_mapping_tree[current_tree_query_index] = data_id;
					self_pos[current] = pos;
					mapping_flag_data[data_id] = true;
					break;
				} else { //mapping NOT OK!
					temp_su.address_pos++;//not ok, then we need the next result
					if (temp_su.address_pos == temp_su.address_size) { // no more data id, so cannot find a match for this query node
						back_trace = true; // indicate that no result is being found, so we need to trace back_trace
						break;
					}
				}
			} //end while
			if (back_trace) { //BACK TRACE
				back_trace = false;
				temp_su.address = null;//clear the temp_address and it index position
				current_tree_query_index--;
				if (current_tree_query_index != -1)
					mapping_flag_data[ actual_mapping_tree[ current_tree_query_index ] ] = false;
			} else
				current_tree_query_index++;
		}
		return;
	}
}
