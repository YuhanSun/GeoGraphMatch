package org.datasyslab.GeoGraphMatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.neo4j.helpers.collection.PagingIterator;

public class Data_Graph {
	//they are initialized when object is constructed
	public int[] label_list;
	ArrayList<ArrayList<Integer>> graph;
	public int cnt_node;
	
	/**
	 * a flag to indicate adjacent list is sorted by label (ascend) and degree (descend)
	 */
	public boolean adjacentlist_sorted = false;
	
	/**
	 * used and initialized in core_decomposition
	 */
	public int MAX_DEGREE;
	
	
	// they are not computed without calling the Calculate function
	HashMap<Integer, Integer> label_cardinality;
	ArrayList<Label_Degree_Node> label_degree_nodes;	//correct
	ArrayList<Integer> label_deg_label_pos;	//correct
	int[] core_number_data;
	int[] MAX_NB_degree_data;
	
	public Pair<Integer, Integer>[] nodes_to_label_info;
	public int[] nodes_data;
	
	public int[] degree_data;
	
	
	public Data_Graph(int node_count)
	{
		this.cnt_node = node_count;
		label_list = new int[node_count];
		graph = new ArrayList<ArrayList<Integer>>(node_count);
	}
	
	public Data_Graph(String datagraph_path, HashMap<Integer, Integer> transfer_table)
	{
		BufferedReader reader = null;
		String line = null;
		try {
			reader = new BufferedReader(new FileReader(new File(datagraph_path)));
			line = reader.readLine();
			String [] line_list = line.split(" ");
			
			if(line_list.length != 3)
				throw new Exception("data graph first line parameters number mismatch!");
			
			if(line_list[0].equals("t") == false)
				throw new Exception("query graph first line does begin with 't'!");
			
			cnt_node = Integer.parseInt(line_list[2]);
			label_list = new int[cnt_node];
			graph = new ArrayList<ArrayList<Integer>>(cnt_node);
			for(int i = 0; i<cnt_node; i++)
			{
				graph.add(new ArrayList<Integer>());
				line = reader.readLine();
				line_list = line.split(" ");
				if(line_list.length != 3)
					throw new Exception("node line parameters number mismatch!");
				if(line_list[0].equals("v") == false)
					throw new Exception("node line does not start with 'v'");
				
				int id = Integer.parseInt(line_list[1]);
				if(id != i)
					throw new Exception("node id does not match with line index!");
				int label_read = Integer.parseInt(line_list[2]);
				Object ob = transfer_table.get(label_read);
				if(ob ==  null)
					throw new Exception("transfer table does not have the label!");
				int real_label = (Integer) ob;
				label_list[i] = real_label;
			}
			
			while((line = reader.readLine()) != null)
			{
				line_list = line.split(" ");
				if(line_list.length != 4)
					throw new Exception("edge line parameters number mismatch!");
				if(line_list[0].equals("e") == false)
					throw new Exception("edge line does not start with 'e'");
				int start = Integer.parseInt(line_list[1]);
				int end = Integer.parseInt(line_list[2]);
				if(start != end)
				{
					graph.get(start).add(end);
					graph.get(end).add(start);
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			OwnMethods.Print("line: " + line);
			// TODO: handle exception
		}
	}
	
	/**
	 * sort adjacent list of each node by label (ascend) and degree(descend)
	 */
	public void AdjacentListSort()
	{
		for(ArrayList<Integer> neighbors : graph)
		{
			Collections.sort(neighbors, new Comparator<Integer>() {

				public int compare(Integer o1, Integer o2) {
					if(label_list[o1] == label_list[o2])
						return Utility.Comparator(graph.get(o2).size(), graph.get(o1).size());
					else
						return Utility.Comparator(label_list[o1], label_list[o2]);
				}
			});
		}
	}
	
	
	
	/**
	 * calculate cardinality of each label
	 * HashMap<Integer, Integer> label_cardinality
	 */
	public void Calculate_Label_Cardinality()
	{
		label_cardinality = new HashMap<Integer, Integer>();
		for(int i = 0; i < label_list.length; i++)
		{
			int label = label_list[i];
			if(label_cardinality.containsKey(label))
				label_cardinality.put(label, label_cardinality.get(label) + 1);
			else
				label_cardinality.put(label, 1);
		}
	}
	
//	public void Get_label_degree_to_node()
//	{
//		ArrayList<Integer> label_degree_to_node = new ArrayList<Integer>(Collections.nCopies(cnt_node, 0));
//		for (int i = 0; i < cnt_node; i++)
//			label_degree_to_node.set(i, i);
//		Collections.sort(label_degree_to_node, new Comparator<Integer>() {
//
//			public int compare(Integer o1, Integer o2) {
//				if(label_list[o1.intValue()] == label_list[o2.intValue()])
//					return Utility.Comparator(graph.get(o1.intValue()).size(), graph.get(o2.intValue()).size());
//				else
//					return Utility.Comparator(label_list[o1.intValue()], label_list[o2.intValue()]);
//			}
//			
//		});
//	}
	
	
	/**
	 * calculate label_degree_nodes (ArrayList<Label_Degree_Node>(cnt_node)) and
	 * label_deg_label_pos (ArrayList<Integer>(label_cardinality.size() + 1))
	 */
	public void Calculate_Label_Degree_Nodes()
	{
		try
		{
			label_degree_nodes = new ArrayList<Label_Degree_Node>(cnt_node);
			for(int i = 0; i < cnt_node; i++)
				label_degree_nodes.add(new Label_Degree_Node(i, label_list[i], graph.get(i).size()));
			Collections.sort(label_degree_nodes, new Comparator<Label_Degree_Node>(){

				public int compare(Label_Degree_Node o1, Label_Degree_Node o2) {
					if(o1.label == o2.label)
						return Utility.Comparator(o1.degree, o2.degree);
					else
						return Utility.Comparator(o1.label, o2.label);
				}
				
			});
			
			if(label_cardinality == null)
					throw new Exception("label_cardinality not initialized!");
			
			label_deg_label_pos = new ArrayList<Integer>(label_cardinality.size() + 1);
			label_deg_label_pos.add(-1);
			int current_label = label_degree_nodes.get(0).label;
			for (int i = 1; i<cnt_node; i++)
			{
				if(current_label == label_degree_nodes.get(i).label)
					continue;
				else
				{
					label_deg_label_pos.add(i - 1);
					current_label = label_degree_nodes.get(i).label;
				}
			}
			label_deg_label_pos.add(cnt_node - 1);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * calculate core_number_data (new int[cnt_node])
	 */
	public void Calculate_Core_Number_Data()
	{
		MAX_DEGREE = 0;
		for(int i = 0; i < cnt_node; i++)
			if(MAX_DEGREE < graph.get(i).size())
				MAX_DEGREE = graph.get(i).size();
		
		core_number_data = new int[cnt_node];
		for(int i = 0; i < cnt_node; i++)
			core_number_data[i] = graph.get(i).size();
		
		int[] bin = new int[MAX_DEGREE + 1];

		for (int i = 0; i < cnt_node; i ++)
			bin[ core_number_data [i] ] ++;

		int start = 0;

		int num;

		for (int d = 0; d <= MAX_DEGREE; d++){
			num = bin[d];
			bin[d] = start;
			start += num;
		}

		int[] pos = new int[cnt_node];
		int[] vert = new int[cnt_node];

		for (int i = 0; i < cnt_node; i++){
			pos[i] = bin[ core_number_data[i] ];
			vert[ pos[i] ] = i;
			bin[ core_number_data[i] ] ++;
		}

		for (int d = MAX_DEGREE; d > 0; d --)
			bin[d] = bin[d-1];
		bin[0] = 0;

		for (int i = 0; i < cnt_node; i++){

			int v = vert[i];

			for (int j = 0; j < graph.get(v).size(); j++){//??not sure whether each adjacent list should be sorted

				int u = graph.get(v).get(j);

				if (core_number_data[u] > core_number_data[v]){

					int du = core_number_data[u];
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
					core_number_data[u]--;
				}
			}
		}
	}

	/**
	 * calculate MAX_NB_degree_data (new int[cnt_node])
	 */
	public void Calculate_MAX_NB_degree_data()
	{
		MAX_NB_degree_data = new int[cnt_node];
		for (int i = 0; i < cnt_node; i++)
		{
			int max_degree = 0;
			for (int j = 0; j < graph.get(i).size(); j++){
				int node = graph.get(i).get(j);
				int degree = graph.get(node).size();
				if (degree > max_degree)
					max_degree = degree;
			}
			MAX_NB_degree_data[i] = max_degree;
		}
	}
	
	/**
	 * 
	 */
	public void Calculate_nodes_to_label_info()
	{
		try {
			if(adjacentlist_sorted == false)
				throw new Exception("adjacent list not sorted!");
			nodes_to_label_info = new Pair[cnt_node * (label_cardinality.size() + 1)];
			for(int i = 0; i < cnt_node; i++)
				nodes_to_label_info[i] = new Pair<Integer, Integer>(0, 0);
			
			int sum_degree = 0;
			for( ArrayList<Integer> neighbor_list : graph)
				sum_degree += neighbor_list.size();
			
			nodes_data = new int[sum_degree];

			int cur_array_index = 0;
			int cur_matrix_index = 0;
			for( int i = 0; i < cnt_node; i++)
			{
				ArrayList<Integer> neighbors_list = graph.get(i);
				
				if (neighbors_list.size() == 0){  //deal with isolated nodes, specially for yeast
//					cerr << i <<  " => degree zero node!!" << endl;
//					nodes_info[i] = cur_array_index;
					continue;
				}
//				nodes_info[i] = cur_array_index; // indicate the starting position of node i's adjacent list in "nodes_data"
//				sort(nodes[i].begin(), nodes[i].end(), sortByLabelAndDegree);//sort by label and then dascending order of degree
//				copy (nodes[i].begin(), nodes[i].end(), nodes_data + cur_array_index  );
				
				for(int j = 0; j < neighbors_list.size(); j++)
					nodes_data[cur_array_index + j] = neighbors_list.get(j);
				
				cur_array_index += neighbors_list.size(); // maintain the index
				
				int cur_label = label_list[ neighbors_list.get(0)];
				int cur_count = 1;

				if (neighbors_list.size() == 1) { //special case: there is only one node in the adjacent list
					nodes_to_label_info[i * (label_cardinality.size() + 1) + cur_label] = new Pair<Integer, Integer>(cur_matrix_index, cur_count);
					cur_matrix_index += cur_count;
					continue;
				}

				for (int j = 1; j < neighbors_list.size(); j++) {

					int this_label = label_list[neighbors_list.get(j)];

					if (this_label == cur_label)
						cur_count++;
					else {
						nodes_to_label_info[i * (label_cardinality.size() + 1) + cur_label] = new Pair<Integer, Integer>(cur_matrix_index, cur_count);
						cur_matrix_index += cur_count;
						cur_label = this_label;
						cur_count = 1;
					}

				}

				nodes_to_label_info[i * (label_cardinality.size() + 1) + cur_label] = new Pair<Integer, Integer>(cur_matrix_index, cur_count);
				cur_matrix_index += cur_count;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void Calculate_degree_data()
	{
		degree_data = new int[cnt_node];
		for (int i = 0; i < graph.size(); i++)
			degree_data[i] = graph.get(i).size();
	}
}
