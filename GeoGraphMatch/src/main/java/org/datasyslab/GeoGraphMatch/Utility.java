package org.datasyslab.GeoGraphMatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class Utility {
	
	
	public static int Comparator(int a, int b)
	{
		if (a < b) 
			return -1;
		else
			if(a > b)
				return 1;
			else 
				return 0;
	}
	
	public static int lower_bound(ArrayList<Label_Degree_Node> label_degree_nodes, int low, int high, int value)
	{
	    if ( low < 0) return 0;
	    if (low>=high )
	    {
	      if ( value <= label_degree_nodes.get(low).degree ) return low;
	      return low+1;
	    }
	    int mid=(low+high)/2;
	    if ( value> label_degree_nodes.get(mid).degree)
	        return lower_bound(label_degree_nodes, mid+1,high,value);
	    return lower_bound(label_degree_nodes, low, mid, value);
	}
	
	/**
	 * read transfer table from a file
	 * @param table_path
	 * @return
	 */
	public static HashMap<Integer, Integer> Read_Transfer_Table(String table_path)
	{
		HashMap<Integer, Integer> transfer_table = new HashMap<Integer, Integer>();
		BufferedReader reader = null;
		String line = null;
		try {
			reader = new BufferedReader(new FileReader(new File(table_path)));
			while((line = reader.readLine())!=null)
			{
				String [] line_list = line.split("\t");
				int ori_label = Integer.parseInt(line_list[0]);
				int transfer_label = Integer.parseInt(line_list[1]);
				if(transfer_label == 0)
					continue;
				transfer_table.put(ori_label, transfer_label);
			}
			reader.close();
		} catch (Exception e) {
			OwnMethods.Print(line);
			e.printStackTrace();
			return null;
		}
		return transfer_table; 
	}
	
	/**
	 * not used now
	 * @param datagraph_path
	 * @return
	 */
	public static HashMap<Integer, Integer> Preprocess_DataGraph(String datagraph_path)
	{
		BufferedReader reader = null;
		String line = null;
		HashMap<Integer, Integer> label_cardinality = new HashMap<Integer, Integer>();
		try {
			reader = new BufferedReader(new FileReader(new File(datagraph_path)));
			line = reader.readLine();
			String [] line_list = line.split(" ");
			if(line_list.length != 3)
			{
				OwnMethods.Print(String.format("%s first line parameters number mismatch!", datagraph_path));
				return null;
			}
			if(line_list[0].equals("t") == false)
			{
				OwnMethods.Print(String.format("%s format no 't' at the beginning!", datagraph_path));
				return null;
			}
			int node_count = Integer.parseInt(line_list[2]);//first line contains total number of nodes
			for(int i = 0; i<node_count; i++)
			{
				line = reader.readLine();
				line_list = line.split(" ");
				if(line_list.length != 3)
				{
					OwnMethods.Print(String.format("node line parameters number mismatch!"));
					return null;
				}
				if(line_list[0].equals("v") == false)
				{
					OwnMethods.Print("node line does not start with 'v'");
				}
				int label = Integer.parseInt(line_list[2]);
				if(label_cardinality.containsKey(label))
					label_cardinality.put(label, label_cardinality.get(label) + 1);
				else
					label_cardinality.put(label, 1);
			}
			reader.close();
		} catch (Exception e) {
			OwnMethods.Print(line);
			e.printStackTrace();
			if(reader!=null)
				try {
					reader.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
		}
		return label_cardinality;
	}
	
	/**
	 * read query graphs from a file with specific number no transfer table
	 * @param querygraph_path
	 * @param read_count
	 * @return
	 */
	public static ArrayList<Query_Graph> ReadQueryGraphs(String querygraph_path, int read_count)
	{
		ArrayList<Query_Graph> query_Graphs = new ArrayList<Query_Graph>();
		BufferedReader reader = null;
		String line = null;
		try 
		{
			reader = new BufferedReader(new FileReader(new File(querygraph_path)));
			for(int current_read_count = 0; current_read_count < read_count; current_read_count++)
			{
				line = reader.readLine();
				String [] line_list = line.split(" ");
				if(line_list.length != 4)
				{
					OwnMethods.Print(String.format("query graph first line parameters number mismatch!"));
					return null;
				}
				if(line_list[0].equals("t") == false)
				{
					OwnMethods.Print("query graph first line does begin with 't'!");
					return null;
				}
				int node_count = Integer.parseInt(line_list[2]);
				int edge_count = Integer.parseInt(line_list[3]);
				Query_Graph query_Graph = new Query_Graph(node_count);
				for(int i = 0; i<node_count; i++)
				{
					line = reader.readLine();
					line_list = line.split(" ");
					int node_id = Integer.parseInt(line_list[0]);
					if(node_id != i)
					{
						OwnMethods.Print(String .format("node_id not consistent with line index at %d", i));
						OwnMethods.Print(line);
						return null;
					}
					
					int node_label = Integer.parseInt(line_list[1]);
					int degree = Integer.parseInt(line_list[2]);
					
					query_Graph.label_list[i] = node_label;
					ArrayList<Integer> neighbors = new ArrayList<Integer>(degree);
					for(int j = 0; j<degree; j++)
					{
						int neighbor_id = Integer.parseInt(line_list[j+3]);
						neighbors.add(neighbor_id);
					}
					query_Graph.graph.add(neighbors);
				}
				query_Graphs.add(query_Graph);
			}
			reader.close();
		}
		catch (Exception e) {
			OwnMethods.Print(line);
			e.printStackTrace();
		}
		return query_Graphs;
	}
	
	/**
	 * read query graphs from a file with specific number
	 * @param querygraph_path
	 * @param transfer_table
	 * @param read_count
	 * @return
	 */
	public static ArrayList<Query_Graph> ReadQueryGraphs(String querygraph_path, HashMap<Integer, Integer> transfer_table, int read_count)
	{
		ArrayList<Query_Graph> query_Graphs = new ArrayList<Query_Graph>();
		BufferedReader reader = null;
		String line = null;
		try 
		{
			reader = new BufferedReader(new FileReader(new File(querygraph_path)));
			for(int current_read_count = 0; current_read_count < read_count; current_read_count++)
			{
				line = reader.readLine();
				String [] line_list = line.split(" ");
				if(line_list.length != 4)
				{
					OwnMethods.Print(String.format("query graph first line parameters number mismatch!"));
					return null;
				}
				if(line_list[0].equals("t") == false)
				{
					OwnMethods.Print("query graph first line does begin with 't'!");
					return null;
				}
				int node_count = Integer.parseInt(line_list[2]);
				int edge_count = Integer.parseInt(line_list[3]);
				Query_Graph query_Graph = new Query_Graph(node_count);
				for(int i = 0; i<node_count; i++)
				{
					line = reader.readLine();
					line_list = line.split(" ");
					int node_id = Integer.parseInt(line_list[0]);
					if(node_id != i)
					{
						OwnMethods.Print(String .format("node_id not consistent with line index at %d", i));
						OwnMethods.Print(line);
						return null;
					}
					
					int node_label = Integer.parseInt(line_list[1]);
					int transfer_label = transfer_table.get(node_label);
					int degree = Integer.parseInt(line_list[2]);
					
					query_Graph.label_list[i] = transfer_label;
					ArrayList<Integer> neighbors = new ArrayList<Integer>(degree);
					for(int j = 0; j<degree; j++)
					{
						int neighbor_id = Integer.parseInt(line_list[j+3]);
						neighbors.add(neighbor_id);
					}
					query_Graph.graph.add(neighbors);
				}
				query_Graphs.add(query_Graph);
			}
			reader.close();
		}
		catch (Exception e) {
			OwnMethods.Print(line);
			e.printStackTrace();
		}
		return query_Graphs;
	}

	/**
	 * read all query graphs from a file
	 * @param querygraph_path
	 * @param transfer_table
	 * @return
	 */
	public static ArrayList<Query_Graph> ReadQueryGraphs(String querygraph_path, HashMap<Integer, Integer> transfer_table)
	{
		ArrayList<Query_Graph> query_Graphs = new ArrayList<Query_Graph>();
		BufferedReader reader = null;
		String line = null;
		try 
		{
			reader = new BufferedReader(new FileReader(new File(querygraph_path)));
			while((line = reader.readLine()) != null)
			{
				String [] line_list = line.split(" ");
				if(line_list.length != 4)
				{
					OwnMethods.Print(String.format("query graph first line parameters number mismatch!"));
					return null;
				}
				if(line_list[0].equals("t") == false)
				{
					OwnMethods.Print("query graph first line does begin with 't'!");
					return null;
				}
				int node_count = Integer.parseInt(line_list[2]);
				int edge_count = Integer.parseInt(line_list[3]);
				Query_Graph query_Graph = new Query_Graph(node_count);
				for(int i = 0; i<node_count; i++)
				{
					line = reader.readLine();
					line_list = line.split(" ");
					int node_id = Integer.parseInt(line_list[0]);
					if(node_id != i)
					{
						OwnMethods.Print(String .format("node_id not consistent with line index at %d", i));
						OwnMethods.Print(line);
						return null;
					}
					
					int node_label = Integer.parseInt(line_list[1]);
					int transfer_label = transfer_table.get(node_label);
					int degree = Integer.parseInt(line_list[2]);
					
					query_Graph.label_list[i] = transfer_label;
					ArrayList<Integer> neighbors = new ArrayList<Integer>(degree);
					for(int j = 0; j<degree; j++)
					{
						int neighbor_id = Integer.parseInt(line_list[j+3]);
						neighbors.add(neighbor_id);
					}
					query_Graph.graph.add(neighbors);
				}
				query_Graphs.add(query_Graph);
			}
		}
		catch (Exception e) {
			OwnMethods.Print(line);
			e.printStackTrace();
		}
		return query_Graphs;
	}
}
