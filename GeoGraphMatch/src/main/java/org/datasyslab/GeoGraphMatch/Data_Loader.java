package org.datasyslab.GeoGraphMatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class Data_Loader {
	
	//load graph without spatial info
	public static void LoadGraph(String graphfile_path, String db_path, HashMap<Integer, Integer> transfer_table)
	{
		BufferedReader reader = null;
		BatchInserter inserter = null;
		String line = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "10g");
		
		try
		{
			inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
			reader = new BufferedReader(new FileReader(new File(graphfile_path)));
			line = reader.readLine();
			String [] line_list = line.split(" ");
			if(line_list.length != 3)
			{
				OwnMethods.Print(String.format("%s first line parameters number mismatch!", graphfile_path));
				return;
			}
			if(line_list[0].equals("t") == false)
			{
				OwnMethods.Print(String.format("%s format no 't' at the beginning!", graphfile_path));
				return;
			}
			int node_count = Integer.parseInt(line_list[2]);//first line contains total number of nodes
			
			for(int i = 0; i<node_count; i++)//read nodes and labels
			{
				line = reader.readLine();
				line_list = line.split(" ");
				if(line_list.length != 3)
				{
					OwnMethods.Print(String.format("node line parameters number mismatch!"));
					return;
				}
				if(line_list[0].equals("v") == false)
				{
					OwnMethods.Print("node line does not start with 'v'");
					return;
				}
				int node_id = Integer.parseInt(line_list[1]);
				int label = Integer.parseInt(line_list[2]);
				
				Object object = transfer_table.get(label);
				if(object == null)
					throw new Exception("transfer table does not contain label!");
				int transfer_label = (Integer)object;
				
				Label node_label = DynamicLabel.label("GRAPH_"+transfer_label);
				inserter.createNode(node_id, null, node_label);
			}
			
			RelationshipType edge_label = DynamicRelationshipType.withName("LINK");
			while((line = reader.readLine())!=null)//read edges
			{
				line_list = line.split(" ");
				if(line_list.length != 4)
				{
					OwnMethods.Print(String.format("edge line parameters number mismatch!"));
					return;
				}
				if(line_list[0].equals("e") == false)
				{
					OwnMethods.Print("edge line does not start with 'e'");
					return;
				}
				int start = Integer.parseInt(line_list[1]);
				int end = Integer.parseInt(line_list[2]);
				if(start !=end)
					inserter.createRelationship(start, end, edge_label, null);
			}
			reader.close();
			inserter.shutdown();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			OwnMethods.Print(line);
		}
		finally {
			if(reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
	
	public static void LoadQueryGraph(ArrayList<Query_Graph> query_Graphs, String db_path)
	{
		BatchInserter inserter = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "10g");
		
		try
		{
			inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
			for(int i = 0; i < query_Graphs.size(); i++)
			{
				Query_Graph query_Graph = query_Graphs.get(i);
				int node_count = query_Graph.label_list.length;
				int[] id_in_neo4j = new int[node_count];
				int current_id = 0;
				Label node_label = DynamicLabel.label("QUERY_"+i);
				for(int label : query_Graph.label_list)
				{
					Map<String, Object> properties = new HashMap<String, Object>();
					properties.put("id", current_id);
					properties.put("label", label);
					id_in_neo4j[current_id] = (int) inserter.createNode(properties, node_label);
					current_id++;
				}
				
				current_id = 0;
				RelationshipType edge_label = DynamicRelationshipType.withName("LINK");
				for(ArrayList<Integer> neighbors : query_Graph.graph)
				{
					for(int neighbor : neighbors)
						if(current_id < neighbor)
							inserter.createRelationship(id_in_neo4j[current_id], id_in_neo4j[neighbor], edge_label, null);
					current_id++;
				}
			}
			
			inserter.shutdown();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void LoadGraph()
	{
		String data_name = "hprd";
		String graphfile_path = "/home/yuhansun/Documents/GeoGraphMatchData/" + data_name;
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_"+data_name+"/data/graph.db";
		String transfer_table_path = "/home/yuhansun/Documents/GeoGraphMatchData/transfertable_"+data_name+".txt";
		HashMap<Integer, Integer> transfer_table = Utility.Read_Transfer_Table(transfer_table_path);
		LoadGraph(graphfile_path, db_path, transfer_table);
	}
	
	public static void LoadQueryGraph()
	{
		String data_name = "hprd";
		String querygraph_path = "/home/yuhansun/Documents/GeoGraphMatchData/hprd25d";
		String transfer_table_path = "/home/yuhansun/Documents/GeoGraphMatchData/transfertable_"+data_name+".txt";
		HashMap<Integer, Integer> transfer_table = Utility.Read_Transfer_Table(transfer_table_path);
		ArrayList<Query_Graph> query_Graphs  =  Utility.ReadQueryGraphs(querygraph_path, transfer_table);
		
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_"+data_name+"/data/graph.db";
		LoadQueryGraph(query_Graphs, db_path);
	}
	
	public static void main(String[] args) {
//		LoadGraph();
		LoadQueryGraph();
	}

}
