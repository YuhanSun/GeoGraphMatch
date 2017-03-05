package org.datasyslab.GeoGraphMatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.Buffer;
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
	
	/**
	 * load graph, entity, hrmbr (is stored with arraylist) with my format
	 * @param graphfile_path
	 * @param entity_path
	 * @param hmbr_path
	 * @param db_path
	 */
	public static void LoadGraph_backup(String graphfile_path, String entity_path, String hmbr_path, String db_path)
	{
		BufferedReader reader_graph = null;
		BufferedReader reader_entity = null;
		BufferedReader reader_hmbr = null;
		BatchInserter inserter = null;
		String line_graph = null;
		String line_entity = null;
		String line_hmbr = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "10g");
		
		Config p_Config = new Config();
		String lon_name = p_Config.GetLongitudePropertyName();
		String lat_name = p_Config.GetLatitudePropertyName();
		
		try
		{
			inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
			reader_graph = new BufferedReader(new FileReader(new File(graphfile_path)));
			reader_entity = new BufferedReader(new FileReader(new File(entity_path)));
			reader_hmbr = new BufferedReader(new FileReader(new File(hmbr_path)));
			line_graph = reader_graph.readLine();
			line_entity = reader_entity.readLine();
			line_hmbr = reader_hmbr.readLine();
			
			String [] list_hmbr = line_hmbr.split(","); 
			int node_count = Integer.parseInt(list_hmbr[0]);
			if(node_count != Integer.parseInt(line_entity) || node_count != Integer.parseInt(line_entity))
				throw new Exception(String.format("node count mismatch in: %s\n%s\n%s\n", graphfile_path, entity_path, hmbr_path));
			
			int hop_num = Integer.parseInt(list_hmbr[1]);
			
			for(int i = 0; i<node_count; i++)//read nodes and labels
			{
				line_entity = reader_entity.readLine();
				String [] list_entity = line_entity.split(",");
				
				line_hmbr = reader_hmbr.readLine();
				list_hmbr = line_hmbr.split(";");
				
				Map<String, Object> properties = new HashMap<String, Object>();
				
				ArrayList<String> hmbr = new ArrayList<String>(hop_num);
				for ( int j = 0; j < hop_num; j++)
				{
					String start = list_hmbr[j].substring(0, 2);
					
					if(start.equals("0"))
						hmbr.add(null);
					else
						hmbr.add(list_hmbr[j].substring(2, list_hmbr[j].length()));
				}
				properties.put("hmbr", hmbr.toString());
				
				int isspatial = Integer.parseInt(list_entity[1]);
				if(isspatial == 1)
				{
					double lon = Double.parseDouble(list_entity[2]);
					double lat = Double.parseDouble(list_entity[3]);
					properties.put(lon_name, lon);
					properties.put(lat_name, lat);
					Label node_label = DynamicLabel.label("GRAPH_1");
					inserter.createNode(i, properties, node_label);
				}
				else
				{
					Label node_label = DynamicLabel.label("GRAPH_0");
					inserter.createNode(i, properties, node_label);
				}
			}
			
			RelationshipType edge_label = DynamicRelationshipType.withName("LINK");
			for(int i = 0; i < node_count; i++)//read edges
			{
				line_graph = reader_graph.readLine();
				String [] list_graph = line_graph.split(",");
				int start = Integer.parseInt(list_graph[0]);
				
				if(i != start)
				{
					OwnMethods.Print(line_graph);
					throw new Exception(String.format("node index inconsistent with line index in %s", graphfile_path));
				}
				
				int neighbor_count = Integer.parseInt(list_graph[1]);
				
				for ( int j = 0; j < neighbor_count; j++)
				{
					int end = Integer.parseInt(list_graph[j+2]);
					if(start < end)
						inserter.createRelationship(start, end, edge_label, null);
				}
			}
			reader_graph.close();
			reader_entity.close();
			reader_hmbr.close();
			inserter.shutdown();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * load graph, entity, hrmbr (each stored separately) with my format
	 * @param graphfile_path
	 * @param entity_path
	 * @param hmbr_path
	 * @param db_path
	 */
	public static void LoadGraph(String graphfile_path, String entity_path, String hmbr_path, String db_path)
	{
		BufferedReader reader_graph = null;
		BufferedReader reader_entity = null;
		BufferedReader reader_hmbr = null;
		BatchInserter inserter = null;
		String line_graph = null;
		String line_entity = null;
		String line_hmbr = null;
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "10g");
		
		Config p_Config = new Config();
		String lon_name = p_Config.GetLongitudePropertyName();
		String lat_name = p_Config.GetLatitudePropertyName();
		String[] rect_corner_name = p_Config.GetRectCornerName();
		String minx_name = rect_corner_name[0];
		String miny_name = rect_corner_name[1];
		String maxx_name = rect_corner_name[2];
		String maxy_name = rect_corner_name[3];
		
		try
		{
			inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(),config);
			reader_graph = new BufferedReader(new FileReader(new File(graphfile_path)));
			reader_entity = new BufferedReader(new FileReader(new File(entity_path)));
			reader_hmbr = new BufferedReader(new FileReader(new File(hmbr_path)));
			line_graph = reader_graph.readLine();
			line_entity = reader_entity.readLine();
			line_hmbr = reader_hmbr.readLine();
			
			String [] list_hmbr = line_hmbr.split(","); 
			int node_count = Integer.parseInt(list_hmbr[0]);
			if(node_count != Integer.parseInt(line_entity) || node_count != Integer.parseInt(line_entity))
				throw new Exception(String.format("node count mismatch in: %s\n%s\n%s\n", graphfile_path, entity_path, hmbr_path));
			
			int hop_num = Integer.parseInt(list_hmbr[1]);
			
			for(int i = 0; i<node_count; i++)//read nodes and labels
			{
				line_entity = reader_entity.readLine();
				String [] list_entity = line_entity.split(",");
				
				line_hmbr = reader_hmbr.readLine();
				list_hmbr = line_hmbr.split(";");
				
				Map<String, Object> properties = new HashMap<String, Object>();
				
				for ( int j = 0; j < hop_num; j++)
				{
					String start = list_hmbr[j].substring(0, 2);
					
					if(start.equals("0") == false)
					{
						String rect = list_hmbr[j].substring(3, list_hmbr[j].length()-1);
						String[] liString = rect.split(",");
						double minx = Double.parseDouble(liString[0]);
						double miny = Double.parseDouble(liString[1]);
						double maxx = Double.parseDouble(liString[2]);
						double maxy = Double.parseDouble(liString[3]);
						properties.put(String.format("HMBR_%d_%s", j + 1, minx_name), minx);
						properties.put(String.format("HMBR_%d_%s", j + 1, miny_name), miny);
						properties.put(String.format("HMBR_%d_%s", j + 1, maxx_name), maxx);
						properties.put(String.format("HMBR_%d_%s", j + 1, maxy_name), maxy);
					}
				}
				
				int isspatial = Integer.parseInt(list_entity[1]);
				if(isspatial == 1)
				{
					double lon = Double.parseDouble(list_entity[2]);
					double lat = Double.parseDouble(list_entity[3]);
					properties.put(lon_name, lon);
					properties.put(lat_name, lat);
					Label node_label = DynamicLabel.label("GRAPH_1");
					inserter.createNode(i, properties, node_label);
				}
				else
				{
					Label node_label = DynamicLabel.label("GRAPH_0");
					inserter.createNode(i, properties, node_label);
				}
			}
			
			RelationshipType edge_label = DynamicRelationshipType.withName("LINK");
			for(int i = 0; i < node_count; i++)//read edges
			{
				line_graph = reader_graph.readLine();
				String [] list_graph = line_graph.split(",");
				int start = Integer.parseInt(list_graph[0]);
				
				if(i != start)
				{
					OwnMethods.Print(line_graph);
					throw new Exception(String.format("node index inconsistent with line index in %s", graphfile_path));
				}
				
				int neighbor_count = Integer.parseInt(list_graph[1]);
				
				for ( int j = 0; j < neighbor_count; j++)
				{
					int end = Integer.parseInt(list_graph[j+2]);
					if(start < end)
						inserter.createRelationship(start, end, edge_label, null);
				}
			}
			reader_graph.close();
			reader_entity.close();
			reader_hmbr.close();
			inserter.shutdown();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * load CFL format graph without spatial
	 * @param graphfile_path
	 * @param db_path
	 * @param transfer_table
	 */
	public static void LoadGraphCFL(String graphfile_path, String db_path, HashMap<Integer, Integer> transfer_table)
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
	
	/**
	 * load graph with my format
	 */
	public static void LoadGraph()
	{
		String graphfile_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/graph.txt";
		String entity_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/entity.txt";
		String hmbr_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/HMBR.txt";
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_"+dataset+"/data/graph.db";
		LoadGraph(graphfile_path, entity_path, hmbr_path, db_path);
	}

	/**
	 * load graph with cfl format
	 */
	public static void LoadGraphCFL()
	{
		String data_name = "hprd";
		String graphfile_path = "/home/yuhansun/Documents/GeoGraphMatchData/" + data_name;
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_"+data_name+"/data/graph.db";
		String transfer_table_path = "/home/yuhansun/Documents/GeoGraphMatchData/transfertable_"+data_name+".txt";
		HashMap<Integer, Integer> transfer_table = Utility.Read_Transfer_Table(transfer_table_path);
		LoadGraphCFL(graphfile_path, db_path, transfer_table);
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
	
	static String dataset = "foursquare";
	
	public static void main(String[] args) {
		LoadGraph();
//		LoadQueryGraph();
	}

}
