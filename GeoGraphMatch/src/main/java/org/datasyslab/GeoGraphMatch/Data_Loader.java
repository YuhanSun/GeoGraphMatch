package org.datasyslab.GeoGraphMatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
	public static void LoadGraph(String graphfile_path, String db_path)
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
				}
				int node_id = Integer.parseInt(line_list[1]);
				int label = Integer.parseInt(line_list[2]);
				Label node_label = DynamicLabel.label("GRAPH_"+label);
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
				}
				int start = Integer.parseInt(line_list[1]);
				int end = Integer.parseInt(line_list[2]);
				inserter.createRelationship(start, end, edge_label, null);
			}
			reader.close();
			inserter.shutdown();
		}
		catch(Exception e)
		{
			OwnMethods.Print(line);
			e.printStackTrace();
		}
		finally {
			if(reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if(inserter != null)
				inserter.shutdown();
		}
	}

	public static void LoadGraph()
	{
		String graphfile_path = "/home/yuhansun/Documents/GeoGraphMatchData/hprd";
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_hprd/data/graph.db";
		LoadGraph(graphfile_path, db_path);
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LoadGraph();
	}

}
