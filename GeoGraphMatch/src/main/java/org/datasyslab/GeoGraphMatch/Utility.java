package org.datasyslab.GeoGraphMatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import javax.management.Query;

public class Utility {

	public static ArrayList<Query_Graph> ReadQueryGraphs(String query_path)
	{
		ArrayList<Query_Graph> query_Graphs = new ArrayList<Query_Graph>();
		BufferedReader reader = null;
		String line = null;
		try 
		{
			reader = new BufferedReader(new FileReader(new File(query_path)));
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
					int node_label = Integer.parseInt(line_list[1]);
					int degree = Integer.parseInt(line_list[2]);
					
					query_Graph.label_list.add(node_label);
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
