package org.datasyslab.GeoGraphMatch;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import javax.management.Query;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.register.Register.Int;
import org.omg.CORBA.PUBLIC_MEMBER;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class Minhop_Match {
	
	public static boolean TIME_RECORD = true;
	
	public Neo4j_Graph_Store p_neo;
	public String lon_name;
	public String lat_name;
	
	public int query_node_count;
	
	public int[] neo4j_time;
	public int[] hmbr_check_time;
	public int[] spa_check_time;
	public long start;
	
	public Minhop_Match()
	{
		p_neo = new Neo4j_Graph_Store();
		Config config = new Config();
		lon_name = config.GetLongitudePropertyName();
		lat_name = config.GetLatitudePropertyName();
	}
	
	public static void test()
	{
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/path.txt";
		ArrayList<Query_Graph> query_Graphs = Utility.ReadQueryGraphs(querygraph_path, 2);
		
		Query_Graph query_Graph = query_Graphs.get(0);
		query_Graph.spa_predicate = new MyRectangle[2];
//		query_Graph.spa_predicate[2] = new MyRectangle(114.735827,16.403898,115.975189,17.643261);
		query_Graph.spa_predicate[1] = new MyRectangle(-80.438853, 34.927905,-76.843579, 36.670577);
		
		Minhop_Match minhop_Match = new Minhop_Match();
		long start = System.currentTimeMillis();
		ArrayList<int[]> maps = minhop_Match.SubgraphMatch_Spa(query_Graph, 100000);
		OwnMethods.Print(System.currentTimeMillis() - start);
		
		OwnMethods.Print(maps.size());
		
		return;
	}
	
	public static void GetQueryPlanTest()
	{
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/path.txt";
		ArrayList<Query_Graph> query_Graphs = Utility.ReadQueryGraphs(querygraph_path, 2);
		Query_Graph query_Graph = query_Graphs.get(0);
		
		query_Graph.spa_predicate = new MyRectangle[2];
		query_Graph.spa_predicate[1] = new MyRectangle(-80.438853, 34.927905,-76.843579, 36.670577);
		
		Config config = new Config();
		String lon_name = config.GetLongitudePropertyName();	String lat_name = config.GetLatitudePropertyName();
		String query = Utility.FormCypherQuery(query_Graph, lon_name, lat_name);
		query += "limit 100";
		OwnMethods.Print(query);
		
		String dbpath = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_Yelp/data/graph.db";
		Neo4j_API neo4j_API = new Neo4j_API(dbpath);
		Transaction tx = neo4j_API.graphDb.beginTx();
		try {
			Result result = neo4j_API.graphDb.execute(query);
			ExecutionPlanDescription plan = result.getExecutionPlanDescription();
			OwnMethods.Print(plan.toString());
			
			result.close();
			neo4j_API.ShutDown();
			
			tx.success();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			tx.close();
		}
	}

	public static void main(String[] args) {
		
//		test();
		GetQueryPlanTest();

	}
	
	public ArrayList<int[]> SubgraphMatch_Spa(Query_Graph query_Graph, int limit)
	{
		query_node_count = query_Graph.graph.size();
		neo4j_time = new int[query_node_count];
		hmbr_check_time = new int[query_node_count];
		spa_check_time = new int[query_node_count];

		int [][] minhop_index = Ini_Minhop(query_Graph);
		MyRectangle query_rect = null;
		
		for ( MyRectangle rect : query_Graph.spa_predicate)
			if(rect != null)
				query_rect = rect;
				
		ArrayList<int[]> maps = new ArrayList<int[]>(limit);

		int[] match_sequence = {0,1};
		int[] map_count = {0};

		int id1 = match_sequence[0];
		int id2 = match_sequence[1];
		String query = String.format("match (a0:GRAPH_%d) return id(a0),a0", query_Graph.label_list[id1]);

		if(TIME_RECORD)
			start = System.currentTimeMillis();
		String result = p_neo.Execute(query);
		if(TIME_RECORD)
			neo4j_time[0] += System.currentTimeMillis() - start;

		if(TIME_RECORD)
			start = System.currentTimeMillis();
		JsonArray jsonArray = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		for ( JsonElement jsonElement : jsonArray )
		{
			
			JsonArray jsonrow = jsonElement.getAsJsonObject().get("row").getAsJsonArray();
			int data_id = jsonrow.get(0).getAsInt();
			
			String hmbr_str = jsonrow.get(1).getAsJsonObject().get("hmbr").getAsString();
			hmbr_str = hmbr_str.substring(1, hmbr_str.length() - 1);
			String [] str_list = hmbr_str.split(", ");
			
			int minhop_data = str_list.length - 1;
			for ( ; minhop_data >=0; minhop_data--)
			{
				MyRectangle hmbr = new MyRectangle(str_list[minhop_data]);
				if(OwnMethods.Intersect(hmbr, query_rect) == false)
					break;
			}
			if(minhop_data >= minhop_index[1][0])
				continue;
			else
			{
				 query = String.format("match (a)--(b:GRAPH_1) where id(a) = %d and %f < b.%s < %f and %f < b.%s < %f return id(b)", data_id, query_rect.min_x, lon_name, query_rect.max_x, query_rect.min_y, lat_name, query_rect.max_y);
//				 OwnMethods.Print(query);return maps;
				 result = p_neo.Execute(query);
				 JsonArray jsonArray2 = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
				 for ( int i = 0; i < jsonArray2.size(); i++)
				 {
					 int data_id2 = jsonArray.get(i).getAsJsonObject().get("row").getAsJsonArray().get(0).getAsInt();
					 maps.add(new int[]{data_id, data_id2});
					 map_count[0]+= 1;
					 if(map_count[0] == limit)
						 return maps;
				 }
			}
		}
				return maps;
	}
	
	public int[][] Ini_Minhop(Query_Graph query_Graph)
	{
		query_node_count = query_Graph.graph.size();
		int [][] minhop_index = new int[query_node_count][];
		
		for ( int i = 0; i < query_node_count; i++)
		{
			if ( query_Graph.spa_predicate[i] == null)
				minhop_index[i] = null;
			else
				minhop_index[i] = new int[query_node_count];
		}
		
		for ( int i = 0; i < query_node_count; i++)
		{
			if(query_Graph.spa_predicate[i] != null)
			{
				boolean[] visited = new boolean[query_node_count];
				visited[i] = true;
				minhop_index[i][i] = 0;
				
				Queue<Integer> queue = new LinkedList<Integer>();
				queue.add(i);
				int pre_level_count = 1;
				int cur_level_count = 0;
				int level_index = 1;
				
				while ( queue.isEmpty() == false )
				{
					for ( int j = 0; j < pre_level_count; j++)
					{
						int node = queue.poll();
						for ( int k = 0; k < query_Graph.graph.get(node).size(); k++)
						{
							int neighbor = query_Graph.graph.get(node).get(k);
							if(visited[neighbor] == false)
							{
								minhop_index[i][neighbor] = level_index;
								visited[neighbor] = true;
								cur_level_count += 1;
								queue.add(neighbor);
							}
						}
					}
					level_index ++;
					pre_level_count = cur_level_count;
					cur_level_count = 0;
				}
			}
		}
		
		return minhop_index;
	}

}
