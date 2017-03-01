package org.datasyslab.GeoGraphMatch;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import javax.management.Query;

import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters.nameAllPatternElements;
import org.neo4j.cypher.internal.compiler.v2_2.commands.indexQuery;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.register.Register.Int;
import org.omg.CORBA.PUBLIC_MEMBER;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class Minhop_Match {
	
	public static boolean TIME_RECORD = true;
	
	//neo4j connection
	public Neo4j_Graph_Store p_neo;
	public String lon_name;
	public String lat_name;
	
	//neo4j graphdb service
	public Neo4j_API neo4j_API;
	
	String minx_name;
	String miny_name;
	String maxx_name;
	String maxy_name;
	
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
		
		String[] rect_corner_name = config.GetRectCornerName();
		minx_name = rect_corner_name[0];
		miny_name = rect_corner_name[1];
		maxx_name = rect_corner_name[2];
		maxy_name = rect_corner_name[3];
	}
	
	public Minhop_Match(String db_path)
	{
		neo4j_API = new Neo4j_API(db_path);
		Config config = new Config();
		lon_name = config.GetLongitudePropertyName();
		lat_name = config.GetLatitudePropertyName();
		
		String[] rect_corner_name = config.GetRectCornerName();
		minx_name = rect_corner_name[0];
		miny_name = rect_corner_name[1];
		maxx_name = rect_corner_name[2];
		maxy_name = rect_corner_name[3];
	}
	
	public static void test() throws InterruptedException
	{
		int limit = -1;
		long start;
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
		ArrayList<Query_Graph> query_Graphs = Utility.ReadQueryGraphs(querygraph_path, 4);
		
		Query_Graph query_Graph = query_Graphs.get(0);
		query_Graph.spa_predicate = new MyRectangle[2];
//		query_Graph.spa_predicate[1] = new MyRectangle(-97.869058,32.667279,-97.515286,32.835549);
		query_Graph.spa_predicate[1] = new MyRectangle(-121.642749,38.488661,-121.288977,38.656931);
		
//		query_Graph.spa_predicate[0] = new MyRectangle(-113.737320,37.029965,-113.383548,37.198235);
//		query_Graph.spa_predicate[2] = new MyRectangle(-120.488475,47.340806,-120.134703,47.509076);
		
		
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_" + dataset;
		
//		OwnMethods.Print(OwnMethods.ClearCache((String)password));
//		OwnMethods.Print((Object)Neo4j_Graph_Store.StartServer((String)db_path));
//		Thread.currentThread();
//		Thread.sleep(2000);
		
		Minhop_Match minhop_Match = new Minhop_Match();
		minhop_Match.p_neo.Execute("match (n) where id(n) = 1");
		start = System.currentTimeMillis();
		minhop_Match.SubgraphMatch_Spa(query_Graph, limit);
		OwnMethods.Print(System.currentTimeMillis() - start);
		
//		OwnMethods.Print(Neo4j_Graph_Store.StopServer(db_path));
//		
//		OwnMethods.Print(OwnMethods.ClearCache((String)password));
//		OwnMethods.Print((Object)Neo4j_Graph_Store.StartServer((String)db_path));
//		Thread.currentThread();
//		Thread.sleep(2000);
		
		start = System.currentTimeMillis();
		Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match();
		naive_Neo4j_Match.p_neo.Execute("match (n) where id(n) = 1");
		naive_Neo4j_Match.SubgraphMatch_Spa(query_Graph, limit);
		OwnMethods.Print(System.currentTimeMillis() - start);
		
//		Minhop_Match minhop_Match = new Minhop_Match();
//		start = System.currentTimeMillis();
//		minhop_Match.SubgraphMatch_Spa(query_Graph, limit);
//		OwnMethods.Print(System.currentTimeMillis() - start);
		
//		OwnMethods.Print(Neo4j_Graph_Store.StopServer(db_path));
		
		return;
	}
	
	public static void GetQueryPlanTest()
	{
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
		ArrayList<Query_Graph> query_Graphs = Utility.ReadQueryGraphs(querygraph_path, 2);
		Query_Graph query_Graph = query_Graphs.get(1);
		
		query_Graph.spa_predicate = new MyRectangle[3];
		query_Graph.spa_predicate[2] = new MyRectangle(-80.438853, 34.927905,-76.843579, 36.670577);
		
		Config config = new Config();
		String lon_name = config.GetLongitudePropertyName();	String lat_name = config.GetLatitudePropertyName();
//		String query = Utility.FormCypherQuery(query_Graph, lon_name, lat_name);
//		query += " limit 100";
//		query = "profile " + query;
//		query = "explain " + query;
		
		String query = "profile match p = (a)--(b) where id(a) in [0,1,2,3,4,5,6] return p";
		
		OwnMethods.Print(query);
		
		String dbpath = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_Gowalla/data/graph.db";
		Neo4j_API neo4j_API = new Neo4j_API(dbpath);
		Transaction tx = neo4j_API.graphDb.beginTx();
		try {
			Result result = neo4j_API.graphDb.execute(query);
			
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
			}
			
			ExecutionPlanDescription plan = result.getExecutionPlanDescription();
			OwnMethods.Print(plan.toString());
			long page_access = OwnMethods.GetTotalDBHits(plan);
			OwnMethods.Print("Page access :" + page_access);
			
			long row_count = plan.getProfilerStatistics().getRows();
			OwnMethods.Print("Rows: " + row_count);
			
//			tx.success();
			neo4j_API.ShutDown();
			
		} catch (Exception e) {
			if(neo4j_API.graphDb.equals(null) == false)
			{
				OwnMethods.Print("Shutdown");
				neo4j_API.ShutDown();
			}
			e.printStackTrace();
		}
		finally {
		}
	}
	
	private static String password = "syh19910205";
	private static String dataset = "Gowalla";

	public static void main(String[] args) {
		
//		try {
//			test();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		GetQueryPlanTest();

	}
	
//	public ArrayList<int[]> SubgraphMatch_Spa(Query_Graph query_Graph, int limit)
//	{
//		query_node_count = query_Graph.graph.size();
//		neo4j_time = new int[query_node_count];
//		hmbr_check_time = new int[query_node_count];
//		spa_check_time = new int[query_node_count];
//
//		int [][] minhop_index = Ini_Minhop(query_Graph);
//		MyRectangle query_rect = null;
//		
//		for ( MyRectangle rect : query_Graph.spa_predicate)
//			if(rect != null)
//				query_rect = rect;
//				
//		ArrayList<int[]> maps = new ArrayList<int[]>(limit);
//
//		int[] match_sequence = {0,1};
//		int[] map_count = {0};
//
//		int id1 = match_sequence[0];
//		int id2 = match_sequence[1];
//		String query = String.format("match (a0:GRAPH_%d) return id(a0),a0", query_Graph.label_list[id1]);
//
//		if(TIME_RECORD)
//			start = System.currentTimeMillis();
//		String result = p_neo.Execute(query);
//		if(TIME_RECORD)
//			neo4j_time[0] += System.currentTimeMillis() - start;
//
//		if(TIME_RECORD)
//			start = System.currentTimeMillis();
//		JsonArray jsonArray = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
//		for ( JsonElement jsonElement : jsonArray )
//		{
//			
//			JsonArray jsonrow = jsonElement.getAsJsonObject().get("row").getAsJsonArray();
//			int data_id = jsonrow.get(0).getAsInt();
//			
//			String hmbr_str = jsonrow.get(1).getAsJsonObject().get("hmbr").getAsString();
//			hmbr_str = hmbr_str.substring(1, hmbr_str.length() - 1);
//			String [] str_list = hmbr_str.split(", ");
//			
//			int minhop_data = str_list.length - 1;
//			for ( ; minhop_data >=0; minhop_data--)
//			{
//				MyRectangle hmbr = new MyRectangle(str_list[minhop_data]);
//				if(OwnMethods.Intersect(hmbr, query_rect) == false)
//					break;
//			}
//			if(minhop_data >= minhop_index[1][0])
//				continue;
//			else
//			{
//				 query = String.format("match (a)--(b:GRAPH_1) where id(a) = %d and %f < b.%s < %f and %f < b.%s < %f return id(b)", data_id, query_rect.min_x, lon_name, query_rect.max_x, query_rect.min_y, lat_name, query_rect.max_y);
////				 OwnMethods.Print(query);return maps;
//				 result = p_neo.Execute(query);
//				 JsonArray jsonArray2 = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
//				 for ( int i = 0; i < jsonArray2.size(); i++)
//				 {
//					 int data_id2 = jsonArray.get(i).getAsJsonObject().get("row").getAsJsonArray().get(0).getAsInt();
//					 maps.add(new int[]{data_id, data_id2});
//					 map_count[0]+= 1;
//					 if(map_count[0] == limit)
//						 return maps;
//				 }
//			}
//		}
//				return maps;
//	}
	
	public JsonArray SubgraphMatch_Spa(Query_Graph query_Graph, int limit)
	{
		query_node_count = query_Graph.graph.size();
		neo4j_time = new int[query_node_count];
		hmbr_check_time = new int[query_node_count];
		spa_check_time = new int[query_node_count];

		int [][] minhop_index = Ini_Minhop(query_Graph);

		String query = FormCypherQuery(query_Graph, limit, minhop_index);
		
		OwnMethods.Print(query);
		String result = p_neo.Execute(query);
//		OwnMethods.Print(result);
		
		JsonArray jsonArray = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
//		OwnMethods.Print(jsonArray.size());
		return jsonArray;
	}
	
	public Result SubgraphMatch_Spa_API(Query_Graph query_Graph, int limit)
	{
		query_node_count = query_Graph.graph.size();
		neo4j_time = new int[query_node_count];
		hmbr_check_time = new int[query_node_count];
		spa_check_time = new int[query_node_count];

		int [][] minhop_index = Ini_Minhop(query_Graph);

		String query = FormCypherQuery(query_Graph, limit, minhop_index);
		
		OwnMethods.Print(query);
		Result result = neo4j_API.graphDb.execute(query);
		return result;
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

	/**
	 * form cypher query with minhop check
	 * @param query_Graph
	 * @param limit
	 * @return
	 */
	public String FormCypherQuery(Query_Graph query_Graph, int limit, int[][] minhop_index) 
	{
		String query = "profile match ";
		
		//label
		query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
		for(int i = 1; i < query_Graph.graph.size(); i++)
		{
			query += String.format(",(a%d:GRAPH_%d)",i, query_Graph.label_list[i]);
		}
		
		//edge
		for(int i = 0; i<query_Graph.graph.size(); i++)
		{
			for(int j = 0;j<query_Graph.graph.get(i).size();j++)
			{
				int neighbor = query_Graph.graph.get(i).get(j);
				if(neighbor > i)
					query += String.format(",(a%d)--(a%d)", i, neighbor);
			}
		}
		
		//spatial predicate
		int i = 0;
		for(; i < query_Graph.label_list.length; i++)
			if(query_Graph.spa_predicate[i] != null)
			{
				MyRectangle qRect = query_Graph.spa_predicate[i];
				query += String.format(" where %f < a%d.%s < %f ", qRect.min_x, i, lon_name, qRect.max_x);
				query += String.format("and %f < a%d.%s < %f", qRect.min_y, i, lat_name, qRect.max_y);
				i++;
				break; 
			}
		for(; i < query_Graph.label_list.length; i++)
			if(query_Graph.spa_predicate[i] != null)
			{
				MyRectangle qRect = query_Graph.spa_predicate[i];
				query += String.format(" and %f < a%d.%s < %f ", qRect.min_x, i, lon_name, qRect.max_x);
				query += String.format("and %f < a%d.%s < %f", qRect.min_y, i, lat_name, qRect.max_y);
			}
		
		//hmbr prune
		for ( i = 0; i < query_node_count; i++)
		{
			if ( query_Graph.spa_predicate[i] != null)
			{
				MyRectangle cur_rect = query_Graph.spa_predicate[i];
				for ( int j = 0; j < query_node_count; j++)
				{
					if ( i == j)
						continue;
					int minhop = minhop_index[i][j];
					query += String.format(" and a%d.HMBR_%d_%s < %f", j, minhop, minx_name, cur_rect.max_x);
					query += String.format(" and a%d.HMBR_%d_%s < %f", j, minhop, miny_name, cur_rect.max_y);
					query += String.format(" and a%d.HMBR_%d_%s > %f", j, minhop, maxx_name, cur_rect.min_x);
					query += String.format(" and a%d.HMBR_%d_%s > %f", j, minhop, maxy_name, cur_rect.min_y);
				}
			}
		}
		
		//return
		query += " return id(a0)";
		for(i = 1; i<query_Graph.graph.size(); i++)
			query += String.format(",id(a%d)", i);
		
		if(limit != -1)
			query += String.format(" limit %d", limit);
		
		return query;
	}
}
