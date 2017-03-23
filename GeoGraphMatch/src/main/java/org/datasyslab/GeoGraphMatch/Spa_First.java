package org.datasyslab.GeoGraphMatch;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import org.neo4j.cypher.internal.compiler.v1_9.executionplan.PlanDescription;
import org.neo4j.cypher.javacompat.ProfilerStatistics;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;

import commons.*;

public class Spa_First {

	//neo4j graphdb service
	public Neo4j_API neo4j_API;

	String minx_name;
	String miny_name;
	String maxx_name;
	String maxy_name;
	
	public String lon_name;
	public String lat_name;
	
	//postgres service
	private String RTreeName;
	private Connection con;
	private Statement st;
	private ResultSet rs;
	
	//query statistics
	public long postgresql_time;
	public long get_iterator_time;
	public long iterate_time;
	public long result_count;
	public long page_hit_count;

	public Spa_First(String db_path, String p_RTreeName)
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

		this.RTreeName = p_RTreeName;
		con = PostgresJDBC.GetConnection();
		try {
			st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * currently just support one spatial predicate
	 * @param query_Graph
	 * @param limit
	 */
	public void SubgraphMatch_Spa_API(Query_Graph query_Graph, int limit)
	{
		postgresql_time = 0;
		get_iterator_time = 0;
		iterate_time = 0;
		result_count = 0;
		page_hit_count = 0;
		
		String start_str = "profile match ";
		
		//label
		start_str += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
		for(int i = 1; i < query_Graph.graph.size(); i++)
		{
			start_str += String.format(",(a%d:GRAPH_%d)",i, query_Graph.label_list[i]);
		}
		
		//edge
		for(int i = 0; i<query_Graph.graph.size(); i++)
		{
			for(int j = 0;j<query_Graph.graph.get(i).size();j++)
			{
				int neighbor = query_Graph.graph.get(i).get(j);
				if(neighbor > i)
					start_str += String.format(",(a%d)--(a%d)", i, neighbor);
			}
		}
		
		//return
		String return_str = " return a0";
		for(int i = 1; i<query_Graph.graph.size(); i++)
			return_str += String.format(",a%d", i);
		
		if(limit != -1)
			return_str += String.format(" limit %d", limit);
		
		try {
			long start = System.currentTimeMillis();
			MyRectangle query_rect = null;
			int spa_index = 0;
			for ( int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
				if(query_Graph.Has_Spa_Predicate[i])
				{
					query_rect = query_Graph.spa_predicate[i];
					spa_index = i;
					break;
				}
			
			this.RangeQuery(query_rect);
			ArrayList<Integer> ids = new ArrayList<Integer>(200000);
			while (rs.next())
			{
				int id = Integer.parseInt(rs.getString("id").toString());
				ids.add(id);
			}
			postgresql_time = System.currentTimeMillis() - start;
			
			for ( int id : ids)
			{
				start = System.currentTimeMillis();
				String query = start_str + String.format(" where id(a%d) in [%d]", spa_index, id) + return_str;
				Result result = this.neo4j_API.graphDb.execute(query);
				get_iterator_time += System.currentTimeMillis() - start;
				
				start = System.currentTimeMillis();
				while(result.hasNext())
					result.next();
				iterate_time += System.currentTimeMillis() - start;
				
				ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
				ExecutionPlanDescription.ProfilerStatistics profile = planDescription.getProfilerStatistics();
				result_count += profile.getRows();
				page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void RangeQuery(MyRectangle rect)
	{
		try
		{			
			String query = "select id from " + RTreeName + " where location <@ box '((" + rect.min_x + "," + rect.min_y + ")," + "(" + rect.max_x + "," + rect.max_y + "))'";
			rs = st.executeQuery(query);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void ShutDown()
	{
		PostgresJDBC.Close(st);
		PostgresJDBC.Close(con);
		this.neo4j_API.ShutDown();
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
