package org.datasyslab.GeoGraphMatch;

import java.util.ArrayList;
import java.util.Date;

import javax.management.Query;
import javax.rmi.CORBA.Util;
import javax.swing.JApplet;

import org.neo4j.cypher.internal.compiler.v2_2.functions.Str;
import org.neo4j.cypher.internal.compiler.v2_2.perty.recipe.Pretty.nest;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import com.google.gson.JsonArray;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;

import commons.OwnMethods;
import commons.Query_Graph;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

import commons.*;
import commons.*;

public class Experiment {
	
//	public static void test()
//	{
//		STRtree strtree = new STRtree();
//		
//		GeometryFactory fact=new GeometryFactory();
//		Point datapoint=fact.createPoint(new Coordinate(-109.73, 35.08));
//		
//		strtree.insert(datapoint.getEnvelopeInternal(), datapoint);
//		strtree.kNearestNeighbour(new Envelope(-98.6361828, -95.0993852,46.88333326666667,48.392923),
//				fact.toGeometry(new Envelope(-98.6361828, -95.0993852,46.88333326666667,48.392923)),
//				new GeometryItemDistance(), 10);
//
//	}
	
	private static String dataset = "Gowalla";
	private static boolean TEST_FORMAT = false;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		Query1_Hot();
//		Query3_Hot();
//		Query1_Hot_API();
//		Query3_Hot_API();
		
//		HMBR_Ratio(0);
//		HMBR_Ratio(1);
//		HMBR_Ratio(2);
//		HMBR_Ratio(4);
//		HMBR_Ratio(5);
//		HMBR_Ratio(6);
//		HMBR_Ratio(7);
//		HMBR_Ratio(8);
//		HMBR_Ratio(9);
		
//		Query_Hot_API(0);
//		Query_Hot_API(1);
//		Query_Hot_API(2);
//		Query_Hot_API(3);
//		Query_Hot_API(4);
//		Query_Hot_API(5);
//		Query_Hot_API(6);
//		Query_Hot_API(7);
//		Query_Hot_API(9);
//		Query_Hot_API(10);
		Query_Hot_API(11);
		
//		MostNaive(0);
//		MostNaive(1);
//		MostNaive(2);
//		MostNaive(5);
//		MostNaive(6);
		
//		SpaFirst(0);
//		SpaFirst(1);
//		SpaFirst(2);
//		SpaFirst(4);
//		SpaFirst(5);
//		SpaFirst(6);
//		SpaFirst(7);
//		SpaFirst(9);
		
		
//		Neo4j_Naive(5);
		
	}
	
	public static void Neo4j_Naive(int query_id)
	{
		long start;
		long time;
		int limit = -1;
		
		int method_suffix = 1000000;
//		while ( method_suffix < 200000)
		{
			String db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_%s_%d/data/graph.db", dataset, method_suffix);
			
			String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
			ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
			Query_Graph query_Graph = queryGraphs.get(query_id);
			
			String result_detail_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/neo4j_%d_%d_API.txt", dataset, method_suffix, query_id);
			String result_avg_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/neo4j_%d_%d_API_avg.txt", dataset, method_suffix, query_id);
			ArrayList<Long> time_get_iterator = new ArrayList<Long>();
			ArrayList<Long> time_iterate = new ArrayList<Long>();
			ArrayList<Long> total_time = new ArrayList<Long>();
			ArrayList<Long> count = new ArrayList<Long>();
			ArrayList<Long> access = new ArrayList<Long>();
			
			String write_line = String.format("%s\t%d\n", dataset, limit);
			if(!TEST_FORMAT)
			{
				OwnMethods.WriteFile(result_detail_path, true, write_line);
				OwnMethods.WriteFile(result_avg_path, true, write_line);
			}
			
			String head_line = "count\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_avg_path, true, "selectivity\t" + head_line);
			
			int name_suffix = 1;
			int times = 10;
			int expe_count = 15;
			while ( name_suffix <= 200000)
			{
				double selectivity = name_suffix / 1000000.0;
				String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, name_suffix);
				
				write_line = selectivity + "\n" + head_line;
				if(!TEST_FORMAT)
					OwnMethods.WriteFile(result_detail_path, true, write_line);
				
				ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
				Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(db_path);
				for ( int i = 0; i < expe_count; i++)
				{
					MyRectangle rectangle = queryrect.get(i);
					query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];
					
					int j = 0;
					for (  ; j < query_Graph.graph.size(); j++)
						if(query_Graph.Has_Spa_Predicate[j])
							break;
					query_Graph.spa_predicate[j] = rectangle;
					
					if(!TEST_FORMAT)
					{
						OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));
						
						start = System.currentTimeMillis();
						Result result = naive_Neo4j_Match.SubgraphMatch_Spa_API(query_Graph, limit);
						time = System.currentTimeMillis() - start;
						time_get_iterator.add(time);
						
						start = System.currentTimeMillis();
						while(result.hasNext())
							result.next();
						time = System.currentTimeMillis() - start;
						time_iterate.add(time);
						
						int index = time_get_iterator.size() - 1;
						total_time.add(time_get_iterator.get(index) + time_iterate.get(index));
						
						ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
						count.add(planDescription.getProfilerStatistics().getRows());
						access.add(OwnMethods.GetTotalDBHits(planDescription));
						
						write_line = String.format("%d\t%d\t", count.get(i), time_get_iterator.get(i));
						write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
						write_line += String.format("%d\n", access.get(i));
						if(!TEST_FORMAT)
							OwnMethods.WriteFile(result_detail_path, true, write_line);
					}
				}
				naive_Neo4j_Match.neo4j_API.ShutDown();
				
				write_line = String.valueOf(selectivity) + "\t";
				write_line += String.format("%d\t%d\t", Utility.Average(count), Utility.Average(time_get_iterator));
				write_line += String.format("%d\t%d\t", Utility.Average(time_iterate), Utility.Average(total_time));
				write_line += String.format("%d\n", Utility.Average(access));
				if(!TEST_FORMAT)
					OwnMethods.WriteFile(result_avg_path, true, write_line);
				
				long larger_time = Utility.Average(total_time);
				if (larger_time * expe_count > 150 * 1000)
					expe_count = (int) (expe_count * 0.5 / (larger_time * expe_count / 150.0 / 1000.0));
				if(expe_count < 1)
					expe_count = 1;
				
				count.clear();	time_get_iterator.clear();
				time_iterate.clear();	total_time.clear();
				access.clear();
				
				name_suffix *= times;
			}
			
			method_suffix *= 10;
		}
		
		
		
		
	}
	
	public static void HMBR_Ratio(int query_id)
	{
		long start;
		long time;
		int limit = -1;
		
		int method_suffix = 750000;
		while ( method_suffix > 100000)
		{
			String db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_%s_%d/data/graph.db", dataset, method_suffix);
			
			String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
			ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
			Query_Graph query_Graph = queryGraphs.get(query_id);
			
			String result_detail_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/hmbr_%d_%d_API.txt", dataset, method_suffix, query_id);
			String result_avg_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/hmbr_%d_%d_API_avg.txt", dataset, method_suffix, query_id);
			ArrayList<Long> time_get_iterator = new ArrayList<Long>();
			ArrayList<Long> time_iterate = new ArrayList<Long>();
			ArrayList<Long> total_time = new ArrayList<Long>();
			ArrayList<Long> count = new ArrayList<Long>();
			ArrayList<Long> access = new ArrayList<Long>();
			
			String write_line = String.format("%s\t%d\t%s\n", dataset, limit, new Date());
			if(!TEST_FORMAT)
			{
				OwnMethods.WriteFile(result_detail_path, true, write_line);
				OwnMethods.WriteFile(result_avg_path, true, write_line);
			}
			
			String head_line = "count\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_avg_path, true, "selectivity\t" + head_line);
			
			int name_suffix = 1;
			int times = 10;
			int expe_count = 15;
			while ( name_suffix <= 200000)
			{
				double selectivity = name_suffix / 1000000.0;
				String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, name_suffix);
				
				write_line = selectivity + "\n" + head_line;
				if(!TEST_FORMAT)
					OwnMethods.WriteFile(result_detail_path, true, write_line);
				
				ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
				Minhop_Match minhop_Match = new Minhop_Match(db_path);
				boolean hot = false;
				for ( int i = 0; i < expe_count; i++)
				{
					MyRectangle rectangle = queryrect.get(i);
					query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];
					
					int j = 0;
					for (  ; j < query_Graph.graph.size(); j++)
						if(query_Graph.Has_Spa_Predicate[j])
							break;
					query_Graph.spa_predicate[j] = rectangle;
					
					if (hot == false)
					{
						Result result = minhop_Match.SubgraphMatch_Spa_API(query_Graph, -1);
						while(result.hasNext())
							result.next();
						hot = true;
						i--;
						continue;
					}
					
					if(!TEST_FORMAT)
					{
						OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));
						
						start = System.currentTimeMillis();
						Result result = minhop_Match.SubgraphMatch_Spa_API(query_Graph, limit);
						time = System.currentTimeMillis() - start;
						time_get_iterator.add(time);
						
						start = System.currentTimeMillis();
						while(result.hasNext())
							result.next();
						time = System.currentTimeMillis() - start;
						time_iterate.add(time);
						
						int index = time_get_iterator.size() - 1;
						total_time.add(time_get_iterator.get(index) + time_iterate.get(index));
						
						ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
						count.add(planDescription.getProfilerStatistics().getRows());
						access.add(OwnMethods.GetTotalDBHits(planDescription));
						
						write_line = String.format("%d\t%d\t", count.get(i), time_get_iterator.get(i));
						write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
						write_line += String.format("%d\n", access.get(i));
						if(!TEST_FORMAT)
							OwnMethods.WriteFile(result_detail_path, true, write_line);
					}
				}
				minhop_Match.neo4j_API.ShutDown();
				
				write_line = String.valueOf(selectivity) + "\t";
				write_line += String.format("%d\t%d\t", Utility.Average(count), Utility.Average(time_get_iterator));
				write_line += String.format("%d\t%d\t", Utility.Average(time_iterate), Utility.Average(total_time));
				write_line += String.format("%d\n", Utility.Average(access));
				if(!TEST_FORMAT)
					OwnMethods.WriteFile(result_avg_path, true, write_line);
				
				long larger_time = Utility.Average(total_time);
				if (larger_time * expe_count > 150 * 1000)
					expe_count = (int) (expe_count * 0.5 / (larger_time * expe_count / 150.0 / 1000.0));
				if(expe_count < 1)
					expe_count = 1;
				
				count.clear();	time_get_iterator.clear();
				time_iterate.clear();	total_time.clear();
				access.clear();
				
				name_suffix *= times;
			}
			
			method_suffix -= 250000;
		}
		
		
		
		
	}
	
	public static void SpaFirst(int query_id)
	{
		long start;
		long time;
		int limit = -1;
		int expe_count = 15;
		
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_"+ dataset +"/data/graph.db";
		
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		
		String result_detail_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/spa_first_%d_API.txt", dataset, query_id);
		String result_avg_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/spa_first_%d_API_avg.txt", dataset, query_id);
		ArrayList<Long> time_get_iterator = new ArrayList<Long>();
		ArrayList<Long> time_iterate = new ArrayList<Long>();
		ArrayList<Long> total_time = new ArrayList<Long>();
		ArrayList<Long> count = new ArrayList<Long>();
		ArrayList<Long> access = new ArrayList<Long>();
		
		ArrayList<Long> psql_time = new ArrayList<Long>();
		
		String write_line = String.format("%s\t%d\n", dataset, limit);
		if(!TEST_FORMAT)
		{
			OwnMethods.WriteFile(result_detail_path, true, write_line);
			OwnMethods.WriteFile(result_avg_path, true, write_line);
		}
		
		String head_line = "count\tpsql_time\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
		if(!TEST_FORMAT)
			OwnMethods.WriteFile(result_avg_path, true, "selectivity\t" + head_line);
		
		int name_suffix = 1;
		int times = 10;
		while ( name_suffix <= 20000)
		{
			double selectivity = name_suffix / 1000000.0;
			String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, name_suffix);
			
			write_line = selectivity + "\n" + head_line;
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_detail_path, true, write_line);
			
			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			Spa_First spa_First = new Spa_First(db_path, dataset);
			for ( int i = 0; i < expe_count; i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];
				
				int j = 0;
				for (  ; j < query_Graph.graph.size(); j++)
					if(query_Graph.Has_Spa_Predicate[j])
						break;
				query_Graph.spa_predicate[j] = rectangle;
				
				if(!TEST_FORMAT)
				{
					OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));
					
					start = System.currentTimeMillis();
					spa_First.SubgraphMatch_Spa_API(query_Graph, limit);
					time = System.currentTimeMillis() - start;
					
					time_get_iterator.add(spa_First.get_iterator_time);
					time_iterate.add(spa_First.iterate_time);
					total_time.add(time);
					count.add(spa_First.result_count);
					access.add(spa_First.page_hit_count);
					psql_time.add(spa_First.postgresql_time);
					
					write_line = String.format("%d\t%d\t", count.get(i), psql_time.get(i));
					write_line += String.format("%d\t", time_get_iterator.get(i));
					write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
					write_line += String.format("%d\n", access.get(i));
					if(!TEST_FORMAT)
						OwnMethods.WriteFile(result_detail_path, true, write_line);
				}
			}
			spa_First.ShutDown();
			
			write_line = String.valueOf(selectivity) + "\t";
			write_line += String.format("%d\t%d\t", Utility.Average(count), Utility.Average(psql_time));
			write_line += String.format("%d\t", Utility.Average(time_get_iterator));
			write_line += String.format("%d\t%d\t", Utility.Average(time_iterate), Utility.Average(total_time));
			write_line += String.format("%d\n", Utility.Average(access));
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_avg_path, true, write_line);
			
			long larger_time = Utility.Average(total_time);
			if (larger_time * expe_count > 450 * 1000)
				expe_count = (int) (expe_count * 0.5 / (larger_time * expe_count / 450.0 / 1000.0));
			if(expe_count < 1)
				expe_count = 1;
			
			count.clear();	time_get_iterator.clear();
			time_iterate.clear();	total_time.clear();
			access.clear();
			
			name_suffix *= times;
		}
	}
	
	
	public static void MostNaive(int query_id)
	{
		long start;
		long time;
		int limit = -1;
		int expe_count = 1;
		
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_"+ dataset +"/data/graph.db";
		
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		
		String result_detail_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/naive_%d_API.txt", dataset, query_id);
		String result_avg_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/naive_%d_API_avg.txt", dataset, query_id);
		ArrayList<Long> time_get_iterator = new ArrayList<Long>();
		ArrayList<Long> time_iterate = new ArrayList<Long>();
		ArrayList<Long> total_time = new ArrayList<Long>();
		ArrayList<Long> count = new ArrayList<Long>();
		ArrayList<Long> access = new ArrayList<Long>();
		
		String write_line = String.format("%s\t%d\n", dataset, limit);
		if(!TEST_FORMAT)
		{
			OwnMethods.WriteFile(result_detail_path, true, write_line);
			OwnMethods.WriteFile(result_avg_path, true, write_line);
		}
		
		String head_line = "count\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
		if(!TEST_FORMAT)
			OwnMethods.WriteFile(result_avg_path, true, "selectivity\t" + head_line);
		
		int name_suffix = 1;
		int times = 10;
		while ( name_suffix <= 20000)
		{
			double selectivity = name_suffix / 1000000.0;
			String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, name_suffix);
			
			write_line = selectivity + "\n" + head_line;
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_detail_path, true, write_line);
			
			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			Naive_Most_Neo4j_Match naive_Most_Neo4j_Match = new Naive_Most_Neo4j_Match(db_path);
			for ( int i = 0; i < expe_count; i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];
				
				int j = 0;
				for (  ; j < query_Graph.graph.size(); j++)
					if(query_Graph.Has_Spa_Predicate[j])
						break;
				query_Graph.spa_predicate[j] = rectangle;
				
				if(!TEST_FORMAT)
				{
					OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));
					
					start = System.currentTimeMillis();
					naive_Most_Neo4j_Match.SubgraphMatch_Spa_API(query_Graph, limit);
					time = System.currentTimeMillis() - start;
					
					time_get_iterator.add(naive_Most_Neo4j_Match.get_iterator_time);
					time_iterate.add(naive_Most_Neo4j_Match.iterate_time);
					total_time.add(time);
					count.add(naive_Most_Neo4j_Match.result_count);
					access.add(naive_Most_Neo4j_Match.page_hit_count);
					
					write_line = String.format("%d\t%d\t", count.get(i), time_get_iterator.get(i));
					write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
					write_line += String.format("%d\n", access.get(i));
					if(!TEST_FORMAT)
						OwnMethods.WriteFile(result_detail_path, true, write_line);
				}
			}
			naive_Most_Neo4j_Match.neo4j_API.ShutDown();
			
			write_line = String.valueOf(selectivity) + "\t";
			write_line += String.format("%d\t%d\t", Utility.Average(count), Utility.Average(time_get_iterator));
			write_line += String.format("%d\t%d\t", Utility.Average(time_iterate), Utility.Average(total_time));
			write_line += String.format("%d\n", Utility.Average(access));
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_avg_path, true, write_line);
			
			count.clear();	time_get_iterator.clear();
			time_iterate.clear();	total_time.clear();
			access.clear();
			
			name_suffix *= times;
		}
	}
	
	
	
	public static void Query1_Hot()
	{
		double selectivity = 0.000001;
		long start;
		long time;
		int limit = -1;
		
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraphs(querygraph_path, 1);
		Query_Graph query_Graph = queryGraphs.get(0);
		
		String result_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/result_1.txt");
		ArrayList<Long> time_minhop = new ArrayList<Long>();
		ArrayList<Long> time_naive = new ArrayList<Long>();
		ArrayList<Integer> count_minhop = new ArrayList<Integer>();
		ArrayList<Integer> count_naive = new ArrayList<Integer>();
		
		OwnMethods.WriteFile(result_path, true, dataset + "\n");
		
		while ( selectivity < 0.002)
		{
			int log = (int) Math.log10(selectivity);
			String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s_%d_1.txt", dataset, log);
			
			String write_line = selectivity + "\n";
			write_line += "minhop_time\tnaive_time\tminhop_count\tnaive_count\n";
			OwnMethods.WriteFile(result_path, true, write_line);
			
			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			Minhop_Match minhop_Match = new Minhop_Match();
			for ( int i = 0; i < queryrect.size(); i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate[1] = rectangle;
				start = System.currentTimeMillis();
				JsonArray jsonArray = minhop_Match.SubgraphMatch_Spa(query_Graph, limit);
				time = System.currentTimeMillis() - start;
				
				time_minhop.add(time);
				count_minhop.add(jsonArray.size());
			}
			
			Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match();
			for ( int i = 0; i < queryrect.size(); i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate[1] = rectangle;
				start = System.currentTimeMillis();
				JsonArray jsonArray = naive_Neo4j_Match.SubgraphMatch_Spa(query_Graph, limit);
				time = System.currentTimeMillis() - start;
				
				time_naive.add(time);
				count_naive.add(jsonArray.size());
			}

			for ( int i = 0; i < time_minhop.size(); i++)
			{
				write_line = String.format("%d\t%d\t%d\t%d\n", time_minhop.get(i), time_naive.get(i), count_minhop.get(i), count_naive.get(i));
				OwnMethods.WriteFile(result_path, true, write_line);
			}
			OwnMethods.WriteFile(result_path, true, "\n");
			
			selectivity *= 10;
			time_minhop.clear();	time_naive.clear();
			count_minhop.clear();	count_naive.clear();
		}
	}
	
	public static void Query1_Hot_API()
	{
		double selectivity = 0.000001;
		long start;
		long time;
		int limit = -1;
		int expe_count = 5;
		
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_"+dataset+"/data/graph.db";
		
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraphs(querygraph_path, 1);
		Query_Graph query_Graph = queryGraphs.get(0);
		
		String result_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/result_1_API.txt");
		ArrayList<Long> time_minhop = new ArrayList<Long>();
		ArrayList<Long> time_naive = new ArrayList<Long>();
		ArrayList<Long> time_minhop_ser = new ArrayList<Long>();
		ArrayList<Long> time_naive_ser = new ArrayList<Long>();
		ArrayList<Long> count_minhop = new ArrayList<Long>();
		ArrayList<Long> count_naive = new ArrayList<Long>();
		ArrayList<Long> access_minhop = new ArrayList<Long>();
		ArrayList<Long> access_naive = new ArrayList<Long>();
		
		OwnMethods.WriteFile(result_path, true, dataset + "\n");
		
		while ( selectivity < 0.002)
		{
			int log = (int) Math.log10(selectivity);
			String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s_%d_1.txt", dataset, log);
			
			String write_line = selectivity + "\ncount_minhop\tcount_naive\ttime_minhop\ttime_naive\t";
			write_line += "time_minhop_ser\ttime_naive_ser\ttotal_time_minhop\ttotal_time_naive\t";
			write_line += "access_minhop\taccess_naive\n";
			OwnMethods.WriteFile(result_path, true, write_line);
			
			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			Minhop_Match minhop_Match = new Minhop_Match(db_path);
			for ( int i = 0; i < expe_count; i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate[1] = rectangle;
				start = System.currentTimeMillis();
				Result result = minhop_Match.SubgraphMatch_Spa_API(query_Graph, limit);
				time = System.currentTimeMillis() - start;
				
				start = System.currentTimeMillis();
				while ( result.hasNext())
				{
					result.next();
				}
				time_minhop_ser.add(System.currentTimeMillis() - start);
				
				ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
				ExecutionPlanDescription.ProfilerStatistics profilerStatistics = planDescription.getProfilerStatistics();
				time_minhop.add(time);
				count_minhop.add(profilerStatistics.getRows());
				access_minhop.add(OwnMethods.GetTotalDBHits(planDescription));
			}
			minhop_Match.neo4j_API.ShutDown();
			
			Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(db_path);
			for ( int i = 0; i < expe_count; i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));
				query_Graph.spa_predicate[1] = rectangle;
				start = System.currentTimeMillis();
				Result result = naive_Neo4j_Match.SubgraphMatch_Spa_API(query_Graph, limit);
				time = System.currentTimeMillis() - start;
				
				start = System.currentTimeMillis();
				while ( result.hasNext())
					result.next();
				time_naive_ser.add(System.currentTimeMillis() - start);
				
				ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
				ExecutionPlanDescription.ProfilerStatistics profilerStatistics = planDescription.getProfilerStatistics();
				time_naive.add(time);
				count_naive.add(profilerStatistics.getRows());
				access_naive.add(OwnMethods.GetTotalDBHits(planDescription));
			}
			naive_Neo4j_Match.neo4j_API.ShutDown();

			for ( int i = 0; i < time_minhop.size(); i++)
			{
				write_line = String.format("%d\t%d\t%d\t%d\t", count_minhop.get(i), count_naive.get(i), time_minhop.get(i), time_naive.get(i));
				write_line += String.format("%d\t%d\t", time_minhop_ser.get(i), time_naive_ser.get(i));
				write_line += String.format("%d\t%d\t", time_minhop.get(i) + time_minhop_ser.get(i), time_naive.get(i) + time_naive_ser.get(i));
				write_line += String.format("%d\t%d\n", access_minhop.get(i), access_naive.get(i));
				
				OwnMethods.WriteFile(result_path, true, write_line);
			}
			OwnMethods.WriteFile(result_path, true, "\n");
			
			selectivity *= 10;
			time_minhop.clear();	time_naive.clear();
			time_minhop_ser.clear();time_naive_ser.clear();
			count_minhop.clear();	count_naive.clear();
			access_minhop.clear();	access_naive.clear();
		}
	}
	
	public static void Query3_Hot()
	{
		double selectivity = 0.000001;
		long start;
		long time;
		int limit = -1;
		
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraphs(querygraph_path, 3);
		Query_Graph query_Graph = queryGraphs.get(2);
		
		String result_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/result_3.txt");
		ArrayList<Long> time_minhop = new ArrayList<Long>();
		ArrayList<Long> time_naive = new ArrayList<Long>();
		ArrayList<Integer> count_minhop = new ArrayList<Integer>();
		ArrayList<Integer> count_naive = new ArrayList<Integer>();
		
		OwnMethods.WriteFile(result_path, true, dataset + "\n");
		
		while ( selectivity < 0.002)
		{
			int log = (int) Math.log10(selectivity);
			String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s_%d_1.txt", dataset, log);
			
			String write_line = selectivity + "\n";
			write_line += "minhop_time\tnaive_time\tminhop_count\tnaive_count\n";
			OwnMethods.WriteFile(result_path, true, write_line);
			
			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			Minhop_Match minhop_Match = new Minhop_Match();
			for ( int i = 0; i < queryrect.size(); i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate[1] = rectangle;
				start = System.currentTimeMillis();
				JsonArray jsonArray = minhop_Match.SubgraphMatch_Spa(query_Graph, limit);
				time = System.currentTimeMillis() - start;
				
				time_minhop.add(time);
				count_minhop.add(jsonArray.size());
			}
			
			Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match();
			for ( int i = 0; i < queryrect.size(); i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate[1] = rectangle;
				start = System.currentTimeMillis();
				JsonArray jsonArray = naive_Neo4j_Match.SubgraphMatch_Spa(query_Graph, limit);
				time = System.currentTimeMillis() - start;
				
				time_naive.add(time);
				count_naive.add(jsonArray.size());
			}

			for ( int i = 0; i < time_minhop.size(); i++)
			{
				write_line = String.format("%d\t%d\t%d\t%d\n", time_minhop.get(i), time_naive.get(i), count_minhop.get(i), count_naive.get(i));
				OwnMethods.WriteFile(result_path, true, write_line);
			}
			OwnMethods.WriteFile(result_path, true, "\n");
			
			selectivity *= 10;
			time_minhop.clear();	time_naive.clear();
			count_minhop.clear();	count_naive.clear();
		}
	}
	
	public static void Query3_Hot_API()
	{
		double selectivity = 0.000001;
		long start;
		long time;
		int limit = -1;
		int expe_count = 10;
		
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_"+dataset+"/data/graph.db";
		
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraphs(querygraph_path, 3);
		Query_Graph query_Graph = queryGraphs.get(2);
		
		String result_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/result_3_API.txt");
		ArrayList<Long> time_minhop = new ArrayList<Long>();
		ArrayList<Long> time_naive = new ArrayList<Long>();
		ArrayList<Long> time_minhop_ser = new ArrayList<Long>();
		ArrayList<Long> time_naive_ser = new ArrayList<Long>();
		ArrayList<Long> count_minhop = new ArrayList<Long>();
		ArrayList<Long> count_naive = new ArrayList<Long>();
		ArrayList<Long> access_minhop = new ArrayList<Long>();
		ArrayList<Long> access_naive = new ArrayList<Long>();
		
		OwnMethods.WriteFile(result_path, true, dataset + "\n");
		
		while ( selectivity < 0.002)
		{
			int log = (int) Math.log10(selectivity);
			String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s_%d_3.txt", dataset, log);
			
			String write_line = selectivity + "\ncount_minhop\tcount_naive\ttime_minhop\ttime_naive\t";
			write_line += "time_minhop_ser\ttime_naive_ser\ttotal_time_minhop\ttotal_time_naive\t";
			write_line += "access_minhop\taccess_naive\n";
			OwnMethods.WriteFile(result_path, true, write_line);
			
			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			Minhop_Match minhop_Match = new Minhop_Match(db_path);
			for ( int i = 0; i < expe_count; i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate[1] = rectangle;
				start = System.currentTimeMillis();
				Result result = minhop_Match.SubgraphMatch_Spa_API(query_Graph, limit);
				time = System.currentTimeMillis() - start;
				
				start = System.currentTimeMillis();
				while ( result.hasNext())
				{
					result.next();
				}
				time_minhop_ser.add(System.currentTimeMillis() - start);
				
				ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
				ExecutionPlanDescription.ProfilerStatistics profilerStatistics = planDescription.getProfilerStatistics();
				time_minhop.add(time);
				count_minhop.add(profilerStatistics.getRows());
				access_minhop.add(OwnMethods.GetTotalDBHits(planDescription));
				result.close();
			}
			minhop_Match.neo4j_API.ShutDown();
			
			Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(db_path);
			for ( int i = 0; i < expe_count; i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate[1] = rectangle;
				start = System.currentTimeMillis();
				Result result = naive_Neo4j_Match.SubgraphMatch_Spa_API(query_Graph, limit);
				time = System.currentTimeMillis() - start;
				
				start = System.currentTimeMillis();
				while ( result.hasNext())
					result.next();
				time_naive_ser.add(System.currentTimeMillis() - start);
				
				ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
				ExecutionPlanDescription.ProfilerStatistics profilerStatistics = planDescription.getProfilerStatistics();
				time_naive.add(time);
				count_naive.add(profilerStatistics.getRows());
				access_naive.add(OwnMethods.GetTotalDBHits(planDescription));
				result.close();
			}
			naive_Neo4j_Match.neo4j_API.ShutDown();

			for ( int i = 0; i < time_minhop.size(); i++)
			{
				write_line = String.format("%d\t%d\t%d\t%d\t", count_minhop.get(i), count_naive.get(i), time_minhop.get(i), time_naive.get(i));
				write_line += String.format("%d\t%d\t", time_minhop_ser.get(i), time_naive_ser.get(i));
				write_line += String.format("%d\t%d\t", time_minhop.get(i) + time_minhop_ser.get(i), time_naive.get(i) + time_naive_ser.get(i));
				write_line += String.format("%d\t%d\n", access_minhop.get(i), access_naive.get(i));
				
				OwnMethods.WriteFile(result_path, true, write_line);
			}
			OwnMethods.WriteFile(result_path, true, "\n");
			
			selectivity *= 10;
			time_minhop.clear();	time_naive.clear();
			time_minhop_ser.clear();time_naive_ser.clear();
			count_minhop.clear();	count_naive.clear();
			access_minhop.clear();	access_naive.clear();
		}
	}
	
	public static void Query_Hot_API_Equal(int node_count)
	{
		
	}
	
	public static void Query_Hot_API(int query_id)
	{
		long start;
		long time;
		int limit = -1;
		int expe_count = 15;
		
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_"+ dataset +"/data/graph.db";
		
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		
		String result_detail_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/result_%d_API.txt", dataset, query_id);
		String result_avg_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/result_%d_API_avg.txt", dataset, query_id);
		ArrayList<Long> time_minhop = new ArrayList<Long>();
		ArrayList<Long> time_naive = new ArrayList<Long>();
		ArrayList<Long> time_minhop_ser = new ArrayList<Long>();
		ArrayList<Long> time_naive_ser = new ArrayList<Long>();
		ArrayList<Long> count_minhop = new ArrayList<Long>();
		ArrayList<Long> count_naive = new ArrayList<Long>();
		ArrayList<Long> access_minhop = new ArrayList<Long>();
		ArrayList<Long> access_naive = new ArrayList<Long>();
		
		String write_line = String.format("%s\t%d\t%s\n", dataset, limit, new Date());
		if(!TEST_FORMAT)
		{
			OwnMethods.WriteFile(result_detail_path, true, write_line);
			OwnMethods.WriteFile(result_avg_path, true, write_line);
		}
		
		String head_line = "count_minhop\tcount_naive\ttime_minhop\ttime_naive\t";
		head_line += "time_minhop_ser\ttime_naive_ser\ttotal_time_minhop\ttotal_time_naive\t";
		head_line += "access_minhop\taccess_naive\n";
		if(!TEST_FORMAT)
			OwnMethods.WriteFile(result_avg_path, true, "selectivity\t" + head_line);
		
		int name_suffix = 1;
		int times = 10;
		while ( name_suffix <= 20000)
		{
			double selectivity = name_suffix / 1000000.0;
			String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, name_suffix);
			
			write_line = selectivity + "\n" + head_line;
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_detail_path, true, write_line);
			
			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			Minhop_Match minhop_Match = new Minhop_Match(db_path);
			boolean hot = false;
			for ( int i = 0; i < expe_count; i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];
				
				int j = 0;
				for (  ; j < query_Graph.graph.size(); j++)
					if(query_Graph.Has_Spa_Predicate[j])
						break;
				query_Graph.spa_predicate[j] = rectangle;
				
				if (hot == false)
				{
					Result result = minhop_Match.SubgraphMatch_Spa_API(query_Graph, -1);
					while(result.hasNext())
						result.next();
					hot = true;
					i--;
					continue;
				}
				
				if(!TEST_FORMAT)
				{
					OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));

					start = System.currentTimeMillis();
					Result result = minhop_Match.SubgraphMatch_Spa_API(query_Graph, limit);
					time = System.currentTimeMillis() - start;
					time_minhop.add(time);


					start = System.currentTimeMillis();
					while ( result.hasNext())
					{
						result.next();
					}
					time = System.currentTimeMillis() - start;
					time_minhop_ser.add(time);

					ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
					ExecutionPlanDescription.ProfilerStatistics profilerStatistics = planDescription.getProfilerStatistics();
					count_minhop.add(profilerStatistics.getRows());
					access_minhop.add(OwnMethods.GetTotalDBHits(planDescription));
				}
			}
			minhop_Match.neo4j_API.ShutDown();
			
			Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(db_path);
			hot = false;
			for ( int i = 0; i < expe_count; i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];

				int j = 0;
				for (  ; j < query_Graph.graph.size(); j++)
					if(query_Graph.Has_Spa_Predicate[j])
						break;
				query_Graph.spa_predicate[j] = rectangle;
				
				if (hot == false)
				{
					Result result = naive_Neo4j_Match.SubgraphMatch_Spa_API(query_Graph, -1);
					while(result.hasNext())
						result.next();
					hot = true;
					i--;
					continue;
				}

				if(!TEST_FORMAT)
				{
					OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));

					start = System.currentTimeMillis();
					Result result = naive_Neo4j_Match.SubgraphMatch_Spa_API(query_Graph, limit);
					time = System.currentTimeMillis() - start;
					time_naive.add(time);	OwnMethods.Print("get itorator time: "+time);

					start = System.currentTimeMillis();
					while ( result.hasNext())
						result.next();
					time = System.currentTimeMillis() - start;
					time_naive_ser.add(time);	OwnMethods.Print("Iterate time: "+time);

					ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
					ExecutionPlanDescription.ProfilerStatistics profilerStatistics = planDescription.getProfilerStatistics();
					count_naive.add(profilerStatistics.getRows());
					access_naive.add(OwnMethods.GetTotalDBHits(planDescription));
				}
			}
			naive_Neo4j_Match.neo4j_API.ShutDown();

			for ( int i = 0; i < time_minhop.size(); i++)
			{
				write_line = String.format("%d\t%d\t%d\t%d\t", count_minhop.get(i), count_naive.get(i), time_minhop.get(i), time_naive.get(i));
				write_line += String.format("%d\t%d\t", time_minhop_ser.get(i), time_naive_ser.get(i));
				write_line += String.format("%d\t%d\t", time_minhop.get(i) + time_minhop_ser.get(i), time_naive.get(i) + time_naive_ser.get(i));
				write_line += String.format("%d\t%d\n", access_minhop.get(i), access_naive.get(i));
				if(!TEST_FORMAT)
					OwnMethods.WriteFile(result_detail_path, true, write_line);
			}
			write_line = String.valueOf(selectivity) + "\t";
			write_line += String.format("%d\t%d\t%d\t%d\t", Utility.Average(count_minhop), Utility.Average(count_naive), Utility.Average(time_minhop), Utility.Average(time_naive));
			write_line += String.format("%d\t%d\t", Utility.Average(time_minhop_ser), Utility.Average(time_naive_ser));
			write_line += String.format("%d\t%d\t", Utility.Average(time_minhop) + Utility.Average(time_minhop_ser), Utility.Average(time_naive) + Utility.Average(time_naive_ser));
			write_line += String.format("%d\t%d\t%d\n", Utility.Average(access_minhop), Utility.Average(access_naive), expe_count);
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_avg_path, true, write_line);
			
			long minhop_avg = Utility.Average(time_minhop) + Utility.Average(time_minhop_ser);
			long naive_avg = Utility.Average(time_naive) + Utility.Average(time_naive_ser);
			long larger_time = (minhop_avg > naive_avg ? minhop_avg:naive_avg);
			if (larger_time * expe_count > 150 * 1000)
				expe_count = (int) (expe_count * 0.5 / (larger_time * expe_count / 150.0 / 1000.0));
			if(expe_count < 1)
				expe_count = 1;
			
			time_minhop.clear();	time_naive.clear();
			time_minhop_ser.clear();time_naive_ser.clear();
			count_minhop.clear();	count_naive.clear();
			access_minhop.clear();	access_naive.clear();
			
			name_suffix *= times;
		}
	}

}
