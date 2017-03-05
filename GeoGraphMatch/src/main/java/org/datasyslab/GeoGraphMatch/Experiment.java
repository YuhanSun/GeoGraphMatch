package org.datasyslab.GeoGraphMatch;

import java.util.ArrayList;

import javax.management.Query;
import javax.rmi.CORBA.Util;

import org.neo4j.cypher.internal.compiler.v2_2.perty.recipe.Pretty.nest;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import com.google.gson.JsonArray;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

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
		
//		GenerateQueryRectangle();
//		GetRealSelectivity();
		
//		Query_Hot_API(0);
//		Query_Hot_API(3);
//		Query_Hot_API(2);
//		Query_Hot_API(4);
//		Query_Hot_API(5);
		Query_Hot_API(6);
//		Query_Hot_API(7);
//		Query_Hot_API(8);
//		Query_Hot_API(9);
//		Query_Hot_API(10);
		
		
		
//		GenerateRandomQueryGraph();
		
	}
	
	public static void GenerateRandomQueryGraph()
	{
		int node_count = 4;
		int spa_pred_count = 1;
		String datagraph_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
		ArrayList<ArrayList<Integer>> datagraph = OwnMethods.ReadGraph(datagraph_path);
		
		String entity_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
		ArrayList<Entity> entities = OwnMethods.ReadEntity(entity_path);
		
		ArrayList<Integer> labels = new ArrayList<Integer>(entities.size());
		for ( int i = 0; i < entities.size(); i++)
		{
			if(entities.get(i).IsSpatial)
				labels.add(1);
			else
				labels.add(0);
		}
		
		ArrayList<Query_Graph> query_Graphs = new ArrayList<Query_Graph>(10);
		for ( int i = 0; i < 10; i++)
		{
			Query_Graph query_Graph = OwnMethods.GenerateRandomGraph(datagraph, labels, 
				entities, node_count, spa_pred_count);
			OwnMethods.Print(query_Graph.toString() + "\n");
			query_Graphs.add(query_Graph);
		}
		String querygraph_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph/%d.txt", node_count);
		Utility.WriteQueryGraph(querygraph_path, query_Graphs);
		
	}
	
	public static void GenerateQueryRectangle() {
		int experiment_count = 50;
		String entity_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
		ArrayList<Entity> entities = OwnMethods.ReadEntity((String)entity_path);
		int spa_count = OwnMethods.GetSpatialEntityCount(entities);
		STRtree stRtree = OwnMethods.ConstructSTRee(entities);
		
		String center_id_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/"
				+ "%s/%s_centerids.txt", dataset, dataset);
		ArrayList<Integer> center_ids = OwnMethods.ReadCenterID(center_id_path);
		ArrayList<Integer> final_center_ids = OwnMethods.GetRandom_NoDuplicate(center_ids, experiment_count);
		
		double selectivity = 0.000001;
		int times = 10;
		while (selectivity <= 1)
		{
			int k = (int) (selectivity * spa_count);
			String write_line = "";
			for (int id : final_center_ids)
			{
				double lon = entities.get(id).lon;
				double lat = entities.get(id).lat;
				GeometryFactory factory = new GeometryFactory();
				Point center = factory.createPoint(new Coordinate(lon, lat));
				Object[] result = stRtree.kNearestNeighbour(center.getEnvelopeInternal(),
						new GeometryFactory().toGeometry(center.getEnvelopeInternal()),
						new GeometryItemDistance(), k);
				double radius = 0.0;
				for (Object object : result)
				{
					Point point = (Point) object;
					double dist = center.distance(point);
					if(dist > radius)
						radius = dist;
				}
				OwnMethods.Print(radius);
				double a = Math.sqrt(Math.PI) * radius;
				double minx = center.getX() - a / 2;
				double miny = center.getY() - a / 2;
				double maxx = center.getX() + a / 2;
				double maxy = center.getY() + a / 2;
				
				write_line += String.format("%f\t%f\t%f\t%f\n", minx, miny, maxx, maxy);
			}
			String output_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/"
					+ "query/spa_predicate/%s/queryrect_%s.txt", dataset, String.valueOf(selectivity));
			OwnMethods.WriteFile(output_path, true, write_line);
			selectivity *= times;
			
		}
	}
	
//	public static void GenerateQueryRectangle() {
//		int experiment_count = 50;
//		String entity_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
//		ArrayList<Entity> entities = OwnMethods.ReadEntity((String)entity_path);
//		int spa_count = OwnMethods.GetSpatialEntityCount(entities);
//		STRtree stRtree = OwnMethods.ConstructSTRee(entities);
//		
//		String center_id_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/"
//				+ "%s/%s_centerids.txt", dataset, dataset);
//		ArrayList<Integer> center_ids = OwnMethods.ReadCenterID(center_id_path);
//		ArrayList<Integer> final_center_ids = OwnMethods.GetRandom_NoDuplicate(center_ids, experiment_count);
//		
//		double base_selectivity = 0.005;
//		int times = 1;
//		while (times <= 8)
//		{
//			double selectivity = base_selectivity * times;
//			int k = (int) (selectivity * spa_count);
//			String write_line = "";
//			for (int id : final_center_ids)
//			{
//				double lon = entities.get(id).lon;
//				double lat = entities.get(id).lat;
//				GeometryFactory factory = new GeometryFactory();
//				Point center = factory.createPoint(new Coordinate(lon, lat));
//				Object[] result = stRtree.kNearestNeighbour(center.getEnvelopeInternal(),
//						new GeometryFactory().toGeometry(center.getEnvelopeInternal()),
//						new GeometryItemDistance(), k);
//				double radius = 0.0;
//				for (Object object : result)
//				{
//					Point point = (Point) object;
//					double dist = center.distance(point);
//					if(dist > radius)
//						radius = dist;
//				}
//				OwnMethods.Print(radius);
//				double a = Math.sqrt(Math.PI) * radius;
//				double minx = center.getX() - a / 2;
//				double miny = center.getY() - a / 2;
//				double maxx = center.getX() + a / 2;
//				double maxy = center.getY() + a / 2;
//				
//				write_line += String.format("%f\t%f\t%f\t%f\n", minx, miny, maxx, maxy);
//			}
//			String output_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/"
//					+ "query/spa_predicate/%s/queryrect_%d.txt", dataset, times);
//			OwnMethods.WriteFile(output_path, true, write_line);
//			times *= 2;
//			
//		}
//	}

	public static void GetRealSelectivity()
	{
		String entity_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/Gowalla/entity.txt";
		ArrayList<Entity> entities = OwnMethods.ReadEntity(entity_path);
		double spa_count = 1280953;
		STRtree stRtree = OwnMethods.ConstructSTRee(entities);
		
		int times = 1;
		double base_selectivity = 0.005;
		while ( times <= 8)
		{
			String queryrect_path = String.
					format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", 
					dataset, times);
			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			
			String output_path = String.
					format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/selectivity.txt", 
							dataset);
			
			double selectivity = base_selectivity * times;
			String write_line = selectivity + "\n";
			for ( MyRectangle rectangle : queryrect)
			{
				java.util.List<Point> result = stRtree.query(new Envelope(rectangle.min_x, rectangle.max_x,
						rectangle.min_y, rectangle.max_y));
				write_line += String.format("%f\t%d\n", result.size() / 1280953.0, result.size());
			}
			OwnMethods.WriteFile(output_path, true, write_line + "\n");
			times *= 2;
		}
	}
	
//	public static void GetRealSelectivity()
//	{
//		double selectivity = 0.000001;
//		int query_id = 3;
//		
//		String entity_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/Gowalla/entity.txt";
//		ArrayList<Entity> entities = OwnMethods.ReadEntity(entity_path);
//		double spa_count = 1280953;
//		STRtree stRtree = OwnMethods.ConstructSTRee(entities);
//		
//		while ( selectivity < 0.002)
//		{
//			int log = (int) Math.log10(selectivity);
//			String queryrect_path = String.
//					format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s_%d_%d.txt", 
//					dataset, log, query_id);
//			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
//			
//			String output_path = String.
//					format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/selectivity_%s_%d.txt", 
//							dataset, query_id);
//			
//			String write_line = selectivity + "\n";
//			for ( MyRectangle rectangle : queryrect)
//			{
//				java.util.List<Point> result = stRtree.query(new Envelope(rectangle.min_x, rectangle.max_x,
//						rectangle.min_y, rectangle.max_y));
//				write_line += String.format("%f\n", result.size() / 1280953.0);
//			}
//			OwnMethods.WriteFile(output_path, true, write_line + "\n");
//			selectivity *= 10;
//		}
//	}
	
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
		
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_Gowalla/data/graph.db";
		
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
		
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_Gowalla/data/graph.db";
		
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
		
		String write_line = String.format("%s\t%d\n", dataset, limit);
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
		
		double selectivity = 0.000001;
		int times = 10;
		while ( selectivity <= 0.2)
		{
			String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%s.txt", dataset, String.valueOf(selectivity));
			
			write_line = selectivity + "\n" + head_line;
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_detail_path, true, write_line);
			
			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			Minhop_Match minhop_Match = new Minhop_Match(db_path);
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
					Result result = minhop_Match.SubgraphMatch_Spa_API(query_Graph, limit);
					time = System.currentTimeMillis() - start;
					time_minhop.add(time);	OwnMethods.Print("get itorator time: "+time);


					start = System.currentTimeMillis();
					while ( result.hasNext())
					{
						result.next();
					}
					time = System.currentTimeMillis() - start;
					time_minhop_ser.add(time);	OwnMethods.Print("Iterate time: "+time);

					ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
					ExecutionPlanDescription.ProfilerStatistics profilerStatistics = planDescription.getProfilerStatistics();
					count_minhop.add(profilerStatistics.getRows());
					access_minhop.add(OwnMethods.GetTotalDBHits(planDescription));
				}
			}
			minhop_Match.neo4j_API.ShutDown();
			
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
			if (larger_time * expe_count > 450 * 1000)
				expe_count = (int) (expe_count * 0.5 / (larger_time * expe_count / 450.0 / 1000.0));
			if(expe_count < 1)
				expe_count = 1;
			
			time_minhop.clear();	time_naive.clear();
			time_minhop_ser.clear();time_naive_ser.clear();
			count_minhop.clear();	count_naive.clear();
			access_minhop.clear();	access_naive.clear();
			
			selectivity *= times;
		}
	}

}
