package org.datasyslab.GeoGraphMatch;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;

import commons.*;

public class Experiment_Prepare {

	private static String dataset = "Gowalla";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		GenerateHMBRRatioIndex();
	}
	
	public static void GenerateHMBRRatioIndex()
	{
		try {
			String filepath = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/graph.txt";
			int node_count = OwnMethods.GetNodeCountGeneral(filepath);
			ArrayList<Integer> whole_ids = new ArrayList<Integer>(node_count);
			for ( int i = 0; i < node_count; i++)
				whole_ids.add(i);
			
			int name_suffix = 250000;
			while (name_suffix < 800000)
			{
				double selectivity = name_suffix / 1000000.0;
				int index_count = (int) (selectivity * node_count);
				ArrayList<Integer> index_ids = OwnMethods.GetRandom_NoDuplicate(whole_ids, index_count);
				Collections.sort(index_ids);
				String write_filepath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/HMBR_%d_index.txt", dataset, name_suffix);
				OwnMethods.WriteArray(write_filepath, index_ids);
				name_suffix += 250000;
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
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
			OwnMethods.Print(k);
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
				
				write_line += String.format("%s\t%s\t%s\t%s\n", String.valueOf(minx), String.valueOf(miny)
						, String.valueOf(maxx), String.valueOf(maxy));
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
		
		double selectivity = 0.000001;
		int times = 10;
		while ( selectivity < 0.000002)
		{
			String queryrect_path = String.
					format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", 
					dataset, (int)(selectivity * 1000000));
			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			
			String output_path = String.
					format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/selectivity.txt", 
							dataset);
			
			String write_line = selectivity + "\n";
			for ( MyRectangle rectangle : queryrect)
			{
				java.util.List<Point> result = stRtree.query(new Envelope(rectangle.min_x, rectangle.max_x,
						rectangle.min_y, rectangle.max_y));
				write_line += String.format("%s\t%d\n", String.valueOf(result.size() / 1280953.0), result.size());
			}
			OwnMethods.WriteFile(output_path, true, write_line + "\n");
			selectivity *= times;
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
	
}
