package neo4j_benchmark;

import java.util.ArrayList;
import java.util.Iterator;

import javax.management.relation.Relation;

import org.datasyslab.GeoGraphMatch.Naive_Neo4j_Match;
import org.neo4j.cypher.internal.compiler.v2_2.functions.Labels;
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.impl.util.register.NeoRegister.Node;

import commons.OwnMethods;
import commons.Query_Graph;
import commons.Utility;
import commons.Config;
import commons.Labels.GraphLabel;

public class Pipeline_Test {

	private static String dataset = "Gowalla";
	static String string = "";
	static Config config = new Config();
	static String lon_name = config.GetLongitudePropertyName();
	static String lat_name = config.GetLatitudePropertyName();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Test();
//		FromID_Test();
	}
	
	public static void FromID_Test()
	{
		try {
			{
				String string = "";
				Query_Graph query_Graph = new Query_Graph(2);
				query_Graph.label_list[0] = 0;
				query_Graph.label_list[1] = 1;
				query_Graph.graph.get(0).add(1);
				query_Graph.graph.get(1).add(0);
				
				String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_"+dataset+"/data/graph.db";
				String write_path = "/mnt/hgfs/Experiment_Result/neo4j_benchmark/pipeline.txt";
				
				Naive_Neo4j_Match match = new Naive_Neo4j_Match(db_path);
				
				ArrayList<Long> ids = new ArrayList<Long>(1000000);
				
				
				Transaction tx = match.neo4j_API.graphDb.beginTx();
				ResourceIterator<org.neo4j.graphdb.Node> nodes = match.neo4j_API.graphDb.findNodes(DynamicLabel.label("GRAPH_0"));
				
				int i = 0;
				while (nodes.hasNext())
				{
					org.neo4j.graphdb.Node node = nodes.next();
					ids.add(node.getId());
				}
				
				long start = System.currentTimeMillis();
				for (long id : ids)
				{
					org.neo4j.graphdb.Node node = match.neo4j_API.GetNodeByID(id);
					Iterable<Relationship> relationships = node.getRelationships();
					for ( Relationship rel : relationships)
					{
						org.neo4j.graphdb.Node child = rel.getOtherNode(node);
						if(child.hasLabel(GraphLabel.GRAPH_1))
						{
							i++;
							string = child.toString();
						}
					}
				}
				Long time = System.currentTimeMillis() - start;
				
				String writeline = String.format("api by id cold:%d\n", time);
				OwnMethods.WriteFile(write_path, true, writeline);
				tx.success();
				
				start = System.currentTimeMillis();
				tx = match.neo4j_API.graphDb.beginTx();
				i = 0;
				for (long id : ids)
				{
					org.neo4j.graphdb.Node node = match.neo4j_API.GetNodeByID(id);
					Iterable<Relationship> relationships = node.getRelationships();
					for ( Relationship rel : relationships)
					{
						org.neo4j.graphdb.Node child = rel.getOtherNode(node);
						if(child.hasLabel(GraphLabel.GRAPH_1))
						{
							i++;
							string = child.toString();
						}
					}
				}
				time = System.currentTimeMillis() - start;
				writeline = String.format("api by id time:%d\n", time);
				OwnMethods.WriteFile(write_path, true, writeline);
				tx.success();
				match.neo4j_API.ShutDown();
				OwnMethods.Print(i);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void Test()
	{
		try {
			{
				Query_Graph query_Graph = new Query_Graph(2);
				query_Graph.label_list[0] = 0;
				query_Graph.label_list[1] = 1;
				query_Graph.graph.get(0).add(1);
				query_Graph.graph.get(1).add(0);
				
				String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_"+dataset+"/data/graph.db";
				String write_path = "/mnt/hgfs/Experiment_Result/neo4j_benchmark/pipeline.txt";
				Naive_Neo4j_Match match = new Naive_Neo4j_Match(db_path);
				
				long start = System.currentTimeMillis();
				String query = Utility.FormCypherQuery(query_Graph, -1, 0, lon_name, lat_name);
				Result result = match.neo4j_API.graphDb.execute(query);
				int i = 0;
				while ( result.hasNext())
				{	
					result.next();
					string = result.toString();
					i++;
				}
				
				long time = System.currentTimeMillis() - start;
				String writeline = String.format("pipeline time cold: %d\n", time);
				OwnMethods.WriteFile(write_path, true, writeline);
				
				start = System.currentTimeMillis();
				result = match.neo4j_API.graphDb.execute(query);
				i = 0;
				while (result.hasNext())
				{
					result.next();
					string = result.toString();
					i++;
				}
				time = System.currentTimeMillis() - start;
				
				writeline = String.format("pipeline time: %d\n", time);
				OwnMethods.WriteFile(write_path, true, writeline);
				match.neo4j_API.ShutDown();
				OwnMethods.Print(i);
				
				
				match = new Naive_Neo4j_Match(db_path);
				Transaction tx = match.neo4j_API.graphDb.beginTx();
				start = System.currentTimeMillis();
				
				ResourceIterator<org.neo4j.graphdb.Node> nodes = match.neo4j_API.graphDb.findNodes(DynamicLabel.label("GRAPH_0"));
				while (nodes.hasNext())
				{
					org.neo4j.graphdb.Node node = nodes.next();
					Iterable<Relationship> relationships = node.getRelationships();
					for ( Relationship rel : relationships)
					{
						org.neo4j.graphdb.Node child = rel.getOtherNode(node);
						if(child.hasLabel(GraphLabel.GRAPH_1))
						{
							i++;
							string = child.toString();
						}
					}
				}
				time = System.currentTimeMillis() - start;
				writeline = String.format("api no pipeline time cold:%d\n", time);
				OwnMethods.WriteFile(write_path, true, writeline);
				tx.success();
				
				start = System.currentTimeMillis();
				tx = match.neo4j_API.graphDb.beginTx();
				nodes = match.neo4j_API.graphDb.findNodes(GraphLabel.GRAPH_0);
				i = 0;
				while (nodes.hasNext())
				{
					org.neo4j.graphdb.Node node = nodes.next();
					Iterable<Relationship> relationships = node.getRelationships();
					for ( Relationship rel : relationships)
					{
						org.neo4j.graphdb.Node child = rel.getOtherNode(node);
						if(child.hasLabel(GraphLabel.GRAPH_1))
						{
							i++;
							string = child.toString();
						}
					}
				}
				time = System.currentTimeMillis() - start;
				writeline = String.format("api no pipeline time:%d\n\n", time);
				OwnMethods.WriteFile(write_path, true, writeline);
				tx.success();
				match.neo4j_API.ShutDown();
				OwnMethods.Print(i);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
