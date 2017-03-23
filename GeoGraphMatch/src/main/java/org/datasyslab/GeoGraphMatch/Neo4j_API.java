package org.datasyslab.GeoGraphMatch;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class Neo4j_API {

	public GraphDatabaseService graphDb;
	
	public Neo4j_API(String dbpath)
	{
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbpath);
	}
	
	public void ShutDown()
	{
		if(graphDb!=null)
			graphDb.shutdown();
	}
	
	public Node GetNodeByID(long id)
	{
		Node node = null;
		try
		{
			node = graphDb.getNodeById(id);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return node;
	}
	
}
