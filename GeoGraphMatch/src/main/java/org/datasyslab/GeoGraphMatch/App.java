package org.datasyslab.GeoGraphMatch;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void arbitary()
	{
//		String query = "match (a0)--(a1),(a0)--(a2),(a0)--(a3),(a0)--(a4),(a1)--(a0),(a1)--(a7),(a2)--(a0),(a2)--(a4),(a3)--(a0),(a3)--(a4),(a4)--(a0),(a4)--(a2),(a4)--(a3),(a4)--(a5),(a4)--(a6),(a4)--(a7),(a5)--(a4),(a5)--(a6),(a5)--(a7),(a6)--(a4),(a6)--(a5),(a6)--(a7),(a7)--(a1),(a7)--(a4),(a7)--(a5),(a7)--(a6),(a7)--(a8),(a8)--(a7),(a8)--(a9),(a9)--(a8) where id(a0) = 1423 and id(a1) = 1421 and id(a2) = 1427 and id(a3) = 1430 and id(a4) = 1417 and id(a5) = 1422 and id(a6) = 1420 and id(a7) = 1418 and id(a8) = 2594 and id(a9) = 1761 return a0,a1,a2,a3,a4,a5,a6,a7,a8,a9";
		ArrayList<Integer>[] NEC_Node_array = new ArrayList[3];
		NEC_Node_array[0] = new ArrayList<Integer>();
		ArrayList<Integer> node1 = NEC_Node_array [0];
		node1.add(100);
	}
	
    public static void main( String[] args )
    {
//    	CFLMatch_test(); 
    	arbitary();
    }
    
    
    public static void neo4j_query_test()
    {
    	String query_graphs_path = "/home/yuhansun/Documents/GeoGraphMatchData/hprd25d";
//    	String query_graphs_path = "/home/yuhansun/Documents/GeoGraphMatchData/test_query_graph";
//    	String query_graphs_path = "/home/yuhansun/Documents/GeoGraphMatchData/human10s";
//        ArrayList<Query_Graph> query_Graphs = Utility.ReadQueryGraphs(query_graphs_path, 1);
//        Neo4j_Graph_Store p_Neo4j_Graph_Store = new Neo4j_Graph_Store();
//        p_Neo4j_Graph_Store.SubgraphMatch(query_Graphs.get(0));
    }
}
