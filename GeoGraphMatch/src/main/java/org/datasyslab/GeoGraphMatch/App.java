package org.datasyslab.GeoGraphMatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import javax.management.Query;

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
//    	arbitary();
    	neo4j_query_test();
    }
    
    
    public static void neo4j_query_test()
    {
//    	String querygraph_path = "/home/yuhansun/Documents/GeoGraphMatchData/data/QuerySet/hprd25d";
//    	String querygraph_path = "/home/yuhansun/Documents/GeoGraphMatchData/test_query_graph";
    	String querygraph_path = "/home/yuhansun/Documents/GeoGraphMatchData/data/QuerySet/human10s";
    	
//      Query_Graph query_Graph = new Query_Graph(3);
      
//      ArrayList<Integer> node0 = new ArrayList<Integer>(Arrays.asList(1));
//      ArrayList<Integer> node1 = new ArrayList<Integer>(Arrays.asList(0,2));
//      ArrayList<Integer> node2 = new ArrayList<Integer>(Arrays.asList(1));
//      query_Graph.graph = new ArrayList<ArrayList<Integer>>(Arrays.asList(node0, node1, node2));
//      query_Graph.label_list = new int[] {1, 2, 10};
//      query_Graph.label_list = new int[] {0, 0, 1};
//      query_Graph.spa_predicate[2] = new MyRectangle(114.735827,16.403898,115.975189,17.643261);
//      query_Graph.spa_predicate[2] = new MyRectangle(-100, -45, 100, 45);
    	
    	String transfer_table_path = "/home/yuhansun/Documents/GeoGraphMatchData/data/transfertable_hprd.txt";
    	HashMap<Integer, Integer> transfer_table = Utility.Read_Transfer_Table(transfer_table_path);
    	ArrayList<Query_Graph> query_graphs= Utility.ReadQueryGraphs(querygraph_path, transfer_table);
    	Query_Graph query_Graph = query_graphs.get(0);
    	
        Neo4j_Graph_Store p_Neo4j_Graph_Store = new Neo4j_Graph_Store();
        

        
//      p_Neo4j_Graph_Store.SubgraphMatch(query_Graph, 1000);
//        p_Neo4j_Graph_Store.Explain_SubgraphMatch_Spa(query_Graph);
        p_Neo4j_Graph_Store.Explain_SubgraphMatch(query_Graph);
        
//        p_Neo4j_Graph_Store.SubgraphMatch_Spa(query_Graph, 1000);
        
        
        
        
        
//      ArrayList<Query_Graph> query_Graphs = Utility.ReadQueryGraphs(query_graphs_path, transfer_table);
//        p_Neo4j_Graph_Store.SubgraphMatch(query_Graphs.get(0), 1000);
    }
}
