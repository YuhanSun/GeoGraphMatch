package org.datasyslab.GeoGraphMatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.management.Query;
import javax.swing.tree.DefaultMutableTreeNode;

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.greedy.expand;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void arbitary()
	{
//		String query = "match (a0)--(a1),(a0)--(a2),(a0)--(a3),(a0)--(a4),(a1)--(a0),(a1)--(a7),(a2)--(a0),(a2)--(a4),(a3)--(a0),(a3)--(a4),(a4)--(a0),(a4)--(a2),(a4)--(a3),(a4)--(a5),(a4)--(a6),(a4)--(a7),(a5)--(a4),(a5)--(a6),(a5)--(a7),(a6)--(a4),(a6)--(a5),(a6)--(a7),(a7)--(a1),(a7)--(a4),(a7)--(a5),(a7)--(a6),(a7)--(a8),(a8)--(a7),(a8)--(a9),(a9)--(a8) where id(a0) = 1423 and id(a1) = 1421 and id(a2) = 1427 and id(a3) = 1430 and id(a4) = 1417 and id(a5) = 1422 and id(a6) = 1420 and id(a7) = 1418 and id(a8) = 2594 and id(a9) = 1761 return a0,a1,a2,a3,a4,a5,a6,a7,a8,a9";
//		ArrayList<Integer>[] NEC_Node_array = new ArrayList[3];
//		NEC_Node_array[0] = new ArrayList<Integer>();
//		ArrayList<Integer> node1 = NEC_Node_array [0];
//		node1.add(100);
		String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-2.3.3_"+ dataset +"/data/graph.db";
		
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		double selectivity = 0.000001;
		String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%s.txt", dataset, String.valueOf(selectivity));
		
		ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
		query_Graph.spa_predicate[1] = queryrect.get(0);
		Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(db_path);
		Result result = naive_Neo4j_Match.Explain_SubgraphMatch_Spa_API(query_Graph, -1);
//		OwnMethods.Print(result.getExecutionPlanDescription().toString());
		ExecutionPlanDescription plan = result.getExecutionPlanDescription();
		OwnMethods.Print(plan.toString());
		
		DefaultMutableTreeNode root = OwnMethods.GetExecutionPlanTree(plan, 5);
		Queue<DefaultMutableTreeNode> queue = new LinkedList<DefaultMutableTreeNode>();
		queue.add(root);
		while(queue.isEmpty() == false)
		{
			DefaultMutableTreeNode cur_node = queue.poll();
			String line = "";
			line = cur_node.getUserObject().toString() + " children:";
			for (Enumeration<DefaultMutableTreeNode> e = cur_node.children(); e.hasMoreElements();)
			{
				DefaultMutableTreeNode child_node = e.nextElement();
				line += " " + child_node.getUserObject().toString();
				queue.add(child_node);
			}
			OwnMethods.Print(line);
		}
		
//		Queue<ExecutionPlanDescription> queue = new LinkedList<ExecutionPlanDescription>();
//		Queue<ExecutionPlanDescription> result_queue = new LinkedList<ExecutionPlanDescription>();
//		queue.add(plan);
//		result_queue.add(plan);
//		while(queue.isEmpty() == false)
//		{
//			ExecutionPlanDescription cur_plan = queue.poll();
//			List<ExecutionPlanDescription> child_plans = cur_plan.getChildren();
//			for (ExecutionPlanDescription child_plan : child_plans)
//			{
//				queue.add(child_plan);
//				result_queue.add(child_plan);
//			}
//			
//		}
//		while(result_queue.isEmpty() == false)
//		{
//			ExecutionPlanDescription cur_plan = result_queue.poll();
//			OwnMethods.Print("Plan name: " + cur_plan.getName());
//			OwnMethods.Print("Plan identifiers:" + cur_plan.getIdentifiers());
//			OwnMethods.Print("Plan arguments: " + cur_plan.getArguments() + "\n");
//			Map<String, Object> argument = cur_plan.getArguments();
//			if(argument.containsKey("ExpandExpression"))
//			{
//				OwnMethods.Print(argument.get("ExpandExpression"));
//				OwnMethods.Print(argument.get("ExpandExpression").toString().split("--")[1]);
//			}
//			
//		}
		
		naive_Neo4j_Match.neo4j_API.ShutDown();
	}
	
	static String dataset = "Gowalla";
	static int query_id = 9;
	
    public static void main( String[] args )
    {
//    	CFLMatch_test(); 
//    	arbitary();
//    	neo4j_query_test();
    	arbitary();
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
//        p_Neo4j_Graph_Store.Explain_SubgraphMatch(query_Graph);
        
//        p_Neo4j_Graph_Store.SubgraphMatch_Spa(query_Graph, 1000);
        
        
        
        
        
//      ArrayList<Query_Graph> query_Graphs = Utility.ReadQueryGraphs(query_graphs_path, transfer_table);
//        p_Neo4j_Graph_Store.SubgraphMatch(query_Graphs.get(0), 1000);
    }
}
