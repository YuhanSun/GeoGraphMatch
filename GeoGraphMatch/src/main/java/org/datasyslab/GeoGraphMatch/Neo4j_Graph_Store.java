package org.datasyslab.GeoGraphMatch;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.RequestBuilder;
import com.sun.jersey.api.client.WebResource;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import commons.*;

/*
 * This class specifies class file version 49.0 but uses Java 6 signatures.  Assumed Java 6.
 */
public class Neo4j_Graph_Store
implements Graph_Store_Operation {
    private String SERVER_ROOT_URI;
    private String lon_name;
    private String lat_name;
    private WebResource resource;

    public Neo4j_Graph_Store() {
        Config config = new Config();
        this.SERVER_ROOT_URI = config.GetServerRoot();
        this.lon_name = config.GetLongitudePropertyName();
        this.lat_name = config.GetLatitudePropertyName();
        String txUri = String.valueOf(this.SERVER_ROOT_URI) + "/transaction/commit";
        this.resource = Client.create().resource(txUri);
    }

    public WebResource GetCypherResource() {
        return this.resource;
    }

    public WebResource GetRangeQueryResource() {
        String range_query = String.valueOf(this.SERVER_ROOT_URI) + "/ext/SpatialPlugin/graphdb/findGeometriesInBBox";
        WebResource resource = Client.create().resource(range_query);
        return resource;
    }

    public static String StartMyServer(String datasource) {
        String command = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.2.3/bin/neo4j start";
        String result = null;
        try {
            String line;
            Process process = Runtime.getRuntime().exec(command);
            process.waitFor();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuffer sb = new StringBuffer();
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            result = sb.toString();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String StartServer(String database_path) {
        String command = String.valueOf(database_path) + "/bin/neo4j start";
        String result = null;
        try {
            String line;
            Process process = Runtime.getRuntime().exec(command);
            process.waitFor();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuffer sb = new StringBuffer();
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            result = sb.toString();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String StopMyServer(String datasource) {
        String command = "/home/yuhansun/Documents/Real_data/" + datasource + "/neo4j-community-2.2.3/bin/neo4j stop";
        String result = null;
        try {
            String line;
            Process process = Runtime.getRuntime().exec(command);
            process.waitFor();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuffer sb = new StringBuffer();
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            result = sb.toString();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String StopServer(String database_path) {
        String command = String.valueOf(database_path) + "/bin/neo4j stop";
        String result = null;
        try {
            String line;
            Process process = Runtime.getRuntime().exec(command);
            process.waitFor();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuffer sb = new StringBuffer();
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            result = sb.toString();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String Execute(WebResource resource, String query) {
        String payload = "{\"statements\" : [ {\"statement\" : \"" + query + "\"} ]}";
        ClientResponse response = (ClientResponse)((WebResource.Builder)((WebResource.Builder)resource.accept(new String[]{"application/json"}).type("application/json")).entity((Object)payload)).post(ClientResponse.class);
        String result = (String)response.getEntity(String.class);
        response.close();
        return result;
    }

    public String Execute(String query) {
        String payload = "{\"statements\" : [ {\"statement\" : \"" + query + "\"} ]}";
        ClientResponse response = (ClientResponse)((WebResource.Builder)((WebResource.Builder)this.resource.accept(new String[]{"application/json"}).type("application/json")).entity((Object)payload)).post(ClientResponse.class);
        String result = (String)response.getEntity(String.class);
        response.close();
        return result;
    }

    public static ArrayList<String> GetExecuteResultData(String result) {
        JsonArray jsonArr = null;
        try {
            JsonParser jsonParser = new JsonParser();
            JsonObject jsonObject = (JsonObject)jsonParser.parse(result);
            jsonArr = (JsonArray)jsonObject.get("results");
            jsonObject = (JsonObject)jsonArr.get(0);
            jsonArr = (JsonArray)jsonObject.get("data");
            ArrayList<String> l = new ArrayList<String>();
            int i = 0;
            while (i < jsonArr.size()) {
                jsonObject = (JsonObject)jsonArr.get(i);
                String str = jsonObject.get("row").toString();
                str = str.substring(1, str.length() - 1);
                l.add(str);
                ++i;
            }
            return l;
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println("\n" + result);
            System.out.println("\n" + jsonArr.getAsString());
            return null;
        }
    }

    public static JsonArray GetExecuteResultDataASJsonArray(String result) {
        HashSet hs = new HashSet();
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = (JsonObject)jsonParser.parse(result);
        JsonArray jsonArr = (JsonArray)jsonObject.get("results");
        if (jsonArr.size() == 0) {
            System.out.println(result);
            return null;
        }
        jsonObject = (JsonObject)jsonArr.get(0);
        jsonArr = (JsonArray)jsonObject.get("data");
        return jsonArr;
    }

    public static HashSet<Integer> GetExecuteResultDataInSet(String result) {
        HashSet<Integer> hs = new HashSet<Integer>();
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = (JsonObject)jsonParser.parse(result);
        JsonArray jsonArr = (JsonArray)jsonObject.get("results");
        if (jsonArr.size() == 0) {
            return null;
        }
        jsonObject = (JsonObject)jsonArr.get(0);
        jsonArr = (JsonArray)jsonObject.get("data");
        int i = 0;
        while (i < jsonArr.size()) {
            JsonObject jsonOb = (JsonObject)jsonArr.get(i);
            String row = jsonOb.get("row").toString();
            row = row.substring(1, row.length() - 1);
            hs.add(Integer.parseInt(row));
            ++i;
        }
        return hs;
    }

    public static void Graph_Traversal(WebResource resource, long start_id, HashSet<Long> VisitedVertices) {
        String query = "match (a)-->(b) where id(a) = " + Long.toString(start_id) + " return id(b)";
        String result = Neo4j_Graph_Store.Execute(resource, query);
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = (JsonObject)jsonParser.parse(result);
        JsonArray jsonArr = (JsonArray)jsonObject.get("results");
        jsonObject = (JsonObject)jsonArr.get(0);
        jsonArr = (JsonArray)jsonObject.get("data");
        int i = 0;
        while (i < jsonArr.size()) {
            jsonObject = (JsonObject)jsonArr.get(i);
            JsonArray row = (JsonArray)jsonObject.get("row");
            int id = row.get(0).getAsInt();
            if (!VisitedVertices.contains(id)) {
                VisitedVertices.add(Long.valueOf(id));
                Neo4j_Graph_Store.Graph_Traversal(resource, id, VisitedVertices);
            }
            ++i;
        }
    }

    public ArrayList<Integer> GetAllVertices() {
        String query = "match (a:Graph_node) return id(a)";
        String result = Neo4j_Graph_Store.Execute(this.resource, query);
        ArrayList<Integer> l = new ArrayList<Integer>();
        return l;
    }

    public ArrayList<Integer> GetSpatialVertices() {
        String query = "match (a:Graph_node) where has (a." + this.lon_name + ") return id(a)";
        String result = Neo4j_Graph_Store.Execute(this.resource, query);
        HashSet<Integer> hs = Neo4j_Graph_Store.GetExecuteResultDataInSet(result);
        ArrayList<Integer> l = new ArrayList<Integer>();
        Iterator<Integer> iter = hs.iterator();
        while (iter.hasNext()) {
            l.add(iter.next());
        }
        return l;
    }

    public JsonObject GetVertexAllAttributes(long id) {
        String query = "match (a) where id(a) = " + Long.toString(id) + " return a";
        String result = Neo4j_Graph_Store.Execute(this.resource, query);
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = null;
        try {
            jsonObject = (JsonObject)jsonParser.parse(result);
            JsonArray jsonArr = (JsonArray)jsonObject.get("results");
            jsonObject = (JsonObject)jsonArr.get(0);
            jsonArr = (JsonArray)jsonObject.get("data");
            jsonObject = (JsonObject)jsonArr.get(0);
            jsonArr = (JsonArray)jsonObject.get("row");
            jsonObject = (JsonObject)jsonArr.get(0);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println("\n" + result);
        }
        return jsonObject;
    }

    public JsonArray GetVertexAttributes(long id, ArrayList<String> property_name_array) {
        String query = "match (n) where id(n) = " + id + " return ";
        int i = 0;
        while (i < property_name_array.size() - 1) {
            query = String.valueOf(query) + "n." + property_name_array.get(i) + ",";
            ++i;
        }
        query = String.valueOf(query) + "n." + property_name_array.get(i);
        String result = this.Execute(query);
        JsonArray jsonArr = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
        jsonArr = jsonArr.get(0).getAsJsonObject().get("row").getAsJsonArray();
        return jsonArr;
    }

    public JsonArray GetVertexIDandAllAttributes(long id) {
        String query = "match (a) where id(a) = " + Long.toString(id) + " return id(a), a";
        String result = Neo4j_Graph_Store.Execute(this.resource, query);
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = (JsonObject)jsonParser.parse(result);
        JsonArray jsonArr = (JsonArray)jsonObject.get("results");
        jsonObject = (JsonObject)jsonArr.get(0);
        jsonArr = (JsonArray)jsonObject.get("data");
        jsonObject = (JsonObject)jsonArr.get(0);
        jsonArr = (JsonArray)jsonObject.get("row");
        return jsonArr;
    }

    public ArrayList<Integer> GetOutNeighbors(int id) {
        ArrayList<Integer> l = new ArrayList<Integer>();
        String query = "match (a)-[]->(b) where id(a) = " + Integer.toString(id) + " return id(b)";
        String result = Neo4j_Graph_Store.Execute(this.resource, query);
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = (JsonObject)jsonParser.parse(result);
        JsonArray jsonArr = (JsonArray)jsonObject.get("results");
        jsonObject = (JsonObject)jsonArr.get(0);
        jsonArr = (JsonArray)jsonObject.get("data");
        int i = 0;
        while (i < jsonArr.size()) {
            jsonObject = (JsonObject)jsonArr.get(i);
            JsonElement jsonElement = jsonObject.get("row");
            String row = jsonElement.toString();
            row = row.substring(1, row.length() - 1);
            l.add(Integer.parseInt(row));
            ++i;
        }
        return l;
    }

    public ArrayList<Integer> GetInNeighbors(int id) {
        ArrayList<Integer> l = new ArrayList<Integer>();
        String query = "match (a)-[]->(b) where id(b) = " + Integer.toString(id) + " return id(a)";
        String result = Neo4j_Graph_Store.Execute(this.resource, query);
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = (JsonObject)jsonParser.parse(result);
        JsonArray jsonArr = (JsonArray)jsonObject.get("results");
        jsonObject = (JsonObject)jsonArr.get(0);
        jsonArr = (JsonArray)jsonObject.get("data");
        int i = 0;
        while (i < jsonArr.size()) {
            jsonObject = (JsonObject)jsonArr.get(i);
            JsonElement jsonElement = jsonObject.get("row");
            String row = jsonElement.toString();
            row = row.substring(1, row.length() - 1);
            l.add(Integer.parseInt(row));
            ++i;
        }
        return l;
    }

    public String GetVertexAttributeValue(long id, String attributename) {
        JsonParser jsonParser = new JsonParser();
        String query = "match (a) where id(a) = " + Long.toString(id) + " return a." + attributename;
        String result = Neo4j_Graph_Store.Execute(this.resource, query);
        JsonObject jsonObject = (JsonObject)jsonParser.parse(result);
        JsonArray errors = (JsonArray)jsonObject.get("errors");
        if (!errors.toString().equals("[]")) {
            return null;
        }
        JsonArray jsonArray = (JsonArray)jsonObject.get("results");
        jsonObject = (JsonObject)jsonArray.get(0);
        jsonArray = (JsonArray)jsonObject.get("data");
        jsonObject = (JsonObject)jsonArray.get(0);
        String data = jsonObject.toString();
        String row = jsonObject.get("row").toString();
        if ((row = row.substring(1, row.length() - 1)).equals("null")) {
            return null;
        }
        return row;
    }

    public double[] GetVerticeLocation(int id) {
        double[] location = new double[2];
        String query = "match (a) where id(a) = " + id + " return a.longitude, a.latitude";
        ArrayList<String> result = Neo4j_Graph_Store.GetExecuteResultData(Neo4j_Graph_Store.Execute(this.resource, query));
        String data = result.get(0);
        String[] l = data.split(",");
        location[0] = Double.parseDouble(l[0]);
        location[1] = Double.parseDouble(l[1]);
        return location;
    }

    public String AddVertexAttribute(int id, String attributename, String value) {
        String query = "match (a) where id(a) = " + Integer.toString(id) + " set a." + attributename + "=" + value;
        String result = Neo4j_Graph_Store.Execute(this.resource, query);
        return result;
    }

    public boolean HasProperty(int id, String propertyname) {
        String value = this.GetVertexAttributeValue(id, propertyname);
        if (value == null) {
            return false;
        }
        return true;
    }

    public boolean IsSpatial(int id) {
        boolean has = this.HasProperty(id, "latitude");
        return has;
    }

    public int GetVertexID(String label, String attribute, String value) {
        String query = "match (a:" + label + ") where a." + attribute + " = " + value + " return id(a)";
        String result = Neo4j_Graph_Store.Execute(this.resource, query);
        HashSet<Integer> hs = Neo4j_Graph_Store.GetExecuteResultDataInSet(result);
        Iterator<Integer> iter = hs.iterator();
        int id = iter.next();
        return id;
    }

    public static boolean Location_In_Rect(double lat, double lon, MyRectangle rect) {
        if (lat < rect.min_y || lat > rect.max_y || lon < rect.min_x || lon > rect.max_x) {
            return false;
        }
        return true;
    }
    
    
    
}