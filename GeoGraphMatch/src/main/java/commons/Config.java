package commons;

public class Config { public Config() {}
  private String SERVER_ROOT_URI = "http://localhost:7474/db/data";
  
  private String longitude_property_name = "longitude";
  private String latitude_property_name = "latitude";
  
  private String Rect_minx_name = "minx";
  private String Rect_miny_name = "miny";
  private String Rect_maxx_name = "maxx";
  private String Rect_maxy_name = "maxy";
  
  public String GetServerRoot() {
    return SERVER_ROOT_URI;
  }
  
  public String GetLongitudePropertyName() {
    return longitude_property_name;
  }
  
  public String GetLatitudePropertyName() {
    return latitude_property_name;
  }
  
  public String [] GetRectCornerName() 
  {
	  String [] rect_corner_name = new String[4];
	  rect_corner_name[0] = this.Rect_minx_name;
	  rect_corner_name[1] = this.Rect_miny_name;
	  rect_corner_name[2] = this.Rect_maxx_name;
	  rect_corner_name[3] = this.Rect_maxy_name;
	  return rect_corner_name;
  }
}