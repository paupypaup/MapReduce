import com.google.gson.JsonArray;

import java.util.ArrayList;

public class Metadata {
    private String name;
    private int numberOfPages;
    private int pageSize;
    private int size;
    private JsonArray pages;

    public Metadata(String n, int s, JsonArray p){
        name = n;
        numberOfPages = p.size();
        pageSize = 1024;
        size = s;
        pages = p;
    }

}
