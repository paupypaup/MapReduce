import java.nio.charset.StandardCharsets;
import java.util.*;
import java.io.*;
import java.nio.file.*;
import java.rmi.RemoteException;
import java.math.BigInteger;
import java.security.*;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

public class DFS {
	
    int port;
    Chord chord;
    
    private long md5(String objectName) {
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(objectName.getBytes());
            BigInteger bigInt = new BigInteger(1,m.digest());
            return Math.abs(bigInt.longValue());

        } catch(NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public DFS(int port) throws Exception {
        
        this.port = port;
        long guid = md5("" + port);
        chord = new Chord(port, guid);
        Files.createDirectories(Paths.get(guid+"/repository"));
        
        File file = new File(guid+"/repository/"+md5("Metadata"));
        
        if(!file.exists()){
            PrintWriter pr = new PrintWriter(file);
            pr.print("{\"metadata\":[]}");
            pr.close();
            file.createNewFile();
        }
    }
    
    public void join(String Ip, int port) throws Exception {
        chord.joinRing("localHost", port);
        //System.out.println(Ip);
        chord.Print();
    }
    
    public JsonReader readMetaData() throws Exception {
        //Gson jsonParser = null;
        long guid = md5("Metadata");
        ChordMessageInterface peer = chord.locateSuccessor(guid);
        InputStream metadataraw = peer.get(guid);
        // jsonParser = Json.createParser(metadataraw);
        JsonReader reader = new JsonReader(new InputStreamReader(metadataraw, "UTF-8"));
        return reader;
    }
    
    public void writeMetaData(InputStream stream) throws Exception {
        //JsonParser jsonParser _ null;
        long guid = md5("Metadata");
        ChordMessageInterface peer = chord.locateSuccessor(guid);
        peer.put(guid, stream);
    }
   
    public void mv(String oldName, String newName) throws Exception {
        JsonParser jp = new JsonParser();
        JsonReader jr = readMetaData();
        JsonObject metaData = (JsonObject)jp.parse(jr);
        JsonArray ja = metaData.getAsJsonArray("metadata");

        for(int i = 0; i < ja.size(); i++){
            JsonObject oldobj = ja.get(i).getAsJsonObject();
            String name = oldobj.get("name").getAsString();
            if (name.equals(oldName)) {
                JsonArray pageArray = oldobj.get("pages").getAsJsonArray();
                int size = oldobj.get("size").getAsInt();
                
                Metadata newmeta = new Metadata(newName, size, pageArray);

                Gson gson = new GsonBuilder().create();
                String json = gson.toJson(newmeta);

                JsonObject fileObj = new JsonParser().parse(json).getAsJsonObject();
                ja.remove(i);
                ja.add(fileObj);

                String s = metaData.toString();
                InputStream input = new FileStream(s.getBytes());
                writeMetaData(input);
                break;
            }
        }
    }
    
    public String ls() throws Exception {
        System.out.println("======= list =======");
        String result = "";

        JsonParser jp = new JsonParser();
        JsonReader jr = readMetaData();
        JsonObject metaData = (JsonObject)jp.parse(jr);
        JsonArray ja = metaData.getAsJsonArray("metadata");

        for(int i = 0; i < ja.size(); i++) {
            JsonObject jo = ja.get(i).getAsJsonObject();
            String name = jo.get("name").getAsString();
            result += name + "\n";
        }
        return result;
    }

    public void touch(String fileName) throws Exception {

        Metadata meta = new Metadata(fileName, 0, new JsonArray());
        Gson gson = new GsonBuilder().create();

        String json = gson.toJson(meta);

        JsonParser jp = new JsonParser();
        JsonReader jr = readMetaData();
        JsonObject metaData = (JsonObject)jp.parse(jr);
        JsonArray ja = metaData.getAsJsonArray("metadata");

        JsonObject fileObj = new JsonParser().parse(json).getAsJsonObject();
        ja.add(fileObj);

        String s = metaData.toString();
        InputStream input = new FileStream(s.getBytes());
        writeMetaData(input);

    }

    public void delete(String fileName) throws Exception {

        //create JsonArray
        JsonParser jp = new JsonParser();
        JsonObject metaData = (JsonObject)jp.parse(readMetaData());
        JsonArray ja = metaData.getAsJsonArray("metadata");

        //index to remove
        int del = -1;

        for(int i = 0; i < ja.size(); i++){
            //get JObj at index i
            JsonObject jo = ja.get(i).getAsJsonObject();

            //get name of file - e.g. value of key "name" of this object
            String name = jo.get("name").getAsString();
            
            if(name.equals(fileName)){

                //get pages as JsonArray
                JsonArray pages = jo.get("pages").getAsJsonArray();

                //delete each page
                for(int j = 0; j < pages.size(); j++){
                    JsonObject page = pages.get(j).getAsJsonObject();
                    long guid = md5("Metadata");
                    long pGuid = page.get("guid").getAsLong();
                    
                    ChordMessageInterface peer = chord.locateSuccessor(guid);
                    peer.delete(pGuid);
                }
                del = i;
            }
        }

        if (del > -1) {
            ja.remove(del);
            String s = metaData.toString();
            InputStream input = new FileStream(s.getBytes());
            writeMetaData(input);
        } else {
            System.out.println("There is no such file by that name.\n");
        }
    }

    public byte[] read(String fileName, int pageNumber) throws Exception {
        JsonParser jp = new JsonParser();
        JsonReader jr = readMetaData();
        JsonObject metaData = (JsonObject)jp.parse(jr);
        JsonArray ja = metaData.getAsJsonArray("metadata");

        byte[] result = null;

        for(int i = 0; i < ja.size(); i++){
            JsonObject jo = ja.get(i).getAsJsonObject();
            String name = jo.get("name").getAsString();
            if (name.equals(fileName)) {
                JsonArray pageArray = jo.get("pages").getAsJsonArray();
                //int index = 0;
                if(pageNumber != -1) {
                    //index = pageNumber-1;
                    for(int j = 0; j < pageArray.size(); j++) {
                        if(pageArray.get(j).getAsJsonObject().getAsJsonPrimitive("number").getAsInt() == (pageNumber)) {
                            long guidGet = pageArray.get(j).getAsJsonObject().getAsJsonPrimitive("guid").getAsLong();

                            long guid = md5("Metadata");
                            ChordMessageInterface peer = chord.locateSuccessor(guid);
                            InputStream is = peer.get(guidGet);

                            ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
                            int r = is.read();
                            while (r != 0) {
                                byteBuffer.write(r);
                                r = is.read();
                            }
                            byteBuffer.flush();
                            is.close();
                            result = byteBuffer.toByteArray();
                        }
                    }
                }
                else {
                    //index = pageArray.size()-1;
                    int j = pageArray.size()-1;
                    long guidGet = pageArray.get(j).getAsJsonObject().getAsJsonPrimitive("guid").getAsLong();
                    ChordMessageInterface peer = chord.locateSuccessor(guidGet);
                    InputStream is = peer.get(guidGet);

                    ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
                    int r = is.read();
                    while (r != 0) {
                        byteBuffer.write(r);
                        r = is.read();
                    }
                    byteBuffer.flush();
                    is.close();
                    result = byteBuffer.toByteArray();
                }
            }
        }
        return result;
    }
    
    public byte[] tail(String fileName) throws Exception
    {
        return read(fileName, -1);
    }
    
    public byte[] head(String fileName) throws Exception
    {
        return read(fileName, 1);
    }

    public void append(String fileName, byte[] data) throws Exception {

        int eight_counter = 0;

        JsonParser jp = new JsonParser();
        JsonReader jr = readMetaData();
        JsonObject metaData = (JsonObject)jp.parse(jr);
        JsonArray ja = metaData.getAsJsonArray("metadata");

        JsonObject toAppend = null;
        
        for(int i = 0; i < ja.size(); i++) {
            JsonObject jo = ja.get(i).getAsJsonObject();
            String name = jo.get("name").getAsString();
            if (name.equals(fileName)) {
                toAppend = jo;
                break;
            }
        }

        if (toAppend != null) {
            int maxSize = toAppend.get("pageSize").getAsInt();
            JsonArray pageArray = toAppend.get("pages").getAsJsonArray();
            int size = toAppend.get("size").getAsInt();
            String string = new String(data);

            ArrayList<Page> pages = new ArrayList<>();

            int pageNumber = size + 1;

            String newline = System.getProperty("line.separator");

            ArrayList <String> newpages = new ArrayList<>();
            int position = 100;
            while (position != -1){
                position = ordinalIndexOf(string, newline, 8);
                if (position != -1){
                    newpages.add(string.substring(0, position));
                    string = string.substring(position+2);
                } else {
                    newpages.add(string);
                }
            }

            byte [] subset;
            for (int i = 0; i < newpages.size(); i++){

                subset = newpages.get(i).getBytes();

                //Adding the actual file to the DFS
                InputStream is = new FileStream(subset);
                long guid = md5("Metadata");
                ChordMessageInterface peer = chord.locateSuccessor(guid);
                long guidPage = md5(fileName + pageNumber);
                peer.put(guidPage, is);

                //Adding the page to the ArrayList to update the metadata later
                pages.add(new Page(pageNumber, guidPage, subset.length));
                pageNumber++;
            }



            //Delete old metadata
            delete(fileName);

            //Write new metadata
            Gson ggson = new Gson();
            JsonElement element = ggson.toJsonTree(pages, new TypeToken<List<Page>>() {}.getType());

            if (! element.isJsonArray()) {
                throw new Exception();
            }

            Metadata meta = new Metadata(fileName, size+data.length, element.getAsJsonArray());
            Gson gson = new GsonBuilder().create();

            String json = gson.toJson(meta);

            jp = new JsonParser();
            jr = readMetaData();
            metaData = (JsonObject)jp.parse(jr);
            ja = metaData.getAsJsonArray("metadata");

            JsonObject fileObj = new JsonParser().parse(json).getAsJsonObject();
            ja.add(fileObj);

            String s = metaData.toString();
            InputStream input = new FileStream(s.getBytes());
            writeMetaData(input);


        } else {
            System.out.println("No such file exists in the DFS");
        }
    }

    public static int ordinalIndexOf(String str, String substr, int n) {
        int pos = str.indexOf(substr);
        while (--n > 0 && pos != -1)
            pos = str.indexOf(substr, pos + 1);
        return pos;
    }

    /**
     * This method runs the MapReduce when called which uses the MapReduceInterface to complete this task
     * @param fileName The file name to be affected by the MapReduce
     * @throws Exception 
     */
    public void runMapReduce(String fileName) throws Exception{
    	Mapper mapper = new Mapper();
    	
    	JsonParser jp = new JsonParser();
        JsonReader jr = readMetaData();
        JsonObject metaData = (JsonObject)jp.parse(jr);
        JsonArray ja = metaData.getAsJsonArray("metadata");
        
        for (int i = 0; i < ja.size(); i++) {
        	JsonObject jo = ja.get(i).getAsJsonObject();
            String name = jo.get("name").getAsString();
            if (name.equals(fileName)) {
            	JsonArray pages = jo.get("pages").getAsJsonArray();
                ChordMessageInterface peer;

                for (int j = 0; j < pages.size(); j++) {
                    long pageGUID = pages.get(j).getAsJsonObject().getAsJsonPrimitive("guid").getAsLong();
                    try {
                        peer = chord.locateSuccessor(md5("metadata"));
                        chord.mapContext(pageGUID, mapper, peer);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                while (!chord.isPhaseCompleted()){
                    Thread.sleep(1000);
                }
                chord.reduceContext(chord.getId(), mapper, chord);
            }
        }
    }
}
