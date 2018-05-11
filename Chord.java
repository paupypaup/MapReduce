package mapreduce.MapReduce;

import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.net.*;
import java.util.*;
import java.io.*;

public class Chord extends java.rmi.server.UnicastRemoteObject implements ChordMessageInterface {
    private static final int M = 2;

    private Registry registry;    // rmi registry for lookup the remote objects.
    private ChordMessageInterface successor;
    private ChordMessageInterface predecessor;
    private ChordMessageInterface[] finger;
    private int nextFinger;
    private long guid;   		// GUID (i)
    private int numOfRecords;
    private Set<Long> set = new HashSet<Long>();
    private TreeMap <Long, String> reduceTree;
    private TreeMap <Long, List<String>> BMap;

    public Chord(int port, long guid) throws RemoteException {
        reduceTree = new TreeMap<>();
        BMap = new TreeMap<>();
        int j;
        finger = new ChordMessageInterface[M];
        for (j=0;j<M; j++){
            finger[j] = null;
        }
        this.guid = guid;

        predecessor = this;
        successor = this;
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                stabilize();
                fixFingers();
                checkPredecessor();
            }
        }, 500, 500);
        try{
            // create the registry and bind the name and object.
            System.out.println(guid + " is starting RMI at port="+port);
            registry = LocateRegistry.createRegistry( port );
            registry.rebind("Chord", this);
        }
        catch(RemoteException e){
            throw e;
        }
    }


    public Boolean isKeyInSemiCloseInterval(long key, long key1, long key2) {
       if (key1 < key2)
           return (key > key1 && key <= key2);
      else
          return (key > key1 || key <= key2);
    }

    public Boolean isKeyInOpenInterval(long key, long key1, long key2) {
      if (key1 < key2)
          return (key > key1 && key < key2);
      else
          return (key > key1 || key < key2);
    }

    public void put(long guidObject, InputStream stream) throws RemoteException {
      try {
          String fileName = "./"+guid+"/repository/" + guidObject;
          FileOutputStream output = new FileOutputStream(fileName);
          while (stream.available() > 0)
              output.write(stream.read());
          output.close();
      }
      catch (IOException e) {
          System.out.println(e);
      }
    }


    public InputStream get(long guidObject) throws RemoteException {
        FileStream file = null;
        try {
             file = new FileStream("./"+guid+"/repository/" + guidObject);
        } catch (IOException e)
        {
            throw(new RemoteException("File does not exists"));
        }
        return file;
    }

    public void delete(long guidObject) throws RemoteException {
        File file = new File("./"+guid+"/repository/" + guidObject);
        file.delete();
    }

    public long getId() throws RemoteException {
        return guid;
    }
    public boolean isAlive() throws RemoteException {
	    return true;
    }

    public ChordMessageInterface getPredecessor() throws RemoteException {
	    return predecessor;
    }

	public ChordMessageInterface getSuccessor() throws RemoteException {
	    return successor;
	}

    public ChordMessageInterface locateSuccessor(long key) throws RemoteException {
	    if (key == guid)
            throw new IllegalArgumentException("Key must be distinct that  " + guid);
	    if (successor.getId() != guid)
	    {
	      if (isKeyInSemiCloseInterval(key, guid, successor.getId()))
	        return successor;
	      ChordMessageInterface j = closestPrecedingNode(key);

          if (j == null)
	        return null;
	      return j.locateSuccessor(key);
        }
        return successor;
    }

    public ChordMessageInterface closestPrecedingNode(long key) throws RemoteException {
        if(key != guid) {
            int i = M - 1;
            while (i >= 0) {
                try{

                    if(isKeyInSemiCloseInterval(finger[i].getId(), guid, key)) {
                        if(finger[i].getId() != key)
                            return finger[i];
                        else {
                            return successor;
                        }
                    }
                }
                catch(Exception e)
                {
                    // Skip ;
                }
                i--;
            }
        }
        return successor;
    }

    public void joinRing(String ip, int port)  throws RemoteException {
        try{
            System.out.println("Get Registry to joining ring");
            Registry registry = LocateRegistry.getRegistry(ip, port);
            ChordMessageInterface chord = (ChordMessageInterface)(registry.lookup("Chord"));
            predecessor = null;
            successor = chord.locateSuccessor(this.getId());
            System.out.println("Joining ring");
        }
        catch(RemoteException | NotBoundException e){
            successor = this;
        }
    }

    public void findingNextSuccessor(){
        int i;
        successor = this;
        for (i = 0;  i< M; i++)
        {
            try
            {
                if (finger[i].isAlive())
                {
                    successor = finger[i];
                }
            }
            catch(RemoteException | NullPointerException e)
            {
                finger[i] = null;
            }
        }
    }

    public void stabilize() {
        try {
            if (successor != null) {
                ChordMessageInterface x = successor.getPredecessor();

                if (x != null && x.getId() != this.getId() && isKeyInOpenInterval(x.getId(), this.getId(), successor.getId())) {
                  successor = x;
                }
                if (successor.getId() != getId()) {
                  successor.notify(this);
                }
            }
        } catch(RemoteException | NullPointerException e1) {
            findingNextSuccessor();
        }
    }

    public void notify(ChordMessageInterface j) throws RemoteException {
         if (predecessor == null || (isKeyInOpenInterval(j.getId(), predecessor.getId(), guid)))
             predecessor = j;
            try {
                File folder = new File("./"+guid+"/repository/");
                File[] files = folder.listFiles();
                for (File file : files) {
                    long guidObject = Long.valueOf(file.getName());
                    if(guidObject < predecessor.getId() && predecessor.getId() < guid) {
                        predecessor.put(guidObject, new FileStream(file.getPath()));
                        file.delete();
                    }
                }
                } catch (ArrayIndexOutOfBoundsException e) {
                //happens sometimes when a new file is added during foreach loop
            } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void fixFingers() {
        long id = guid;
        try {
            long nextId = this.getId() + 1 << (nextFinger + 1);
            finger[nextFinger] = locateSuccessor(nextId);

            if (finger[nextFinger].getId() == guid)
                finger[nextFinger] = null;
            else
                nextFinger = (nextFinger + 1) % M;
        }
        catch(RemoteException | NullPointerException e){
            e.printStackTrace();
        }
    }

    public void checkPredecessor() {
      try {
          if (predecessor != null && !predecessor.isAlive())
              predecessor = null;
      }
      catch(RemoteException e)
      {
          predecessor = null;
//           e.printStackTrace();
      }
    }


    void Print()
    {
        int i;
        try {
            if (successor != null)
                System.out.println("successor "+ successor.getId());
            if (predecessor != null)
                System.out.println("predecessor "+ predecessor.getId());
            for (i=0; i<M; i++)
            {
                try {
                    if (finger != null)
                        System.out.println("Finger "+ i + " " + finger[i].getId());
                } catch(NullPointerException e)
                {
                    finger[i] = null;
                }
            }
        }
        catch(RemoteException e){
	       System.out.println("Cannot retrieve id");
        }
    }

    public void setWorkingPeer(Long page) {
        set.add(page);
    }

    public void completePeer(Long page, int n) throws RemoteException{
    	this.numOfRecords += n;
        set.remove(page);
    }
    

    public Boolean isPhaseCompleted(){
        return set.isEmpty();
    }
    

    public void reduceContext(Long source, MapReduceInterface reducer, ChordMessageInterface context) throws RemoteException {
        System.out.println("============running reduceContext=========");
        if (source != guid) {
        	successor.reduceContext(source, reducer, context);
        }
        context.setWorkingPeer(source);
        Thread reduceThread = new Thread(() -> {
            Set<Long> keys = BMap.keySet();
            try {
                for (Long key : keys) {
                    List<String> getList = BMap.get(key);
                    reducer.reduce(key, getList, context);
                    this.completePeer(source, 1);

                    if (reduceTree.size() == BMap.size()){
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        reduceThread.start();
        try {
            reduceThread.join();



//            Set<Long> keys = reduceTree.keySet();
//
//            for (Long key : keys) {
//                String s = reduceTree.get(key);
//
//                System.out.println(s);
//            }



        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {

            String aggFileName = "reduced.txt";
            FileWriter fstream = new FileWriter(aggFileName);
            BufferedWriter out = new BufferedWriter(fstream);

            for (Map.Entry<Long, String> entry : reduceTree.entrySet()) {
                out.write(entry.getKey() + "; " + entry.getValue() + "\n");
                out.flush();
            }

            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void mapContext(Long page, MapReduceInterface mapper, ChordMessageInterface context) throws RemoteException, InterruptedException {
    	Thread thread = new Thread(() -> {
            try {
                context.setWorkingPeer(page);
                InputStream is = context.get(page);
                ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
                int subset = is.read();

                while (subset != 0){
                    byteBuffer.write(subset);
                    subset = is.read();
                }

                byte[] readByte = byteBuffer.toByteArray();
                String toMap = new String(readByte);

                String lines[] = toMap.split("\\r?\\n");
                for (int i = 0; i < lines.length; i++){
                    String pair = lines[i];
                    if (pair.contains(";")){
                        String split[] = pair.split(";");
                        long key = Long.valueOf(split[0]);
                        String val = split[1];

                        mapper.map(key, val, this);
                    }
                }
                context.completePeer(page, lines.length);

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    	thread.start();
    	thread.join();
    }

    public void emitMap(Long key, String value) throws RemoteException {
        if (predecessor == null){
            System.out.println("predecessor is null");
        }else {
            if (isKeyInOpenInterval(key, predecessor.getId(), successor.getId())) {
                if (!BMap.containsKey(key)){
                    List<String> list = new ArrayList<>();
                    BMap.put(key,list);
                }
                BMap.get(key).add(value);

            } else {
                ChordMessageInterface peer = locateSuccessor(key);
                peer.emitMap(key, value);
            }
        }
    }

    public void emitReduce(Long key, String value) throws RemoteException {
        if (isKeyInOpenInterval(key, predecessor.getId(), successor.getId())) {
            reduceTree.put(key, value);
        } else {
            ChordMessageInterface peer = locateSuccessor(key);
            peer.emitReduce(key, value);
        }
    }
}
