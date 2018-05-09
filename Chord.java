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
    private TreeMap<Long, String> PreBReduce;
    private TreeMap<Long, String> SucBReduce;
    private TreeMap< Long, List<String>> BMap;

    /** Ensure key belongs within the interval and exactly on the chord
     * @param key guid to be compared between the two keys
     * @param key1 beginning of the guid
     * @param key2 end of the guid
     * @return True if the guid belongs between the two keys
     */
    public Boolean isKeyInSemiCloseInterval(long key, long key1, long key2)
    {
       if (key1 < key2)
           return (key > key1 && key <= key2);
      else
          return (key > key1 || key <= key2);
    }

    /** Ensure key belongs within the interval but not exactly on the chord
     * @param key guid to be compared between the two keys
     * @param key1 beginning of the guid
     * @param key2 end of the guid
     * @return True if the guid belongs between the two keys
     */
    public Boolean isKeyInOpenInterval(long key, long key1, long key2)
    {
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
        // todo
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

    public void findingNextSuccessor()
    {
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
          if (successor != null)
          {
              ChordMessageInterface x = successor.getPredecessor();

              if (x != null && x.getId() != this.getId() && isKeyInOpenInterval(x.getId(), this.getId(), successor.getId()))
              {
                  successor = x;
              }
              if (successor.getId() != getId())
              {
                  successor.notify(this);
              }
          }
      } catch(RemoteException | NullPointerException e1) {
          findingNextSuccessor();

      }
    }

    public void notify(ChordMessageInterface j) throws RemoteException {
         if (predecessor == null || (predecessor != null
                    && isKeyInOpenInterval(j.getId(), predecessor.getId(), guid)))
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

    public Chord(int port, long guid) throws RemoteException {
        int j;
	    finger = new ChordMessageInterface[M];
        for (j=0;j<M; j++){
	       finger[j] = null;
     	}
        this.guid = guid;

        predecessor = null;
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

	public Map<Long, String> getReduce() throws RemoteException {
		return BReduce;
	}
    
    /** Increment the set by 1 when a peer is tasked to work.
     * 
     */
    public void setWorkingPeer(Long page)
    {
        set.add(page);
    }
    
    /** Reduce the set by 1 when a peer has completed its page
     * 
     */
    public void completePeer(Long page, int n) throws RemoteException{
    	this.numOfRecords += n;
        set.remove(page);
    }
    
    /** Check to see if the peer has completed its process by check for the empty set.
     * @return if the set is empty
     */
    public Boolean isPhaseCompleted(){
        return set.isEmpty();
    }
    
    /** Goes through each node and have it runs the reduceContext, Reducing their BMap
     * @param source Whoever started the recursion so it can stop when it reaches back to itself
     * @param reducer The reducer class to perform either map or reduce
     * @param context The node performing the process. Provide flexability so anyone can make this done it's work properly
     */
    public void reduceContext(Long source, MapReduceInterface reducer, ChordMessageInterface context) throws RemoteException {
        if (source != guid) {
        	successor.reduceContext(source, reducer, context);
        }
        context.setWorkingPeer(source);;
        Thread mappingThread = new Thread() {
        	public void run() {
        		Set<Long> setOfKeys = BMap.keySet();
        		try {
        			for (Long key : setOfKeys) {
        				List<String> getList = BMap.get(key);
        				reducer.reduce(key, getList, context);
        			}
        		} catch (Exception e) {
        			e.printStackTrace();
        		}
        		try {
        			completePeer(source, 1);
        		} catch (RemoteException e) {
        			e.printStackTrace();
        		}
        	}
        };
        mappingThread.start();
    }

    /** Reads a page and send the content to the mapper so it can map the word to the correct key
     * @param page The page to be read
     * @param mapper The reducer class to perform either map or reduce
     * @param context The node performing the process. Provide flexability so anyone can make this done it's work properly
     */
    public void mapContext(Long page, MapReduceInterface mapper, ChordMessageInterface context) throws RemoteException {
    	Thread mappingThread = new Thread() {
    		public void run() {
    			try {
    	            setWorkingPeer(page);
    	            InputStream is = context.get(page);
    	            ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
    	            int nRead = is.read();
    	            while (nRead != 0){
    	                byteBuffer.write(nRead);
    	                nRead = is.read();
    	            }
    	            byteBuffer.flush();
    	            is.close();
    	            byte[] readByte = new byte[1024];
    	            readByte = byteBuffer.toByteArray();
    	            mapper.map(page, new String(readByte), context);   
    	            set.remove(page);
    	        } catch (IOException e) {
    	            e.printStackTrace();
    	        }
    		}
    	};
    	mappingThread.start();
    }
    
    /** Check to see if the key lands before or after the node, if not it will be passed to the next node
     * @param key The key to be compared between the two nodes
     * @param value The value of the key
     */
    public void emitMap(Long key, String value) throws RemoteException {
        if (isKeyInOpenInterval(key, predecessor.getId(), successor.getId())) {
        	List<String> list = BMap.get(key);
            if (list == null) list = new ArrayList<String>();
            list.add(value);
            BMap.put(key, list);
        } else {
        	ChordMessageInterface peer = locateSuccessor(key);
            peer.emitMap(key, value);
        }
    }

    /** Check to see if the key lands before the guid or after the guid
     * @param key The key to be compared between the two guids
     * @param value The value of the key
     */
    public void emitReduce(Long key, String value) throws RemoteException {
        if (isKeyInOpenInterval(key, predecessor.getId(), successor.getId())) {
            PreBReduce.put(key, value);
        } else if (isKeyInOpenInterval(key, guid, successor.getId())) {
            SucBReduce.put(key, value);
        }
    }

	public TreeMap<Long, String> getPredecessorReduce() throws RemoteException {
		return PreBReduce;
	}
	
	public TreeMap<Long, String> getSuccessorReduce() throws RemoteException {
		return SucBReduce;
	}
}
