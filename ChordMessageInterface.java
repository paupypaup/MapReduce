import java.rmi.*;
import java.io.*;
import java.util.*;

public interface ChordMessageInterface extends Remote
{
    public ChordMessageInterface getPredecessor()  throws RemoteException;
	public ChordMessageInterface getSuccessor() throws RemoteException;
    ChordMessageInterface locateSuccessor(long key) throws RemoteException;
    ChordMessageInterface closestPrecedingNode(long key) throws RemoteException;
    public void joinRing(String Ip, int port)  throws RemoteException;
    public void notify(ChordMessageInterface j) throws RemoteException;
    public boolean isAlive() throws RemoteException;
    public long getId() throws RemoteException;
    
    public void emitMap(Long key, String value) throws RemoteException;
    public void emitReduce(Long key, String value) throws RemoteException;
    
    public void put(long guidObject, InputStream inputStream) throws IOException, RemoteException;
    public InputStream get(long guidObject) throws IOException, RemoteException;
    public void delete(long guidObject) throws IOException, RemoteException;

    public void setWorkingPeer(Long page) throws RemoteException;
    public void completePeer(Long page, int n) throws RemoteException;
    public Boolean isPhaseCompleted() throws RemoteException;
    
    public void reduceContext(Long source, MapReduceInterface reducer, ChordMessageInterface context) throws RemoteException;
    public void mapContext(Long page, MapReduceInterface mapper, ChordMessageInterface context) throws RemoteException;

    // public void map(Long key, String value, ChordMessageInterface context) throws IOException;
    // public void reduce(Long key, List< String > value, ChordMessageInterface context) throws IOException;
    
    public TreeMap<Long, String> getPredecessorReduce() throws RemoteException;
    public TreeMap<Long, String> getSuccessorReduce() throws RemoteException;
}