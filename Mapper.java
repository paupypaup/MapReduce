import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.rmi.Remote;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class Mapper implements MapReduceInterface, Serializable{

	/**
     * Distribute the words throughout file system, putting the word according to its guid in between processes.
     * @param key The guid of the page
     * @param value The string value to be mapped
     */
    public void map(Long key, String value, ChordMessageInterface context) throws IOException {
        context.emitMap(key, value);
    }

    /**
     * Take the list of strings of the map and reduce it to a reduced tree map.
     * @param key They key of the guid of the page
     * @param value The string value to be mapped
     */
    public void reduce(Long key, List< String > value, ChordMessageInterface context) throws IOException {
        context.emitReduce(key, value.get(0) +":"+value.size());
    }

}