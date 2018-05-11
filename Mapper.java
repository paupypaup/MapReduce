package mapreduce.MapReduce;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.rmi.Remote;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class Mapper implements MapReduceInterface, Serializable{

    /**
     * This method calls the emitMap method in the ChordMessageInterface 
     * @param key A long which represents a key to be used
     * @param value a string value which is associated with the passed in key
     * @param context The ChordMessageInterface which has abstract methods.
     * @throws IOException 
     */
    public void map(Long key, String value, ChordMessageInterface context) throws IOException {
        context.emitMap(key, value);
    }

    /**
     * This method calls the emitReduce method in the ChordMessageInterface 
     * @param key A long which represents a key to be used
     * @param value a string list which is associated with the passed in key
     * @param context The ChordMessageInterface which has abstract methods.
     * @throws IOException 
     */
    public void reduce(Long key, List< String > value, ChordMessageInterface context) throws IOException {
        context.emitReduce(key, value.get(0));
    }

}