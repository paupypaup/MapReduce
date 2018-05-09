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
     * @param chord That is currently doing the distribution of the word
     */
    public void map(Long key, String value, ChordMessageInterface context) throws IOException {
    	String[] words = value.split(" ");
    	for (int i = 0; i < words.length; i++) {
    		long guidGet = md5(words[i]);
    		context.emitMap(key, value);
    	}
    }

    /**
     * Take the list of strings of the map and reduce it to a reduced tree map.
     * @param key They key of the guid of the page
     * @param value The string value to be mapped
     * @param chord The chord that is currently doing the distribution of the word
     */
    public void reduce(Long key, List< String > value, ChordMessageInterface context) throws IOException {
        context.emitReduce(key, value.get(0) +":"+value.size());
    }
    
    /**
	 * Convert string into guid
	 * @param objectName The string to be converted into guid
	 * @return guid The guid of the string
	 */
    private long md5(String objectName) {
		try {
			MessageDigest m = MessageDigest.getInstance("MD5");
			m.reset();
			m.update(objectName.getBytes());
			BigInteger bigInt = new BigInteger(1,m.digest());
			return Math.abs(bigInt.longValue());
		}
		catch(NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return 0;
	}
}