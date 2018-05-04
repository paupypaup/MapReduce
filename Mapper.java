import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.rmi.Remote;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class Mapper implements MapReduceInterface, Serializable{

    @Override
    public void map(Long key, String value, ChordMessageInterface context) throws IOException {
    	String[] words = value.split(" ");
    	for (int i = 0; i < words.length; i++) {
    		long guidGet = md5(words[i]);
    		context.emitMap(key, value);
    	}
    }

    @Override
    public void reduce(Long key, List< String > value, ChordMessageInterface context) throws IOException {
        context.emitReduce(key, value.get(0) +":"+value.size());
    }
    
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