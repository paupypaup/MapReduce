import java.io.IOException;
import java.util.List;

public interface MapReduceInterface {
    public void map(Long key, String value, ChordMessageInterface context) throws IOException;
    public void reduce(Long key, List< String > value, ChordMessageInterface context) throws IOException;
}
