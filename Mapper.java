import java.io.IOException;
import java.util.List;

public class Mapper implements MapReduceInterface{

    @Override
    public void map(Long key, String value, ChordMessageInterface context) throws IOException {
        context.emitMap(key, value);
    }

    @Override
    public void reduce(Long key, List< String > value, ChordMessageInterface context) throws IOException {
        context.emitReduce(key, value);
    }

}