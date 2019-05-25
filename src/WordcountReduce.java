import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by Sunwei on 2017/11/6.
 */
public class WordcountReduce extends Reducer<Text,IntWritable,Text,IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> iterable, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable i:iterable){
            sum=sum+i.get();
        }
        context.write(key,new IntWritable(sum));
    }

}
