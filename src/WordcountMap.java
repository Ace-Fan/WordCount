import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Sunwei on 2017/11/6.
 */

public class WordcountMap extends Mapper<LongWritable,Text,Text,IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
// IntWritable one=new IntWritable(1);
        //得到输入的每一行数据
        String line=value.toString();

        StringTokenizer st=new StringTokenizer(line);
//StringTokenizer "kongge"
        while (st.hasMoreTokens()){
            String word= st.nextToken();
            context.write(new Text(word),new IntWritable(1));   //output
        }
    }
}
