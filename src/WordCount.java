import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Sunwei on 2017/11/6.
 */
public class WordCount {
    public static void main(String[] args){
        //创建配置对象
        Configuration conf=new Configuration();
        try{
            //创建job对象
            Job job = Job.getInstance(conf, "word count");
//Configuration conf, String jobName
            //设置运行job的类
            job.setJarByClass(WordCount.class);
            //设置mapper 类
            job.setMapperClass(WordcountMap.class);
            //设置reduce 类
            job.setReducerClass(WordcountReduce.class);

            //设置map输出的key value
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //设置reduce 输出的 key value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //设置输入输出的路径
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            //提交job
            boolean b = job.waitForCompletion(true);
            if(!b){
                System.out.println("wordcount task fail!");
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
