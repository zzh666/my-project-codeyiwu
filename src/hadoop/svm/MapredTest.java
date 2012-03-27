package hadoop.svm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-27
 * Time: 下午8:51
 * To change this template use File | Settings | File Templates.
 */
@SuppressWarnings("deprecation")
public class MapredTest {

    public static class Map extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            FileSplit fs = (FileSplit)(context.getInputSplit());
            String path = fs.getPath().toString();
            path = path + value.toString();
            context.write(new IntWritable(1), new Text(path));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public static IntWritable linenum = new IntWritable(1);

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for( Text val : values) {
                context.write(linenum, val) ;
                linenum = new IntWritable(linenum.get() +1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(args.length != 2) {
            System.err.println("Uncompletely args.");
            System.exit(2);
        }

        Job job = new Job(conf, "TestInputPath");
        job.setJarByClass(MapredTest.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}