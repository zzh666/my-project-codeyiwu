package hadoop.svm;

import libsvm.svm_train;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-27
 * Time: 下午9:00
 * To change this template use File | Settings | File Templates.
 */
public class MapredSVMTrain {
    public static class Map extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            // write inputstream to localfile at /tmp/ directory
            Date date = new Date();
            long milsec = date.getTime();
            String tmpfile = new String("/tmp/t_"+milsec);
            BufferedWriter bw = null;
            try {
                bw = new BufferedWriter(new FileWriter(tmpfile));
                bw.write(value.toString().toCharArray());
            } catch (IOException e) {
                throw new IOException("Write local file error.");
            } finally {
                bw.close();
            }

            // train tmp localfile and output model
            String[] as = new String[2];
            as[0] = tmpfile;
            as[1] = new String(as[0]+".model");

            try {
                svm_train.main(as);
            } catch (IOException e) {
                throw new IOException("Training error occured.");
            }

            Configuration conf = new Configuration();
            // get input & output from conf
            String inputp = conf.get("input");
            String outputp = conf.get("output");
            InputStream in = null;
            OutputStream out = null;
            FileSystem fs;
            String src = as[1];
            String dst = outputp+as[1];
            try {
                // maybe could not create dir ../tmp/..----------------
                fs = FileSystem.get(URI.create(dst), conf);
                in = new BufferedInputStream(new FileInputStream((as[1])));
                out = fs.create(new Path(dst), new Progressable() {
                    @Override
                    public void progress() {
                        System.out.print(".");
                    }
                });
                IOUtils.copyBytes(in, out, 4096, true);
            } catch (IOException e) {
                throw  new IOException("upload localfile to hdfs error.");
            } finally {
                IOUtils.closeStream(out);
                in.close();
            }
            context.write(new IntWritable(1), new Text(dst));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {
        
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for( Text val : values) {
                context.write(val, new Text("")) ;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2) {
            System.err.println("Uncompletely args.");
            System.exit(2);
        }

        Job job = new Job(conf, "MapredSVMTrain");
        job.setJarByClass(MapredSVMTrain.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        conf.set("input",  otherArgs[0]);
        conf.set("output", otherArgs[1]);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
