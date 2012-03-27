package hadoop.svm;

import hadoop.tools.TestRes;
import libsvm.svm_predict;
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
 * Time: 下午9:09
 * To change this template use File | Settings | File Templates.
 */
public class MapredSVMTest {

    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

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

            svm_predict st = new svm_predict();
            String[] as = new String[3];
            as[0] = tmpfile;
            as[1] = as[0]+"model";
            Configuration conf = new Configuration();
            InputStream in = null;
            OutputStream out = null;
            FileSystem fs;
            String model = conf.get("model");
            try {
                fs = FileSystem.get(URI.create(model), conf);
                in = fs.open(new Path(model));
                out = new BufferedOutputStream(new FileOutputStream(as[1]));
                IOUtils.copyBytes(in, out, 4096, false);
            } catch (IOException e) {
                throw new IOException("download model file error.");
            } finally {
                IOUtils.closeStream(in);
                if(out != null) out.close();
            }

            as[2] = new String(as[0]+".output");
            TestRes res = null;
            try {
                // make the predict method return the correct and total ------------
                res = svm_predict.main(as);
            } catch (IOException e) {
                throw new IOException("Testing error occured.");
            }

            // upload model file to hdfs in mapping step
            String src = as[2];
            String dst = conf.get("output")+as[2];
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
            context.write(new IntWritable(1), new IntWritable(res.getPre()));
            context.write(new IntWritable(2), new IntWritable(res.getTot()));
        }
    }
    public static class Reduce extends Reducer<IntWritable, IntWritable, Text, Text> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int i=0;
            for( IntWritable val : values) {
                i += val.get();
            }
            context.write(new Text(), new Text(new IntWritable(i).toString())); ;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //if(otherArgs.length != 2) {
        if(args.length != 2) {
            System.err.println("Uncompletely args.");
            System.exit(2);
        }

        Job job = new Job(conf, "MapredSVMTrain");
        job.setJarByClass(MapredSVMTest.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        conf.set("input",  args[0]);
        conf.set("output", args[1]);
        conf.set("model",  args[2]);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
