package hadoop.svm;

import hadoop.tools.TestRes;
import libsvm.svm_predict;
import libsvm.svm_train;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-22
 * Time: 下午10:57
 * To change this template use File | Settings | File Templates.
 */
public class MRSVMTest {

    public static class Map extends MapReduceBase implements Mapper<Object, Text, IntWritable, IntWritable> {
        public void map(Object key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
                throws IOException {

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
            output.collect(new IntWritable(1), new IntWritable(res.getPre()));
            output.collect(new IntWritable(2), new IntWritable(res.getTot()));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, Text, Text> {
        @Override
        public void reduce(IntWritable key, Iterator<IntWritable> values,
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            int i = 0;
            while(values.hasNext()) {
                //output.collect(values.next(), new Text(""));
                i += values.next().get();
                output.collect(new Text(key.toString()), new Text(new IntWritable(i).toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 3) {
            System.out.println("argment must contain inputpath, outputpath & modelpath.");
            return;
        }

        JobConf conf = new JobConf(hadoop.svm.MRSVMTest.class);
        conf.setJobName("MapReduceSVMTestJob");

        conf.setInputFormat(WholeFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.set("input", args[0]);
        conf.set("output", args[1]);
        conf.set("model", args[2]);

        JobClient.runJob(conf);
    }
}
