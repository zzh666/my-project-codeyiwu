package hadoop.svm;

import libsvm.svm_filt;

import libsvm.svm_train;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-4-11
 * Time: 上午12:08
 * To change this template use File | Settings | File Templates.
 */
public class MRSVMTrainnFilt {
    public static void main(String[] args) throws  Exception {
        // number of split, input path, output path
        for(int i=0;i<args.length;i++)
            System.out.println(args[i]);

        Configuration conf = new Configuration();
        JobConf job = new JobConf(conf, MRSVMTrainnFilt.class);
        job.setJobName("MapredCascadeSVMTrainJob");
        
        job.setInputFormat(WholeFileInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        job.setMapperClass(FiltMap.class);
        job.setReducerClass(TrainReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        conf.set("intermediate", args[0]);
        conf.set("inputpath", args[1]);
        conf.set("outputpath", args[2]);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        Path[] path = new Path[1];

        FileInputFormat.addInputPath(job, new Path(args[1]));

        path[0] = new Path(args[1]);
        FileInputFormat.setInputPaths(job, path);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        JobClient.runJob(job);
    }
    public static class FiltMap extends MapReduceBase 
            implements Mapper<Object, Text, IntWritable, Text> {

        public String inputpath;
        public String outputpath;
        
        public void configure(JobConf job) {
            inputpath = job.get("inputpath");
            outputpath = job.get("outputpath");
        }
        
        public void map(Object key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
            throws IOException {

            // write value inputstream to localfile at /tmp/ directory
            Date date = new Date();
            long milsec = date.getTime();
            String tmpfile = new String("/tmp/t_"+milsec);
            BufferedWriter bw = null;
            try {
                bw = new BufferedWriter(new FileWriter(tmpfile));
                bw.write(value.toString().toCharArray());
            } catch (IOException e) {
                throw new IOException("write local file error.");
            } finally {
                bw.close();
            }

            // train tmp localfile and output model
            String[] as = new String[2];
            as[0] = tmpfile;
            as[1] = new String(as[0]+"_model");
            String spl = new String(as[1]+"_spl");
            try {
                svm_filt.main(as);
            } catch (IOException e) {
                throw new IOException("filting error occured.");
            }

            BufferedReader br = null;
            String line = null;
            int k=0;
            try {
                br = new BufferedReader(new FileReader(spl));
                while((line=br.readLine()) != null) {
                    output.collect(new IntWritable(k++%2), new Text(line));
                }
            } catch (IOException e) {
                throw new IOException("write local file error.");
            } finally {
                br.close();
            }
        }
    }

    public static class TrainReduce extends MapReduceBase
            implements Reducer<IntWritable, Text, Text, Text> {
        public String inputpath;
        public String outputpath;

        public void configure(JobConf job) {
            inputpath = job.get("inputpath");
            outputpath = job.get("outputpath");
        }
        @Override
        public void reduce(IntWritable key, Iterator<Text> values,
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            // write value to localfile at /tmp/ directory
            Date date = new Date();
            long milsec = date.getTime();
            String tmpfile = new String("/tmp/t_"+milsec);
            BufferedWriter bw = null;
            try {
                bw = new BufferedWriter(new FileWriter(tmpfile));
                while(values.hasNext()) {
                    bw.write(values.next().toString()+"\n");
                }
            } catch (IOException e) {
                throw new IOException("write local file error.");
            } finally {
                bw.close();
            }

            String[] as = new String[2];
            as[0] = tmpfile;
            as[1] =tmpfile+".model";
            try {
                svm_train.main(as);
            } catch (IOException e) {
                throw new IOException("train last svm error.");
            }

            // upload model file to hdfs in mapping step
            Configuration conf = new Configuration();
            InputStream in = null;
            OutputStream out = null;
            FileSystem fs;
            String src = as[1];
            String dst;
            if(outputpath!=null)
                dst = outputpath+as[1];
            else
                dst = as[1];
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
            output.collect(new Text(dst), new Text(""));
        }
    }
}
