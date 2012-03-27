package hadoop.svm;

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
 * Date: 12-3-22
 * Time: 下午4:14
 * To change this template use File | Settings | File Templates.
 */
public class MRSVMTrain {
    public static String input = "";
    public static String output = "";
    
    public static class Map extends MapReduceBase implements Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
                throws IOException{

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
//            String line = null;
//            try {
//                BufferedReader br = new BufferedReader(new FileReader(as[1]));
//                while( (line=br.readLine())!= null ) {
//                    output.collect(new IntWritable(1), new Text(line));
//                }
//            }catch (FileNotFoundException e) {
//                throw new IOException("File not found.");
//            }catch (IOException e) {
//                throw new IOException("Read model file error.");
//            }

            // upload model file to hdfs in mapping step
            Configuration conf = new Configuration();
            InputStream in = null;
            OutputStream out = null;
            FileSystem fs;
            String src = as[1];
            String dst = output+as[1];
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

            output.collect(new IntWritable(1), new Text(dst));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text> {
        @Override
        public void reduce(IntWritable key, Iterator<Text> values,
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            while(values.hasNext()) {
                output.collect(values.next(), new Text(""));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            System.out.println("argment must contain inputpath, outputpath.");
            return;
        }

        JobConf conf = new JobConf(MRSVMTrain.class);
        conf.setJobName("MapReduceSVMTrainJob");

        conf.setInputFormat(NonSplittableTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        input = args[0];
        output = args[1];

        JobClient.runJob(conf);
    }
}
