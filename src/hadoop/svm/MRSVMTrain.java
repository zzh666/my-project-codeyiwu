package hadoop.svm;

import libsvm.svm_train;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
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
    public static class Map extends MapReduceBase implements Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
                throws IOException{
            //String path = value.toString();
            //int lidx = path.lastIndexOf('/');
            //String suf = path.substring(lidx+1);
            Date date = new Date();
            String tmpfile = new String("/tmp/t.0000"+date.getTime());
            BufferedWriter bw = null;
            try {
                bw = new BufferedWriter(new FileWriter(tmpfile));
                bw.write(value.toString().toCharArray());
            } catch (IOException e) {
                throw new IOException("Write local file error.");
            } finally {
                bw.close();
            }

            svm_train st = new svm_train();
            String[] as = new String[2];
            as[0] = tmpfile;
            as[1] = new String(as[0]+".model");

            try {
                st.run(as);
            } catch (IOException e) {
                throw new IOException("Training error occured.");
            }
            String line = null;
            try {
                BufferedReader br = new BufferedReader(new FileReader(as[1]));
                while( (line=br.readLine())!= null ) {
                    output.collect(new IntWritable(1), new Text(line));
                }
            }catch (FileNotFoundException e) {
                throw new IOException("File not found.");
            }catch (IOException e) {
                throw new IOException("Read model file error.");
            }
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

        JobClient.runJob(conf);
    }
}
