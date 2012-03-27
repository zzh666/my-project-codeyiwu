package hadoop.svm;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-27
 * Time: 下午8:39
 * To change this template use File | Settings | File Templates.
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, Text> {
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }
    @Override
    public RecordReader<NullWritable, Text> getRecordReader(
            InputSplit split, JobConf job, Reporter reporter)
            throws IOException {
        return new WholeFileRecordReader((FileSplit)split, job);
    }
}
