package hadoop.svm;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-22
 * Time: 下午4:06
 * To change this template use File | Settings | File Templates.
 */
@Deprecated
public class NonSplittableTextInputFormat extends TextInputFormat {
    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }

}
