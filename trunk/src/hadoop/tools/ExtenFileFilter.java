package hadoop.tools;

import java.io.File;
import java.io.FileFilter;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-21
 * Time: 下午5:10
 * To change this template use File | Settings | File Templates.
 */
public class ExtenFileFilter implements FileFilter {
    private String extension;

    public ExtenFileFilter(String extension) {
        this.extension = extension;
    }
    public boolean accept(File file) {
        if(file.isDirectory( )) {
            return false;
        }
        String name = file.getName( );
        // find the last
        int idx = name.lastIndexOf(".");
        if(idx == -1) {
            return false;
        } else
        if(idx == name.length( ) -1) {
            return false;
        } else {
            return this.extension.equals(name.substring(idx+1));
        }
    }
}
