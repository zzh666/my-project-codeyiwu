package hadoop.tools;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-21
 * Time: 下午4:19
 * To change this template use File | Settings | File Templates.
 */
public abstract class Merger {
    
    protected String filter;
    protected String path;
    protected String mergefile;
    
    abstract public boolean merge();
    
    public File[] getMergeFiles() {
        File file = new File(path);
        mergefile = file.getAbsolutePath()+"/merge."+filter+"_";
        File[] files = file.listFiles(new ExtenFileFilter(filter));
        System.out.println(mergefile);
        return  files;
    }

    public BufferedWriter getBufferedWriter() {
        FileOutputStream fw;
        BufferedWriter bw;
        try {
            fw = new FileOutputStream(new File(mergefile));
            OutputStreamWriter osr = new OutputStreamWriter(fw,"GBK");
            bw = new BufferedWriter(osr);
        } catch (FileNotFoundException e) {
            System.out.println("could not write the file: "+mergefile);
            e.printStackTrace();
            return null;
        } catch (UnsupportedEncodingException e) {
            System.out.println("unsupported encoding for GBK");
            e.printStackTrace();
            return null;
        }
        return bw;
    }

    public List<BufferedReader> getBufferedReaders() throws IOException{
        List<BufferedReader> brs = new ArrayList<BufferedReader>();
        List<FileInputStream> frs = new ArrayList<FileInputStream>();

        File[] inputFiles = getMergeFiles();
        String tmp="";
        try {
            FileInputStream fi;
            BufferedReader br;
            for(int i=0;i<inputFiles.length;i++) {
                tmp = inputFiles[i].getAbsolutePath();
                fi = new FileInputStream(tmp);
                InputStreamReader isr = new InputStreamReader(fi,"GBK");
                br = new BufferedReader(isr);
                System.out.println("open buffered reader: "+tmp);
                frs.add(fi);
                brs.add(br);
            }
            
        } catch (FileNotFoundException e) {
            System.out.println("could not open the split file: "+tmp);
            for(int i=0;i<frs.size();i++)
                frs.get(i).close();
            frs.clear();
            brs.clear();
            return brs;
        }
        return brs;
    }
    
    public Merger(String p, String f) {
        this.path = p;
        this.filter = f;
    }
    public String getPath() {
        return path;
    }
    public void setPath(String p) {
        this.path = p;
    }
    public String getFilter() {
        return filter;
    }
    public void setFilter(String f) {
        this.filter = f;
    }
}
