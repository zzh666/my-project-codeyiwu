package hadoop.tools;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-21
 * Time: 下午1:52
 * To change this template use File | Settings | File Templates.
 */
public abstract class Splitter {

    protected int splitNum;
    protected String fileName;

    public static final String dir_suffix = "_splits";

    abstract public boolean split() throws IOException;
 
    public String[] getSplitFileNames() {
        int sn = this.splitNum;
        String fn = this.fileName;

        File file = new File(fn);
        String dir = fn+dir_suffix;
        if(!(new File(dir).mkdir()))
            System.out.println("could not create subdir: "+dir);
        else
            System.out.println("create subdir: "+dir);
        String sfn = dir+"/"+file.getName()+"_part_";

        String[] sfns = new String[sn];
        for(int i=0;i<sn;i++) {
            sfns[i] = sfn+String.valueOf(i)+".tr";
            System.out.println(sfns[i]);
        }
        return sfns;
    }

    public BufferedReader getBufferedReader() {
        FileInputStream fr;
        BufferedReader br;
        try {
            fr = new FileInputStream(new File(fileName));
            InputStreamReader isr = new InputStreamReader(fr,"GBK");
            br = new BufferedReader(isr);
        } catch (FileNotFoundException e) {
            System.out.println("could not open the file: "+this.fileName);
            e.printStackTrace();
            return null;
        } catch (UnsupportedEncodingException e) {
            System.out.println("unsupported encoding for GBK");
            e.printStackTrace();
            return null;
        }
        return br;
    }

    public List<BufferedWriter> getBufferedWriterList() throws IOException {
        String[] sfns = getSplitFileNames();

        List<FileOutputStream> fws = new ArrayList<FileOutputStream>();
        List<BufferedWriter> bws = new ArrayList<BufferedWriter>();
        String sfn = new String("");
        try {
            FileOutputStream fw;
            BufferedWriter bw;
            for(int i=0;i<sfns.length;i++) {
                sfn = sfns[i];
                fw = new FileOutputStream(sfn);
                OutputStreamWriter osw = new OutputStreamWriter(fw, "GBK");
                bw = new BufferedWriter(osw);
                System.out.println("open bufferedwrite: "+sfn);
                fws.add(fw);
                bws.add(bw);
            }
        } catch (FileNotFoundException e) {
            System.out.println("could not open the split file: "+sfn);
            for(int i=0;i<fws.size();i++)
                fws.get(i).close();
            fws.clear();
            bws.clear();
            return bws;
        }
        return bws;
    }

    public Splitter(int sn, String fn) {
        splitNum = sn;
        fileName = fn;
    }
    
    public int getSplitNum() {
        return splitNum;
    }
    public void setSplitNum(int sn) {
        splitNum = sn;
    }
    public String getFileName() {
        return fileName;
    }
    public void setFileName(String fn) {
        fileName = fn;
    }
}
