package hadoop.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-21
 * Time: 下午4:39
 * To change this template use File | Settings | File Templates.
 */
public class CatMerger extends Merger{

    public boolean merge() {
        BufferedWriter bw;
        List<BufferedReader> brs;
        try {
            brs = getBufferedReaders();
            bw = getBufferedWriter();
        } catch (IOException e) {
            System.out.println("file opened error.");
            e.printStackTrace();
            return false;
        }

        if(brs.size() == 0) {
            System.out.println("There is no file to be merged up.");
            return false;
        }

        String line = "";
        for(int i=0;i<brs.size();i++) {
            try {
                //System.out.println("merge file "+i);
                BufferedReader br = brs.get(i);
                
                while ((line=br.readLine()) !=null) {
                    bw.write(line);
                    bw.newLine();
                    System.out.println("file: "+i+" -> merge file");
                }
                bw.flush();
            }catch (IOException e) {
                System.out.println("merge file "+i+" error.");
                e.printStackTrace();
                return false;
            }
        }
        try {
            bw.close();
            for(int i=0;i<brs.size();i++)
                brs.get(i).close();
        } catch ( IOException e) {
            e.printStackTrace();
        }
        brs.clear();
        return true;
    }

    public CatMerger(String p, String f) {
        super(p, f);
    }
}
