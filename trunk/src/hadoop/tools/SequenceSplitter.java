package hadoop.tools;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-21
 * Time: 下午1:53
 * To change this template use File | Settings | File Templates.
 */
public class SequenceSplitter extends Splitter{

    public boolean split() throws IOException{

        BufferedReader br = getBufferedReader();
        List<BufferedWriter> bws = getBufferedWriterList();

        if(bws.size() != splitNum) {
            System.out.println("split files created error.");
            return false;
        }

        try {
            String line;
            int cnt=0;
            while( (line=br.readLine()) != null ) {
                //line = new String(line.getBytes(), "UTF8");
                int tmp = cnt % splitNum;
                bws.get(tmp).write(line);
                bws.get(tmp).newLine();
                //System.out.println("Line: " + String.valueOf(cnt) + " -> splitfile: " + String.valueOf(tmp)
                //+" context: "+line);
                // flush to disk for each line
                System.out.println("Line: "+String.valueOf(cnt));
                //bws.get(tmp).flush();
                cnt++;
            }
            for(int i=0;i<splitNum;i++)
                bws.get(i).flush();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public SequenceSplitter(int splitNum, String fileName) {
        super(splitNum, fileName);
    }
}
