package hadoop.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-21
 * Time: 下午1:56
 * To change this template use File | Settings | File Templates.
 */
public class RandomSplitter extends Splitter{

    public boolean split() throws IOException {

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
                double r = Math.random();
                int tmp = (int) (r * splitNum);
                bws.get(tmp).write(line);
                bws.get(tmp).newLine();
                //System.out.println("Random var: "+String.valueOf(r)+" Line: " + String.valueOf(cnt)
                        //+ " -> splitfile: " + String.valueOf(tmp)+" context: "+line);
                System.out.println("Line: "+String.valueOf(cnt));
                // flush to disk for each line
                bws.get(tmp).flush();
                cnt++;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public RandomSplitter(int splitNum, String fileName) {
        super(splitNum, fileName);
    }
}
