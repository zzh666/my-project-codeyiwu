package hadoop.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.lang.Integer;
import java.lang.Double;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-21
 * Time: 下午4:53
 * To change this template use File | Settings | File Templates.
 */
public class SvmMerger extends Merger{

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
        String st="", kt="", l1="", l2="", tmp="";
        double gm, rho;
        int nrc, tsv;
        Map<Integer, Integer> ns = new HashMap<Integer, Integer>();
        // initialization
        gm = 0; rho = 0; nrc=0; tsv = 0;
        BufferedReader br;
        String line = "";
        try {
            for(int i=0;i<brs.size();i++) {
                br = brs.get(i);
                line = br.readLine();       //svm_type
                st = getWord(line, 1);
                line = br.readLine();       //kernel_type
                kt = getWord(line, 1);
                line = br.readLine();       //gamma
                tmp = getWord(line, 1);
                gm += Double.valueOf(tmp);
                line = br.readLine();       //nr_class
                tmp = getWord(line, 1);
                nrc = Integer.valueOf(tmp);
                line = br.readLine();       //total_sv
                tmp = getWord(line, 1);
                tsv += Integer.valueOf(tmp);
                line = br.readLine();       //rho
                tmp = getWord(line, 1);
                rho += Double.valueOf(tmp);
                line = br.readLine();       //label
                l1 = getWord(line, 1);
                l2 = getWord(line, 2);
                line = br.readLine();       //nr_sv
                tmp = getWord(line, 1);
                int ll1 = Integer.valueOf(l1);
                int ll2 = Integer.valueOf(l2);
                if(!ns.containsKey(l1))
                    ns.put(ll1, 0);
                ns.put(ll1, ns.get(ll1) + Integer.valueOf(tmp));
                tmp = getWord(line, 2);
                if(!ns.containsKey(ll2))
                    ns.put(ll2, 0);
                ns.put(ll2, ns.get(ll2) + Integer.valueOf(tmp));
                br.readLine();              //SV
            }

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return false;
        } catch (NullPointerException e) {
            System.out.println("null pointer exception. "+line);
            e.printStackTrace();
            return false;
        }
//        System.out.println("svm_type "+st);
//        System.out.println("kernel_type "+kt);
//        System.out.println("gamma "+String.valueOf(gm/brs.size()));
//        System.out.println("nr_class "+String.valueOf(nrc));
//        System.out.println("total_sv "+String.valueOf(tsv));
//        System.out.println("rho "+String.valueOf(rho/brs.size()));
//        String print = "";
//        Iterator<Map.Entry<String, Integer>> it = ns.entrySet().iterator();
//        while(it.hasNext()) {
//            Map.Entry entry = it.next();
//            print += entry.getKey();
//            print += " ";
//        }
//        System.out.println("label "+print);
//        print = "";
//        it = ns.entrySet().iterator();
//        while(it.hasNext()) {
//            Map.Entry entry = it.next();
//            print += entry.getValue();
//            print += " ";
//        }
//        System.out.println("nr_sv "+print);
//        System.out.println("SV");
        try {
            bw.write("svm_type "+st);
            bw.newLine();
            bw.write("kernel_type "+kt);
            bw.newLine();
            bw.write("gamma "+String.valueOf(gm/brs.size()));
            bw.newLine();
            bw.write("nr_class "+String.valueOf(nrc));
            bw.newLine();
            bw.write("total_sv "+String.valueOf(tsv));
            bw.newLine();
            bw.write("rho "+String.valueOf(rho/brs.size()));
            bw.newLine();
            String print = "";
            Iterator<Map.Entry<Integer, Integer>> it = ns.entrySet().iterator();
            int[] kar = new int[ns.size()];
            int idx=0;
            while(it.hasNext()) {
                Map.Entry entry = it.next();
                //print += entry.getKey();
                //print += " ";
                kar[idx++] = Integer.valueOf(entry.getKey().toString());
            }
            if(idx == 2) {
                if(kar[0] > kar[1]) {
                    int t = kar[1];
                    kar[1] = kar[0];
                    kar[0] = t;
                }
            }
            for(int k=0;k<idx;k++)
                print += String.valueOf(kar[k]) + " ";
            bw.write("label "+print);
            bw.newLine();
            print = "";
            //it = ns.entrySet().iterator();
            for(int k=0;k<idx;k++) {
                print += ns.get((kar[k])).toString() + " ";
            }
            bw.write("nr_sv "+print);
            bw.newLine();
            bw.write("SV");
            bw.newLine();
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(int i=0;i<brs.size();i++) {
            br = brs.get(i);
            try {
                while( (line=br.readLine()) != null) {
                    bw.write(line);
                    bw.newLine();
                }
                bw.flush();
            } catch (IOException e) {
                System.out.println("error occured in file: "+i);
                e.printStackTrace();
                return false;
            }
        }
        try {
            bw.close();
            for(int i=0;i<brs.size();i++)
                brs.get(i).close();
        } catch (IOException e) {
            System.out.println("file close error.");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public SvmMerger(String p, String f) {
        super(p, f);
    }
    
    public String getWord(String line, int idx) throws IOException{
        String tmp;
        //System.out.println("get index: "+idx+" from "+line);
        List<String> s = new ArrayList<String>();
        int i;
        while(true) {
            i = line.indexOf(' ');
            if(i == -1) {
                //System.out.println("    "+line+"+");
                s.add(line);
                break;
            }
            tmp = line.substring(0,i);
            line = line.substring(i+1);
            //System.out.println("    "+tmp+"+"+line);
            s.add(tmp);
        }
        if(s.size() <= i)
            throw new IOException("get word error");
        //System.out.println("-->"+s.toString()+" ++ "+idx+" -- "+s.get(idx));
        return s.get(idx);
    }
}
