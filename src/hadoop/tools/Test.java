package hadoop.tools;

import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-21
 * Time: 下午2:38
 * To change this template use File | Settings | File Templates.
 */
public class Test {

    public static void main(String args[]) {
//        Splitter splitter = SplitnMergeFactory.createSplitter(3, "D:/test/expr3.log_bu", SplitnMergeFactory.SplitType.RANDOM);
//        try{
//            splitter.split();
//        } catch (IOException e) {
//            System.out.println("some exception happened.");
//        }
//
        
        String dir = "D:/test/expr3.log_bu_splits/";   // directory of your choice
        File file = new File(dir);
        File[] files = file.listFiles(new ExtenFileFilter("tr"));

//        for(int i=0;i<files.length;i++)
//            System.out.println(files[i].getAbsolutePath());
//
//        Merger merger = SplitnMergeFactory.createMerger("D:/test/expr3.log_bu_splits/","tr");
//        merger.merge();
//
//        Merger svmmerger = SplitnMergeFactory.createMerger("D:/test/", "model", SplitnMergeFactory.MergeType.SVM);
//        svmmerger.merge();
        
//        String test = "1_2_3_4";
//        int i = test.indexOf('_');
//        String substr = test.substring(0,i);
//        System.out.println(substr);

        Date date = new Date();
        System.out.println(date.getTime());

        int i=100000;
        while(i-- > 0) ;

        System.out.println(date.getTime());
        System.out.println(new Date().getTime());
        
        System.out.println("hello world.");
    }
}
