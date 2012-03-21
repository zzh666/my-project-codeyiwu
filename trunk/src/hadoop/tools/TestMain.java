package hadoop.tools;

import java.io.File;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-21
 * Time: 下午2:38
 * To change this template use File | Settings | File Templates.
 */
public class TestMain {

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

        for(int i=0;i<files.length;i++)
            System.out.println(files[i].getAbsolutePath());

        Merger merger = SplitnMergeFactory.createMerger("D:/test/expr3.log_bu_splits/",
                "tr");
        merger.merge();
        System.out.println("hello world.");
    }
}
