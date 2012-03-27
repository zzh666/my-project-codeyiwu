package hadoop.tools;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-22
 * Time: 下午9:21
 * To change this template use File | Settings | File Templates.
 */
public class Main {

    // file format for example: D:/test/train
    public static void trainSplit(String file, int split) {
        Splitter splitter = SplitnMergeFactory.createSplitter(split, file, SplitnMergeFactory.SplitType.SEQUENCE);
        try{
            splitter.split();
        } catch (IOException e) {
            System.out.println("splitting training file error.");
            e.printStackTrace();
        }
    }

    // path format for example: D:/test/train | suffix = "model"
    public static void trainMerge(String path, String suffix) {
        Merger svmmerger = SplitnMergeFactory.createMerger(path, suffix, SplitnMergeFactory.MergeType.SVM);
        svmmerger.merge();
    }
    
    public static void testSplit(String file, int split) {
        Splitter splitter = SplitnMergeFactory.createSplitter(split, file, SplitnMergeFactory.SplitType.SEQUENCE);
        try{
            splitter.split();
        } catch (IOException e) {
            System.out.println("splitting testing file error.");
            e.printStackTrace();
        }
    }
    
    public static void testMerge(String path, String suffix) {
        Merger merger = SplitnMergeFactory.createMerger(path, suffix);
        merger.merge();
    }
    
    public static void main(String[] args) {
        for(int i=0;i<args.length;i++) 
            System.out.print(args[i]+"");
        System.out.println();
        
        if(args.length != 3) {
            System.out.println("argments number error. please input 4 argments");
            System.out.println("+\'a\' String int for trainSplit()");
            System.out.println("+\'b\' String String for trainMerge()");
            System.out.println("+\'c\' String int for testSplit()");
            System.out.println("+\'d\' String String for testMerge()");
            return;
        }

        char x = args[0].charAt(0);
        switch (x) {
            case 'a':
                System.out.println("-->do trainSplit method with: "+args.toString());
                trainSplit(args[1], Integer.valueOf(args[2]));
                break;
            case 'b':
                System.out.println("-->do trainMerge method with: "+args.toString());
                trainMerge(args[1], args[2]);
                break;
            case 'c':
                System.out.println("-->do testSplit method with: "+args.toString());
                testSplit(args[1], Integer.valueOf(args[2]));
                break;
            case 'd':
                System.out.println("-->do testMerge method with: "+args.toString());
                testMerge(args[1], args[2]);
                break;
            default:
                System.out.println("error args[1], please instead it of \'a\' \'b\' \'c\' or \'d\'");
                System.out.println("+\'a\' String int for trainSplit()");
                System.out.println("+\'b\' String String for trainMerge()");
                System.out.println("+\'c\' String int for testSplit()");
                System.out.println("+\'d\' String String for testMerge()");
                break;
        }
    }
}
