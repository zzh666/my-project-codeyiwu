package hadoop.tools;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-21
 * Time: 下午1:52
 * To change this template use File | Settings | File Templates.
 */
public class SplitnMergeFactory {
    public enum SplitType {
        SEQUENCE, RANDOM
    }
    public enum MergeType {
        CAT, SVM
    }

    public static Splitter createSplitter(int splitNum, String fileName) {
        return new SequenceSplitter(splitNum, fileName);
    }

    public static Splitter createSplitter(int splitNum, String fileName, SplitType splitType) {
        if(splitType == SplitType.SEQUENCE)
            return new SequenceSplitter(splitNum, fileName);
        if(splitType == SplitType.RANDOM)
            return new RandomSplitter(splitNum, fileName);

        return null;
    }

    public static  Merger createMerger(String path, String filter) {
        return new CatMerger(path, filter);
    }
    public static  Merger createMerger(String path, String filter, MergeType mergeType) {
        if(mergeType == MergeType.CAT)
            return new CatMerger(path, filter);
        if(mergeType == MergeType.SVM)
            return new SvmMerger(path, filter);

        return null;
    }
}
