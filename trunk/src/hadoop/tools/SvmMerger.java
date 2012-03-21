package hadoop.tools;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-21
 * Time: 下午4:53
 * To change this template use File | Settings | File Templates.
 */
public class SvmMerger extends Merger{

    public boolean merge() {

        return true;
    }

    public SvmMerger(String p, String f) {
        super(p, f);
    }
}
