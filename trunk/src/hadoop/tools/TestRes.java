package hadoop.tools;

/**
 * Created by IntelliJ IDEA.
 * User: yiwu
 * Date: 12-3-27
 * Time: 下午3:24
 * To change this template use File | Settings | File Templates.
 */
public class TestRes {
    private int pre;
    private int tot;
    
    public TestRes() {
        pre = 0;
        tot = 0;
    }
    public TestRes(int p, int t) {
        this.pre = p;
        this.tot = t;
    }
    public int getPre() {
        return pre;
    }
    public void setPre(int p) {
        this.pre = p;
    }
    public int getTot() {
        return tot;
    }
    public void setTot(int t) {
        this.tot = t;
    }
}
