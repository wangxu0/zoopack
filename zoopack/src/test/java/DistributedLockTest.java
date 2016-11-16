import org.junit.Test;
import org.zoopack.lock.DistributedLock;

/**
 * @author wangxu
 * @date 2016/11/15
 */
public class DistributedLockTest {

    @Test
    public void testLock() {
        DistributedLock lock = new DistributedLock("/locks/testlock");
        lock.lock();
        //Access resources
        System.out.println("访问资源成功。");
        lock.unlock();
    }

}
