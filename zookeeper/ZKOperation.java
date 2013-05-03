import java.io.IOException;
import java.util.List;
import java.util.Stack;
import java.util.Date;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/* zookeeper version: 3.3.5
 * export ZOOBINDIR=$ZOOBINDIR
 * javac -cp `$ZOOBINDIR/zkEnv.sh|awk -F= '{print $2}'` ZKOperation.java 
**/

public class ZKOperation {

    //目前tickTime=2000, 会话超时时间设为15倍ticktime
    private static final int SESSION_TIMEOUT=30000;

    public ZooKeeper zk;

    private class MyWatcher implements Watcher {
        public void process(WatchedEvent event) {
            System.out.println("WATCHER::");
            System.out.println(event.toString());
        }
    }

    private void createInstance() throws IOException
    {  
        zk = new ZooKeeper("localhost:2181", this.SESSION_TIMEOUT, new MyWatcher());
    }

    private void close() throws InterruptedException
    {  
        zk.close();
    }

    private static void printStat(Stat stat) {
        System.err.println("cZxid = 0x" + Long.toHexString(stat.getCzxid()));
        System.err.println("ctime = " + new Date(stat.getCtime()).toString());
        System.err.println("mZxid = 0x" + Long.toHexString(stat.getMzxid()));
        System.err.println("mtime = " + new Date(stat.getMtime()).toString());
        System.err.println("pZxid = 0x" + Long.toHexString(stat.getPzxid()));
        System.err.println("cversion = " + stat.getCversion());
        System.err.println("dataVersion = " + stat.getVersion());
        System.err.println("aclVersion = " + stat.getAversion());
        System.err.println("ephemeralOwner = 0x"
                + Long.toHexString(stat.getEphemeralOwner()));
        System.err.println("dataLength = " + stat.getDataLength());
        System.err.println("numChildren = " + stat.getNumChildren());
    }

    interface NodeOperation {
        public void operation(String path) throws InterruptedException, KeeperException;
    }

    class ShowOperation implements NodeOperation
    {  
        public void operation(String path)
            throws KeeperException, InterruptedException
        {  
            System.out.println(path);
        }
    }

    class DeleteOperation implements NodeOperation
    {  
        public void operation(String path)
            throws KeeperException, InterruptedException
        {  
            zk.delete(path, -1);
            //see: http://zookeeper.apache.org/doc/r3.2.2/api/org/apache/zookeeper/ZooKeeper.html#delete(java.lang.String, int)
            System.out.println("deleted: " + path);
        }
    }

    class ShowDetailOperation implements NodeOperation
    {  
        public void operation(String path)
            throws KeeperException, InterruptedException
        {  
            Stat stat = new Stat();
            System.out.println(new String(zk.getData(path, false, stat)));
            printStat(stat);
        }
    }

    public void postorderVisit(String root, NodeOperation nodeop)
        throws IOException, InterruptedException, KeeperException
    {  
        Stack<String> st = new Stack<String>();
        Stack<String> stDone = new Stack<String>();
        st.push(root);
        while (!st.empty()) {
            String cur = st.peek();
            if (stDone.empty() || cur != stDone.peek()) {
                List<String> li = zk.getChildren(cur,false);
                if (li.isEmpty()) {
                    st.pop();
                    nodeop.operation(cur);
                } else {
                    stDone.push(cur);
                    for (String s: li) {
                        st.push(cur + "/" + s);
                    }
                }
            } else {
                nodeop.operation(cur);
                st.pop();
                stDone.pop();
            }
        }
    }

    public NodeOperation nodeFactory(String func) {
        if (func.equals("show")) {
            return this.new ShowOperation();
        } else if (func.equals("showdetail")) {
            return this.new ShowDetailOperation();
        } else if (func.equals("delete")) {
            return this.new DeleteOperation();
        } else {
            return this.new ShowOperation();
        }

    }

    public static void main(String[] args)
        throws IOException, InterruptedException, KeeperException
    {  
        if (args.length < 2 ||
            (!"show".equals(args[0])
            && !"showdetail".equals(args[0])
            && !"delete".equals(args[0])))
        {  
            System.err.println("Usage: java ZKOperation [show|showdetail|delete] path");
            return;
        }
        //String root = "/hive_zookeeper_namespace";
        String root = args[1];
        ZKOperation zkop = new ZKOperation();
        zkop.createInstance();
        ZKOperation.NodeOperation nodeop = zkop.nodeFactory(args[0]);
        zkop.postorderVisit(root, nodeop);
        zkop.close();
    }
}

