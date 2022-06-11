import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    /**
     * 文件上传
     * @throws IOException  异常
     * @throws InterruptedException 中断异常
     * @throws URISyntaxException   url找不到异常
     */
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "wxg");
        // 2 创建目录
//        fs.mkdirs(new Path("/TestHdfsClient"));
        fs.mkdirs(new Path("/TestHdfsClient1"));
        // 3 关闭资源
        fs.close();
    }
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "wxg");
        // 2 上传文件
        fs.copyFromLocalFile(new Path("C:\\Users\\86187\\Desktop\\毕业论文合集\\1988-2019年A股全部上市公司年报PDF.txt"), new Path("/windows"));
        // 3 关闭资源
        fs.close();
        System.out.println("over");
    }
    //HDFS 文件下载
    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/windows"), new Path("e:/banhua.txt"), true);
        // 3 关闭资源
        fs.close();
    }
    //HDFS 文件夹删除
    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "wxg");
        // 2 执行删除
        fs.delete(new Path("/logs"), true);
        // 3 关闭资源
        fs.close();
    }
    //HDFS 文件名更改
    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "wxg");
        // 2 修改文件名称
        fs.rename(new Path("/windows"), new Path("/windowCopy"));
        // 3 关闭资源
        fs.close();
    }
    //HDFS 文件详情查看查看文件名称、权限、 长度、 块信息
    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "wxg");
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            // 文件名称/长度/权限/分组
            System.out.print(status.getPath().getName() + '\t' + status.getLen() + '\t' +
                                status.getPermission() + '\t' + status.getGroup() + '\t');
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) System.out.print(host + '\t');
                System.out.println();
            }
            //System.out.println("--------------------------------------");
        }
        // 3 关闭资源
        fs.close();
    }

    // 判断是文件夹还是文件
    @Test
    public void testFile() throws IOException, URISyntaxException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "wxg");
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        // 2 判断是文件还是文件夹
        for (FileStatus status : listStatus) {
            if (status.isFile()) System.out.println("文件：" + status.getPath().getName());
            else System.out.println("目录：" + status.getPath().getName());
        }
    }
}