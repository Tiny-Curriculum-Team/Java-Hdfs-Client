package org.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;
import java.util.Arrays;

public class IO {

    public static final String HDFS_PATH = "hdfs://127.0.0.1:9000";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    public void setUp() throws Exception{
        System.out.println("set up");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;
        System.out.print("tearDowm");
    }

    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/user/hadoop/newdir"));
    }

    public void create() throws Exception{
        FSDataOutputStream outputStream = fileSystem.create(new Path("/user/hadoop/test.txt"));
        outputStream.write("Hello World".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    public void cat() throws Exception{
        FSDataInputStream inputStream = fileSystem.open(new Path("/user/hadoop/test.txt"));
        IOUtils.copyBytes(inputStream, System.out, 1024);
        inputStream.close();
    }

    public void rename() throws Exception{
        fileSystem.rename(new Path("/user/hadoop/test.txt"), new Path("/user/hadoop/test1.txt"));
    }

    public void copyFromLocalFile() throws Exception{
        fileSystem.copyFromLocalFile(new Path("/home/breezeshane/testfile"), new Path("/user/hadoop/"));
    }

    public void copyToLocalFile() throws Exception{
        fileSystem.copyToLocalFile(false, new Path("/user/hadoop/test.txt"), new Path("/home/breezeshane/test.txt"), true);
        //public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem)
        //使用java io流 而不使用本地文件系统，windows会报空指针异常
    }

    public void listFiles() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/user/hadoop"));
        Arrays.stream(fileStatuses).forEach(fileStatus -> {
            String isDir = fileStatus.isDirectory()? "文件夹" : "文件";
            Short replication = fileStatus.getReplication(); //副本系数
            /**
             *如果采用HDFS Shell put的方式上传文件，采用服务器设置的默认副本系数
             *如果采用java api 方式上传文件，本地没有设置副本系数，默认采用hadoop默认副本系数3
             **/
            Long blockSize = fileStatus.getBlockSize();
            Long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();

            System.out.println(isDir+"\t"+replication+"\t"+blockSize+"\t"+len+"\t"+path);
        });
    }

    public void delete() throws Exception{
        fileSystem.delete(new Path("/user/hadoop/todel"),true);//是否递归删除
    }
}
