package com.code.collection.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.stream.Stream;

public class HDFSClientTest {


    @Test
    public void initHDFS() throws IOException, InterruptedException {

        System.setProperty("hadoop.home.dir", "C:\\develop\\hadoop-2.7.2");

        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        String uri = "hdfs://192.168.31.2:9000/";
        FileSystem fs = FileSystem.get(URI.create(uri), config,"root");

        System.out.println(fs);
    }

    @Test
    public void makeDirAtHDFS() throws IOException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\develop\\hadoop-2.7.2");

        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        String uri = "hdfs://192.168.31.2:9000/";
        FileSystem fs = FileSystem.get(URI.create(uri), config,"root");

        Path path = new Path("hdfs://192.168.31.2:9000/java/client/testdir");
        fs.mkdirs(path);
    }

    @Test
    public void deleteAtHDFS() throws IOException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\develop\\hadoop-2.7.2");

        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        String uri = "hdfs://192.168.31.2:9000/";
        FileSystem fs = FileSystem.get(URI.create(uri), config,"root");

        Path path = new Path("hdfs://192.168.31.2:9000/java/client/testdir");

        //true 表示递归删除
        fs.delete(path,true);
    }

    @Test
    public void putFileToHDFS() throws IOException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\develop\\hadoop-2.7.2");

        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.set("dfs.replication","3");

        String uri = "hdfs://192.168.31.2:9000/";
        FileSystem fs = FileSystem.get(URI.create(uri), config,"root");

        Path src = new Path("D:\\bigdata\\02_尚硅谷大数据技术之Hadoop\\2.资料\\a.zip");
        Path dst = new Path("hdfs://192.168.31.2:9000/java/client/");
        fs.copyFromLocalFile(src,dst);
    }

    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\develop\\hadoop-2.7.2");

        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.set("dfs.replication","3");

        String uri = "hdfs://192.168.31.2:9000/";
        FileSystem fs = FileSystem.get(URI.create(uri), config,"root");

        Path src = new Path("hdfs://192.168.31.2:9000/java/client/");
        Path dst = new Path("C:\\Users\\zy127\\Desktop\\hadoopFile\\");
        fs.copyToLocalFile(src,dst);
    }

    @Test
    public void renameAtHDFS() throws IOException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\develop\\hadoop-2.7.2");

        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.set("dfs.replication","3");

        String uri = "hdfs://192.168.31.2:9000/";
        FileSystem fs = FileSystem.get(URI.create(uri), config,"root");

        Path src = new Path("hdfs://192.168.31.2:9000/java/client/a.zip");
        Path dst = new Path("hdfs://192.168.31.2:9000/java/client/aaaaaaaaaaaaaaa.zip");
        fs.rename(src,dst);
    }

    @Test
    public void readListFiles() throws IOException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\develop\\hadoop-2.7.2");

        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.set("dfs.replication","3");

        String uri = "hdfs://192.168.31.2:9000/";
        FileSystem fs = FileSystem.get(URI.create(uri), config,"root");

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while(files.hasNext()){
            System.out.println("");

            LocatedFileStatus next = files.next();
            System.out.println(next.getPath().getName());
            System.out.println(next.getBlockSize());
            System.out.println(next.getPermission());
            System.out.println(next.getLen());
            System.out.println("");

            BlockLocation[] blockLocations = next.getBlockLocations();
            Stream.of(blockLocations).forEach(item->{
                System.out.println("block-offset:"+item.getOffset());

                try {

                    String[] hosts = item.getHosts();
                    Stream.of(hosts).forEach(System.out::println);

                } catch (IOException e) {
                    e.printStackTrace();
                }

            });
        }
    }

    @Test
    public void findAtHDFS() throws IOException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\develop\\hadoop-2.7.2");

        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.set("dfs.replication","3");

        String uri = "hdfs://192.168.31.2:9000/";
        FileSystem fs = FileSystem.get(URI.create(uri), config,"root");

        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        Stream.of(fileStatuses).forEach(item->{
            if(item.isFile()){
                System.out.println("f--"+item.getPath().getName());
            }else{
                System.out.println("d--"+item.getPath().getName());
            }
        });
    }




}
