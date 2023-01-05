package org.hdfs;

public class Main {
    public static void main(String[] args) throws Exception {
        IO client = new IO();
            client.setUp();
            client.mkdir();
            client.create();
            client.cat();
            client.copyFromLocalFile();
            client.copyToLocalFile();
            client.rename();
            client.listFiles();
            client.tearDown();
    }
}