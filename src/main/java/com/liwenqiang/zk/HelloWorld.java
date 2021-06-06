package com.liwenqiang.zk;

import java.io.IOException;

public class HelloWorld {
    public static void main(String[] args) throws IOException, InterruptedException {
        Master m = new Master(args[0]);
        m.startZK();
        Thread.sleep(20000);
        m.stopZK();
    }
}
