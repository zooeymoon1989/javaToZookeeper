package com.liwenqiang.zk;

import java.io.IOException;

public class HelloWorld {
    public static void main(String[] args) throws IOException, InterruptedException {
        Master m = new Master(args[0]);
        m.startZK();
        m.runForMaster();
        if (m.isLeader) {
            System.out.println("I'm the leader");
            Thread.sleep(20000);
        }else{
            System.out.println("Someone else is the leader");
        }

        m.stopZK();
    }
}
