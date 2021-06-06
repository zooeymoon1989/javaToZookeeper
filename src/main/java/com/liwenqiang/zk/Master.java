package com.liwenqiang.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Master implements Watcher {
    ZooKeeper zk;
    String hostPort;

    public Master(String hostPort) {
        this.hostPort = hostPort;
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort,15000,this);
    }
    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public void stopZK() throws InterruptedException {
        zk.close();
    }
}
