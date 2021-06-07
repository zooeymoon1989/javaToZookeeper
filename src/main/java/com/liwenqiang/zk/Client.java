package com.liwenqiang.zk;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Client implements Watcher {
    final Logger logger = LoggerFactory.getLogger(Watcher.class);
    private ZooKeeper zk;
    private final String hostPort;


    Client(String hostPort) {
        this.hostPort = hostPort;
    }

    @Override
    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public String queueCommand(String command) throws Exception {
        while (true) {
            try {
                return zk.create("/tasks/task-", command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            } catch (InterruptedException e) {
                throw new Exception("already appears to be running");
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Client c = new Client(args[0]);
        c.startZK();
        String name = c.queueCommand(args[1]);
        System.out.println("Created " + name);
    }
}
