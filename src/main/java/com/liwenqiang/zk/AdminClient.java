package com.liwenqiang.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

public class AdminClient implements Watcher {

    final Logger logger = LoggerFactory.getLogger(Watcher.class);
    private ZooKeeper zk;
    private final String hostPort;

    @Override
    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    void start() throws IOException {
        zk = new ZooKeeper(hostPort,15000,this);
    }

    void listState() throws InterruptedException, KeeperException {
        try {
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master",false,stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: "+ new String(masterData) + " since "+ startDate);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("No master");
        } catch (KeeperException e) {
            e.printStackTrace();
        }

        System.out.println("Workers:");

        for (String w: zk.getChildren("/workers",false)) {
            byte[] data = zk.getData("/workers/"+w,false,null);
            String state = new String(data);
            System.out.println("\t" + w + ": "+ state);
        }

        System.out.println("Tasks:");

        for (String t: zk.getChildren("/assign",false)) {
            System.out.println("\t" + t);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        AdminClient c = new AdminClient(args[0]);
        c.start();
        c.listState();
    }
}
