package com.liwenqiang.zk;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class Worker implements Watcher {
    final Logger logger = LoggerFactory.getLogger(Watcher.class);
    private ZooKeeper zk;
    private final String hostPort;
    private final String serverId = Integer.toHexString(new Random().nextInt());

    @Override
    public void process(WatchedEvent e) {
        logger.info(e.toString() + "," + hostPort);
    }

    public Worker(String hostPort) {
        this.hostPort = hostPort;
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    AsyncCallback.StringCallback createWorkerCallback = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                register();
                break;
            case OK:
                logger.info("Registered successfully:" + serverId);
                break;
            case NODEEXISTS:
                logger.warn("Something went wrong:" + KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };

    public void register() {
        zk.create(
                "/workers/worker-" + serverId,
                "idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null
        );
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Worker w = new Worker(args[0]);
        w.startZK();
        Thread.sleep(30000);
    }

}
