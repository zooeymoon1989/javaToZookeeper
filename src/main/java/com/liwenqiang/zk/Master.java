package com.liwenqiang.zk;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class Master implements Watcher {
    final Logger logger = LoggerFactory.getLogger(Watcher.class);
    private ZooKeeper zk;
    private final String hostPort;
    private final String serverId = Integer.toHexString(new Random().nextInt());
    public boolean isLeader = false;
    public AsyncCallback.DataCallback masterCheckCallback = (rc, path, ctx, data, name) -> {
        switch (KeeperException.Code.get(rc)) {
            // 失去连接时
            case CONNECTIONLOSS:
                checkMaster();
                break;
            // 找不到节点
            case NONODE:
                runForMaster();
        }
    };
    public AsyncCallback.StringCallback masterCreateCallback = (rc, path, ctx, name) -> {
        // 获取异常编码
        switch (KeeperException.Code.get(rc)) {
            // 失去连接
            case CONNECTIONLOSS:
                createParent(path, (byte[]) ctx);
                break;
            // 正常
            case OK:
                logger.info("Parent created");
                break;
            // 如果节点已经存在
            case NODEEXISTS:
                logger.warn("Parent already registered: " + path);
                break;
            default:
                logger.error("Something went wrong: ", KeeperException.create(KeeperException.Code.get(rc), path));
        }

        System.out.println("I'm " + (isLeader ? "" : "not") + "the leader");
    };

    public Master(String hostPort) {
        this.hostPort = hostPort;
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public void stopZK() throws InterruptedException {
        zk.close();
    }

    public void checkMaster() {
        // 异步获得数据
        zk.getData("/master", false, masterCheckCallback, null);
    }

    public void runForMaster() {
        zk.create(
                "/master",
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                masterCreateCallback,
                null);
    }

    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    public void createParent(String path, byte[] data) {
        zk.create(
                path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                masterCreateCallback,
                data
        );
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Master m = new Master(args[0]);
        m.startZK();
        m.runForMaster();
        if (m.isLeader) {
            System.out.println("I'm the leader");
            Thread.sleep(20000);
        } else {
            System.out.println("Someone else is the leader");
        }

        m.stopZK();
    }
}
