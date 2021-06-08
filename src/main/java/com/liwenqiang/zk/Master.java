package com.liwenqiang.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class Master implements Watcher {
    final Logger logger = LoggerFactory.getLogger(Watcher.class);
    private ZooKeeper zk;
    private final String hostPort;
    private final String serverId = Integer.toHexString(new Random().nextInt());
    public boolean isLeader = false;
    ChildrenCache workersCache;
    Watcher masterExistsWatcher = e -> {
        if (e.getType() == Event.EventType.NodeDeleted) {
            assert "/master".equals(e.getPath());
            runForMaster();
        }
    };


    public void reassignAndSet(List<String> children) {
        List<String> toProcess;

        if (workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            logger.info("Removing and setting");
            toProcess = workersCache.removedAndSet(children);
        }

        if (toProcess != null) {
            for (String worker : toProcess) {
                getAbsentWorkerTasks(worker);
            }
        }

    }

    private void getAbsentWorkerTasks(String worker) {
        zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }

    AsyncCallback.ChildrenCallback workerAssignmentCallback = (rc, path, ctx, children) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getAbsentWorkerTasks(path);
                break;
            case OK:
                logger.info("Successfully got a list of assignments: " + children.size() + " tasks");
                for (String task : children) {
                    getDataReassign(path + "/" + task, task);
                }
                break;
            default:
                logger.error("getChildren failed", KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };

    private void getDataReassign(String path, String task) {
        zk.getData(path, false, getDataReassignCallback, task);
    }

    AsyncCallback.DataCallback getDataReassignCallback = (rc, path, ctx, data, stat) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getDataReassign(path, (String) ctx);
                break;
            case OK:
                recreateTask()
        }
    }

    Watcher workersChangeWatcher = e -> {
        if ((e.getType()) == Event.EventType.NodeChildrenChanged) {
            assert "/workers".equals(e.getPath());
            getWorkers();
        }
    };

    private final AsyncCallback.ChildrenCallback workersGetChildrenCallback = (rc, path, ctx, children) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getWorkers();
                break;
            case OK:
                logger.info("Successfully got a list of workers:" + children.size() + " workers");
                break;
            default:
                logger.error("getChildren failed", KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };

    enum MasterStates {RUNNING, ELECTED, NOTELECTED}

    private volatile MasterStates state = MasterStates.RUNNING;

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
                checkMaster();
                break;
            // 正常
            case OK:
                state = MasterStates.ELECTED;
                takeLeadership();
                break;
            // 如果节点已经存在
            case NODEEXISTS:
                state = MasterStates.NOTELECTED;
                masterExists();
                break;
            default:
                state = MasterStates.NOTELECTED;
                logger.error("Something went wrong when running for master.", KeeperException.create(KeeperException.Code.get(rc), path));
        }

        System.out.println("I'm " + (isLeader ? "" : "not") + "the leader");
    };

    private final AsyncCallback.StatCallback masterExistsCallback = (rc, path, ctx, stat) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                masterExists();
                break;
            case OK:
                if (stat == null) {
                    state = MasterStates.RUNNING;
                    runForMaster();
                }
                break;
            default:
                checkMaster();
                break;
        }
    };

    void getWorkers() {
        zk.getChildren("/workers",
                workersChangeWatcher,
                workersGetChildrenCallback,
                null);
    }


    private void masterExists() {
        zk.exists("/master", masterExistsWatcher, masterExistsCallback, null);
    }

    private void takeLeadership() {
    }

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
