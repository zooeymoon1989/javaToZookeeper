package com.liwenqiang.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;

public class Worker implements Watcher {
    final Logger logger = LoggerFactory.getLogger(Watcher.class);
    private ZooKeeper zk;
    private final String hostPort;
    private final String serverId = Integer.toHexString(new Random().nextInt());
    private ThreadPoolExecutor executor;
    String name;

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
                break;
            default:
                logger.error("Something went wrong: " +
                        KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };

    AsyncCallback.StatCallback statusUpdateCallback = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                updateStatus((String) ctx);
                break;
        }
    };

    String status;

    synchronized private void updateStatus(String status) {
        if (status.equals(this.status)) {
            // -1 表示版本号检查
            zk.setData("/workers/" + name, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

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


    public void register() {
        name = "worker-" + serverId;
        zk.create(
                "/workers/worker-" + name,
                "idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null
        );
    }

    Watcher newTaskWatcher = e -> {
        if (e.getType() == Event.EventType.NodeChildrenChanged) {
            assert new String("/assign/worker-" + serverId).equals(e.getPath());
            getTasks();
        }
    };

    private void getTasks() {
        zk.getChildren("/assign/worker-" + serverId, newTaskWatcher, tasksGetChildrenCallback, null);
    }


    private final AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat){
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.getData(path, false, taskDataCallback, null);
                    break;
                case OK:
                    /*
                     *  Executing a task in this example is simply printing out
                     *  some string representing the task.
                     */
                    executor.execute( new Runnable() {
                        byte[] data;
                        Object ctx;

                        /*
                         * Initializes the variables this anonymous class needs
                         */
                        public Runnable init(byte[] data, Object ctx) {
                            this.data = data;
                            this.ctx = ctx;

                            return this;
                        }

                        public void run() {
                            logger.info("Executing your task: " + new String(data));
                            zk.create("/status/" + (String) ctx, "done".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.PERSISTENT, taskStatusCreateCallback, null);
                            zk.delete("/assign/worker-" + serverId + "/" + (String) ctx,
                                    -1, taskVoidCallback, null);
                        }
                    }.init(data, ctx));

                    break;
                default:
                    logger.error("Failed to get task data: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    protected ChildrenCache assignedTasksCache = new ChildrenCache();
    AsyncCallback.ChildrenCallback tasksGetChildrenCallback = (rc, path, ctx, children) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                if (children != null) {
                    executor.execute(new Runnable() {
                        List<String> children;
                        AsyncCallback.DataCallback cb;

                        /*
                         * Initializes input of anonymous class
                         */
                        public Runnable init(List<String> children, AsyncCallback.DataCallback cb) {
                            this.children = children;
                            this.cb = cb;

                            return this;
                        }

                        public void run() {
                            if (children == null) {
                                return;
                            }

                            logger.info("Looping into tasks");
                            setStatus("Working");
                            for (String task : children) {
                                logger.trace("New task: {}", task);
                                zk.getData("/assign/worker-" + serverId + "/" + task,
                                        false,
                                        cb,
                                        task);
                            }
                        }
                    }.init(assignedTasksCache.addedAndSet(children), taskDataCallback));
                }
                break;
            default:
                System.out.println("getChildren failed: " + KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };



    AsyncCallback.StringCallback taskStatusCreateCallback = new AsyncCallback.StringCallback(){
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.create(path + "/status", "done".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                            taskStatusCreateCallback, null);
                    break;
                case OK:
                    logger.info("Created status znode correctly: " + name);
                    break;
                case NODEEXISTS:
                    logger.warn("Node exists: " + path);
                    break;
                default:
                    logger.error("Failed to create task data: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }

        }
    };


    AsyncCallback.VoidCallback taskVoidCallback = new AsyncCallback.VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    break;
                case OK:
                    logger.info("Task correctly deleted: " + path);
                    break;
                default:
                    logger.error("Failed to delete task data" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    public static void main(String[] args) throws IOException, InterruptedException {
        Worker w = new Worker(args[0]);
        w.startZK();
        Thread.sleep(30000);
    }

}
