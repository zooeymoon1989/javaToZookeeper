package com.liwenqiang.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.apache.log4j.helpers.LogLog.warn;

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

    static class TaskObject {
        private String task;
        private String taskName;
        private boolean done = false;
        private boolean succesful = false;
        private CountDownLatch latch = new CountDownLatch(1);

        String getTask () {
            return task;
        }

        void setTask (String task) {
            this.task = task;
        }

        void setTaskName(String name){
            this.taskName = name;
        }

        String getTaskName (){
            return taskName;
        }

        void setStatus (boolean status){
            succesful = status;
            done = true;
            latch.countDown();
        }

        void waitUntilDone () {
            try{
                latch.await();
            } catch (InterruptedException e) {
                warn("InterruptedException while waiting for task to get done");
            }
        }

        synchronized boolean isDone(){
            return done;
        }

        synchronized boolean isSuccesful(){
            return succesful;
        }

    }


    AsyncCallback.StringCallback createTaskCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                     * Handling connection loss for a sequential node is a bit
                     * delicate. Executing the ZooKeeper create command again
                     * might lead to duplicate tasks. For now, let's assume
                     * that it is ok to create a duplicate task.
                     */
                    submitTask(((TaskObject) ctx).getTask(), (TaskObject) ctx);

                    break;
                case OK:
                    logger.info("My created task name: " + name);
                    ((TaskObject) ctx).setTaskName(name);
                    watchStatus(name.replace("/tasks/", "/status/"), ctx);

                    break;
                default:
                    logger.error("Something went wrong" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    protected ConcurrentHashMap<String, Object> ctxMap = new ConcurrentHashMap<String, Object>();

    void watchStatus(String path, Object ctx){
        ctxMap.put(path, ctx);
        zk.exists(path,
                statusWatcher,
                existsCallback,
                ctx);
    }

    Watcher statusWatcher = new Watcher(){
        public void process(WatchedEvent e){
            if(e.getType() == Event.EventType.NodeCreated) {
                assert e.getPath().contains("/status/task-");
                assert ctxMap.containsKey( e.getPath() );

                zk.getData(e.getPath(),
                        false,
                        getDataCallback,
                        ctxMap.get(e.getPath()));
            }
        }
    };

    AsyncCallback.StatCallback existsCallback = new AsyncCallback.StatCallback(){
        public void processResult(int rc, String path, Object ctx, Stat stat){
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    watchStatus(path, ctx);

                    break;
                case OK:
                    if(stat != null){
                        zk.getData(path, false, getDataCallback, ctx);
                        logger.info("Status node is there: " + path);
                    }

                    break;
                case NONODE:
                    break;
                default:
                    logger.error("Something went wrong when " +
                            "checking if the status node exists: " +
                            KeeperException.create(KeeperException.Code.get(rc), path));

                    break;
            }
        }
    };

    AsyncCallback.DataCallback getDataCallback = new AsyncCallback.DataCallback(){
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                     * Try again.
                     */
                    zk.getData(path, false, getDataCallback, ctxMap.get(path));
                    return;
                case OK:
                    /*
                     *  Print result
                     */
                    String taskResult = new String(data);
                    logger.info("Task " + path + ", " + taskResult);

                    /*
                     *  Setting the status of the task
                     */
                    assert(ctx != null);
                    ((TaskObject) ctx).setStatus(taskResult.contains("done"));

                    /*
                     *  Delete status znode
                     */
                    //zk.delete("/tasks/" + path.replace("/status/", ""), -1, taskDeleteCallback, null);
                    zk.delete(path, -1, taskDeleteCallback, null);
                    ctxMap.remove(path);
                    break;
                case NONODE:
                    logger.warn("Status node is gone!");
                    return;
                default:
                    logger.error("Something went wrong here, " +
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    AsyncCallback.VoidCallback taskDeleteCallback = new AsyncCallback.VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.delete(path, -1, taskDeleteCallback, null);
                    break;
                case OK:
                    logger.info("Successfully deleted " + path);
                    break;
                default:
                    logger.error("Something went wrong here, " +
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    void submitTask(String task, TaskObject taskCtx){
        taskCtx.setTask(task);
        zk.create("/tasks/task-",
                task.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL,
                createTaskCallback,
                taskCtx);
    }


//    Op deleteZnode(String z) {
//        return Op.delete(z,-1);
//    }

    public static void main(String[] args) throws Exception {
        Client c = new Client(args[0]);
        c.startZK();
        String name = c.queueCommand(args[1]);
        System.out.println("Created " + name);
    }
}
