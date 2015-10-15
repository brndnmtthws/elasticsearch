package org.apache.mesos.elasticsearch.scheduler.cluster;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.scheduler.healthcheck.AsyncPing;
import org.apache.mesos.elasticsearch.scheduler.state.ESTaskStatus;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableState;
import org.apache.mesos.elasticsearch.scheduler.state.StatePath;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Contains all cluster information. Monitors state of cluster elements.
 */
public class ClusterMonitor {
    private static final Logger LOGGER = Logger.getLogger(ClusterMonitor.class);
    private final Configuration configuration;
    private final Scheduler callback;
    private final Map<Protos.TaskInfo, AsyncPing> healthChecks = new HashMap<>();
    private FrameworkState frameworkState;
    private SerializableState zookeeperStateDriver;

    public ClusterMonitor(Configuration configuration, FrameworkState frameworkState, SerializableState zookeeperStateDriver, Scheduler callback) {
        this.frameworkState = frameworkState;
        this.zookeeperStateDriver = zookeeperStateDriver;
        if (configuration == null || callback == null) {
            throw new InvalidParameterException("Constructor parameters cannot be null.");
        }
        this.configuration = configuration;
        this.callback = callback;

        frameworkState.onRegistered(clusterState -> clusterState.getTaskList().forEach(this::startMonitoringTask));
        frameworkState.onNewTask(this::startMonitoringTask);
        frameworkState.onStatusUpdate(this::updateTask);
    }

    // makes
    public void startMonitoringTask(ESTaskStatus esTask) {
        startMonitoringTask(esTask.getTaskInfo());
    }

    /**
     * Start monitoring a task
     * @param taskInfo The task to monitor
     */
    // Precondition: task is in ZK
    // makes
    //   1 GET request to "/frameworkId"
    //   1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskInfo.getTaskId()
    public void startMonitoringTask(Protos.TaskInfo taskInfo) {
        // frameworkState.getFrameworkID() makes:
        //   1 GET request to "/frameworkId"
        final Protos.FrameworkID frameworkID = frameworkState.getFrameworkID();

        LOGGER.debug("Start monitoring: " + taskInfo.getTaskId().getValue() + " for frameworkId: " + frameworkID.getValue());

        // `new ESTaskStatus` makes:
        //     1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskInfo.getTaskId()
        healthChecks.put(
                taskInfo,
                new AsyncPing(
                        callback,
                        frameworkState.getDriver(),
                        configuration,
                        new ESTaskStatus(zookeeperStateDriver, frameworkID, taskInfo, new StatePath(zookeeperStateDriver))
                )
        );
    }

    private void stopMonitoringTask(Protos.TaskInfo taskInfo) {
        LOGGER.debug("Stop monitoring: " + taskInfo.getTaskId().getValue());
        healthChecks.remove(taskInfo).stop(); // Remove task from list and stop its healthchecks.
    }

    private List<Protos.TaskID> getTaskIDList() {
        return healthChecks.keySet().stream().map(Protos.TaskInfo::getTaskId).collect(Collectors.toList());
    }

    private Protos.TaskInfo getTaskInfo(Protos.TaskID taskID) {
        return healthChecks.keySet().stream().filter(taskInfo -> taskInfo.getTaskId().equals(taskID)).findFirst().get();
    }

    /**
     * Updates a task with the given status. Status is written to zookeeper.
     * If the task is in error, then the healthchecks are stopped and state is removed from ZK
     * @param status A received task status
     */
    private void updateTask(Protos.TaskStatus status) {
        if (!getTaskIDList().contains(status.getTaskId())) {
            LOGGER.warn("Could not find task in monitor list.");
            return;
        }

        try {
            if (ESTaskStatus.errorState(status.getState())) {
                LOGGER.error("Task in error state. Removing executor from monitor list: " + status.getExecutorId().getValue() + ", due to: " + status.getState());
                stopMonitoringTask(getTaskInfo(status.getTaskId()));
            }
        } catch (IllegalStateException | IllegalArgumentException e) {
            LOGGER.error("Unable to write executor state to zookeeper", e);
        }
    }

    public Map<Protos.TaskInfo, AsyncPing> getHealthChecks() {
        return healthChecks;
    }
}
