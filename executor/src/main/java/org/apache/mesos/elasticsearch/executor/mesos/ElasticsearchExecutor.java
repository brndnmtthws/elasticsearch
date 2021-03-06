package org.apache.mesos.elasticsearch.executor.mesos;

import com.google.protobuf.InvalidProtocolBufferException;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionTimeoutException;
import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.executor.Configuration;
import org.apache.mesos.elasticsearch.executor.elasticsearch.Launcher;
import org.apache.mesos.elasticsearch.executor.elasticsearch.NodeUtil;
import org.apache.mesos.elasticsearch.executor.model.HostsModel;
import org.apache.mesos.elasticsearch.executor.model.PortsModel;
import org.apache.mesos.elasticsearch.executor.model.RunTimeSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Executor for Elasticsearch.
 */
@SuppressWarnings("PMD.TooManyMethods")
public class ElasticsearchExecutor implements Executor {
    public static final long ES_TIMEOUT = 120L;
    public static final String ES_STATUS_GREEN = "green";
    private final Launcher launcher;
    public static final Logger LOGGER = Logger.getLogger(ElasticsearchExecutor.class.getCanonicalName());
    private final TaskStatus taskStatus;
    private final NodeUtil nodeUtil;
    private Configuration configuration;
    private Node node;

    public ElasticsearchExecutor(Launcher launcher, TaskStatus taskStatus, NodeUtil nodeUtil) {
        this.launcher = launcher;
        this.taskStatus = taskStatus;
        this.nodeUtil = nodeUtil;
    }

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("Executor Elasticsearch registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("Executor Elasticsearch re-registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        LOGGER.info("Executor Elasticsearch disconnected");
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final Protos.TaskInfo task) {
        LOGGER.info("Starting executor with a TaskInfo of:");
        LOGGER.info(task.toString());

        Protos.TaskID taskID = task.getTaskId();
        taskStatus.setTaskID(taskID);

        // Send status update, starting
        driver.sendStatusUpdate(taskStatus.starting());

        try {
            // Parse CommandInfo arguments
            List<String> list = task.getExecutor().getCommand().getArgumentsList();
            String[] args = list.toArray(new String[list.size()]);
            LOGGER.debug("Using arguments: " + Arrays.toString(args));
            configuration = new Configuration(args);

            // Add settings provided in es Settings file
            LOGGER.debug("Using elasticsearch settings file: " + configuration.getElasticsearchCLI().getElasticsearchSettingsLocation());
            Settings.Builder esSettings = configuration.getElasticsearchYmlSettings();
            launcher.addRuntimeSettings(esSettings);

            // Parse ports
            RunTimeSettings ports = new PortsModel(task);
            launcher.addRuntimeSettings(ports.getRuntimeSettings());

            // Parse unicast hosts
            RunTimeSettings hostsModel = new HostsModel(configuration.getElasticsearchHosts());
            launcher.addRuntimeSettings(hostsModel.getRuntimeSettings());

            // Parse cluster name
            launcher.addRuntimeSettings(Settings.builder().put("cluster.name", configuration.getElasticsearchCLI().getElasticsearchClusterName()));

            // Print final settings for logs.
            LOGGER.debug(launcher.toString());

            // Launch Node
            node = launcher.launch();

            try {
                Awaitility.await()
                        .atMost(ES_TIMEOUT, TimeUnit.SECONDS)
                        .pollInterval(1L, TimeUnit.SECONDS)
                        .until(() -> nodeUtil.getNodeStatus(node).equals(ES_STATUS_GREEN));

                // Send status update, running
                driver.sendStatusUpdate(taskStatus.running());
            } catch (ConditionTimeoutException e) {
                LOGGER.error("ES node was not green within " + ES_TIMEOUT + "s.");
                driver.sendStatusUpdate(taskStatus.failed());
                killTask(driver, taskID);
            }
        } catch (InvalidParameterException e) {
            driver.sendStatusUpdate(taskStatus.failed());
            LOGGER.error(e);
        }
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        LOGGER.info("Kill task: " + taskId.getValue());
        driver.sendStatusUpdate(taskStatus.failed());
        stopNode();
        stopDriver(driver, taskStatus.killed());
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        try {
            Protos.HealthCheck healthCheck = Protos.HealthCheck.parseFrom(data);
            LOGGER.info("HealthCheck request received: " + healthCheck.toString());
            driver.sendStatusUpdate(taskStatus.currentState());
        } catch (InvalidProtocolBufferException e) {
            LOGGER.debug("Unable to parse framework message as HealthCheck", e);
        }
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
        LOGGER.info("Shutting down framework...");
        stopNode();
        stopDriver(driver, taskStatus.finished());
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        LOGGER.info("Error in executor: " + message);
        stopNode();
        stopDriver(driver, taskStatus.error());
    }

    private void stopDriver(ExecutorDriver driver, Protos.TaskStatus reason) {
        driver.sendStatusUpdate(reason);
        driver.stop();
    }

    private void stopNode() {
        if (node != null) {
            node.close();
        }
    }
}
