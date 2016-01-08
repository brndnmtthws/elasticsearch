package org.apache.mesos.elasticsearch.executor.mesos;

import com.google.protobuf.InvalidProtocolBufferException;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.NotRegisteredException;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.executor.Configuration;
import org.apache.mesos.elasticsearch.executor.elasticsearch.Launcher;
import org.apache.mesos.elasticsearch.executor.model.HostsModel;
import org.apache.mesos.elasticsearch.executor.model.PortsModel;
import org.apache.mesos.elasticsearch.executor.model.RunTimeSettings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import com.orbitz.consul.Consul;
import org.elasticsearch.search.aggregations.support.format.ValueParser;

import java.net.*;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

/**
 * Executor for Elasticsearch.
 */
@SuppressWarnings("PMD.TooManyMethods")
public class ElasticsearchExecutor implements Executor {
    private final Launcher launcher;
    public static final Logger LOGGER = Logger.getLogger(ElasticsearchExecutor.class.getCanonicalName());
    private final TaskStatus taskStatus;
    private Configuration configuration;
    private String registerIpAddress;
    private Consul consul;
    private Node node;

    public ElasticsearchExecutor(Launcher launcher, TaskStatus taskStatus) {
        this.launcher = launcher;
        this.taskStatus = taskStatus;
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
            if (configuration.getAdvertiseIp().isEmpty()) {
                registerIpAddress = determineExternalIp();
                if (registerIpAddress.isEmpty()) {
                    LOGGER.error("Cannot determine external IP address to register with");
                }
            } else {
                registerIpAddress = configuration.getAdvertiseIp();
            }
            createConsul(configuration.getConsulEndpoint());

            // Add settings provided in es Settings file
            URL elasticsearchSettingsPath = java.net.URI.create(configuration.getElasticsearchSettingsLocation()).toURL();
            LOGGER.debug("Using elasticsearch settings file: " + elasticsearchSettingsPath);
            ImmutableSettings.Builder esSettings = ImmutableSettings.builder().loadFromUrl(elasticsearchSettingsPath);
            launcher.addRuntimeSettings(esSettings);

            // Parse ports
            RunTimeSettings ports = new PortsModel(task);
            launcher.addRuntimeSettings(ports.getRuntimeSettings());

            // Parse unicast hosts
            RunTimeSettings hostsModel = new HostsModel(configuration.getElasticsearchHosts());
            launcher.addRuntimeSettings(hostsModel.getRuntimeSettings());

            // Parse cluster name
            launcher.addRuntimeSettings(ImmutableSettings.builder().put("cluster.name", configuration.getElasticsearchClusterName()));

            // Print final settings for logs.
            LOGGER.debug(launcher.toString());

            // Launch Node
            node = launcher.launch();
            registerConsulService(ports.getRuntimeSettings().get("http.port"), registerIpAddress);

            // Send status update, running
            driver.sendStatusUpdate(taskStatus.running());
        } catch (InvalidParameterException | MalformedURLException | BindException e) {
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

    private void createConsul(String endpoint) {
        if (endpoint.isEmpty()) {
            LOGGER.debug("Consul endpoint is empty");
            return;
        }
        LOGGER.debug("Building consul with endpoint " + endpoint);
        consul = Consul.builder().withUrl(endpoint).build();

    }

    private void registerConsulService(String port, String address)  {
        if (consul == null) {
            LOGGER.debug("Consul object is null");
            return;
        }
        AgentClient consulAgent = consul.agentClient();
        LOGGER.debug("Agent object ok. Registering port " + port + " on " + address);
        String serviceId = "es-" + address.replace(".", "-");
        Registration registration = ImmutableRegistration.builder()
                .port(Integer.parseInt(port))
                .address(address).id(serviceId)
                .check(Registration.RegCheck.http(String.format("http://%s:%s", address, port), 5))
                .name("elasticsearch")
                .build();
        consulAgent.register(registration);
        try {
            consulAgent.pass(serviceId);
        } catch (NotRegisteredException e) {
            LOGGER.error("Pass not succeeded: " + e.getMessage());
        }

    }
    @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
    private String determineExternalIp() throws BindException {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface netInterface = interfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = netInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (!inetAddress.isLoopbackAddress() && inetAddress instanceof Inet4Address) {
                        return inetAddress.getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {
            LOGGER.error(e.getStackTrace());
        }
        throw new BindException("Cannot determine external IP address to register with");
    }
}