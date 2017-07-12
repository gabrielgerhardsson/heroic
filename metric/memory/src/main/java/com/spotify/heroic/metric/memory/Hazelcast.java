package com.spotify.heroic.metric.memory;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Hazelcast {
    public static HazelcastInstance getClientInstance(List<String> servers) {

        String user = "test";
        String pass = "password";
        ClientConfig clientConfig = new ClientConfig();
        //clientConfig.setGroupConfig(new GroupConfig(user, pass));

        ClientNetworkConfig networkConfig = new ClientNetworkConfig();
        for (String address : servers) {
            log.info("Will connect to: " + address);
            networkConfig.addAddress();
        }
        HazelcastInstance instance = HazelcastClient.newHazelcastClient(clientConfig);

        log.info("Connected to Hazelcast. Cluster: " + instance.getCluster().getMembers());
        return instance;
    }
}
