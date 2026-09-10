/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.blobstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.shade.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.storm.testing.InProcessZookeeper;
import org.junit.jupiter.api.Test;

class KeySequenceNumberTest {
    private static final String KEY = "dep-lib-11111111-1111-1111-1111-111111111111.jar";
    private static final NimbusInfo LEADER = new NimbusInfo("nimbus-1", 6627, false);
    private static final NimbusInfo PEER = new NimbusInfo("nimbus-0", 6627, false);

    /**
     * Replays the blob store state changes behind the nimbus log reported on STORM-3871, where a dependency blob that
     * was removed when its topology was cleaned up showed up on the leader again, registered at version 0 and then 1.
     */
    @Test
    void aBlobThatAnotherNimbusRegistersAgainAfterItWasDeletedIsRecreatedOnTheLeader() throws Exception {
        try (InProcessZookeeper zk = new InProcessZookeeper();
             CuratorFramework zkClient = CuratorFrameworkFactory.newClient("localhost:" + zk.getPort(),
                 new ExponentialBackoffRetry(1000, 3))) {
            zkClient.start();

            // the client uploads the dependency: createBlob, then createStateInZookeeper when it closes the stream
            assertEquals(1, register(zkClient, LEADER));
            assertEquals(2, register(zkClient, LEADER));

            // the topology is cleaned up and the leader deletes the blob, as LocalFsBlobStore#deleteBlob does
            zkClient.delete().deletingChildrenIfNeeded().forPath("/blobstore/" + KEY);
            zkClient.delete().deletingChildrenIfNeeded().forPath("/blobstoremaxkeysequencenumber/" + KEY);

            // another nimbus, which still has a copy, registers the key again as if it were new
            assertEquals(1, register(zkClient, PEER));

            // a request for the key makes the leader download it back from that nimbus: createBlob, then
            // createStateInZookeeper, which are the set-path lines ending in -0 and -1 in the report
            assertEquals(0, register(zkClient, LEADER));
            assertEquals(1, register(zkClient, LEADER));
        }
    }

    /**
     * Do what IStormClusterState#setupBlob does with the version KeySequenceNumber hands out.
     */
    private static int register(CuratorFramework zkClient, NimbusInfo nimbus) throws Exception {
        int version = new KeySequenceNumber(KEY, nimbus).getKeySequenceNumber(zkClient);
        String parent = "/blobstore/" + KEY;
        if (zkClient.checkExists().forPath(parent) != null) {
            for (String child : zkClient.getChildren().forPath(parent)) {
                if (child.startsWith(nimbus.toHostPortString())) {
                    zkClient.delete().forPath(parent + "/" + child);
                }
            }
        }
        zkClient.create().creatingParentsIfNeeded().forPath(parent + "/" + nimbus.toHostPortString() + "-" + version);
        return version;
    }
}
