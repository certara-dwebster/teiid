/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.jboss;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.wildfly.clustering.jgroups.ChannelFactory;

class NodeTrackerService implements Service<NodeTracker> {
    public final Supplier<ChannelFactory> channelFactory;
    private NodeTracker tracker;
    private String nodeName;
    private ScheduledExecutorService scheduler;

    public NodeTrackerService(String nodeName, ScheduledExecutorService scheduler, Supplier<ChannelFactory> channelFactorySupplier) {
        this.nodeName = nodeName;
        this.scheduler = scheduler;
        this.channelFactory = channelFactorySupplier;
    }

    @Override
    public void start(StartContext context) throws StartException {
        try {
            NodeTracker tracker = new NodeTracker(channelFactory.get().createChannel("teiid-node-tracker"), this.nodeName) {
                @Override
                public ScheduledExecutorService getScheduledExecutorService() {
                    return scheduler;
                }
            };
        } catch (Exception e) {
            throw new StartException(e);
        }
    }

    @Override
    public void stop(StopContext context) {
        this.tracker = null;
    }

    @Override
    public NodeTracker getValue() throws IllegalStateException,IllegalArgumentException {
        return this.tracker;
    }
}
