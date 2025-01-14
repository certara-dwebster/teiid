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

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.teiid.common.buffer.BufferManager;
import org.teiid.deployers.VDBRepository;
import org.teiid.query.ObjectReplicator;

import java.util.function.Consumer;
import java.util.function.Supplier;

class VDBRepositoryService implements Service<VDBRepository> {
    private VDBRepository repo;
    private Consumer<VDBRepository> repositoryConsumer;
    protected final Supplier<BufferManager> bufferManager;
    protected final Supplier<ObjectReplicator> objectReplicator;

    public VDBRepositoryService(VDBRepository repo, Supplier<BufferManager> bufferManagerDep, Supplier<ObjectReplicator> objectReplicatorDep, Consumer<VDBRepository> repositoryConsumer) {
        this.repo = repo;
        this.bufferManager = bufferManagerDep;
        this.objectReplicator = objectReplicatorDep;
        this.repositoryConsumer = repositoryConsumer;
    }

    @Override
    public void start(StartContext context) throws StartException {
        repo.setBufferManager(this.bufferManager.get());
        repo.setObjectReplicator(this.objectReplicator.get());
        repo.start();
        repositoryConsumer.accept(repo);
    }

    @Override
    public void stop(StopContext context) {
    }

    @Override
    public VDBRepository getValue() throws IllegalStateException, IllegalArgumentException {
        return repo;
    }
}
