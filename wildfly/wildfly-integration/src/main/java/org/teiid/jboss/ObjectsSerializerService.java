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

import java.util.function.Consumer;
import java.util.function.Supplier;

class ObjectsSerializerService implements Service<ObjectSerializer> {
    private Supplier<String> pathInjector;
    private ObjectSerializer serializer;
    private Consumer<ObjectSerializer> serializerConsumer;

    public ObjectsSerializerService(Supplier<String> pathDep, Consumer<ObjectSerializer> serializerCons){
        this.pathInjector = pathDep;
        this.serializerConsumer = serializerCons;
    }

    @Override
    public void start(StartContext context) throws StartException {
        this.serializer = new ObjectSerializer(pathInjector.get());
        this.serializerConsumer.accept(serializer);
    }

    @Override
    public void stop(StopContext context) {
    }

    /**
     * Get the actual dependency value.
     *
     * @return the actual dependency value
     * @throws IllegalStateException    if the value is time-sensitive and the current state does not allow retrieval.
     * @throws IllegalArgumentException when the value cannot be read due to misconfiguration
     */
    @Override
    public ObjectSerializer getValue() throws IllegalStateException, IllegalArgumentException {
        return serializer;
    }
}
