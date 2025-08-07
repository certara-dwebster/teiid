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

package org.teiid.jdbc.tracing;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


@SuppressWarnings("nls")
public class TestTracing {

    @Test
    public void testSpanContextInjection() {

        GlobalOpenTelemetry.resetForTest();
        
        Map<String, String> configProperties = new HashMap<>();
        configProperties.put("otel.traces.exporter", "none");
        configProperties.put("otel.metrics.exporter", "none");
        configProperties.put("otel.logs.exporter", "none");
        
        AutoConfiguredOpenTelemetrySdk autoConfiguredSdk = AutoConfiguredOpenTelemetrySdk.builder()
            .addPropertiesSupplier(() -> configProperties)
            .setResultAsGlobal()
            .build();
        
        Tracer tracer = GlobalOpenTelemetry.getTracer("test");
        assertNull(GlobalTracerInjector.getSpanContext(tracer));
        Span span = tracer.spanBuilder("x").startSpan();
        try (Scope scope = span.makeCurrent()) {
            assertEquals("{\"spanid\":\"" + span.getSpanContext().getSpanId() + "\",\"traceid\":\"" + span.getSpanContext().getTraceId() + "\"}", GlobalTracerInjector.getSpanContext(tracer));
        } finally {
            span.end();
            autoConfiguredSdk.getOpenTelemetrySdk().close();
        }
    }

}
