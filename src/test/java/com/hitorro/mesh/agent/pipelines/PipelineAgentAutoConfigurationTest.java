/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.pipelines;

import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves {@link PipelineAgentAutoConfiguration} is found through
 * {@code META-INF/spring/...AutoConfiguration.imports} under Spring Boot 4 and
 * stays off unless {@code hitorro.mesh.pipelines.enabled=true} (enabling it
 * connects to NATS, which this test does not need).
 */
class PipelineAgentAutoConfigurationTest {

    private static Class<?>[] fromImportsFile() throws ClassNotFoundException {
        List<Class<?>> classes = new ArrayList<>();
        for (String name : ImportCandidates.load(AutoConfiguration.class,
                PipelineAgentAutoConfigurationTest.class.getClassLoader())) {
            if (name.startsWith("com.hitorro.mesh.agent.pipelines.")) classes.add(Class.forName(name));
        }
        return classes.toArray(Class<?>[]::new);
    }

    @Test
    void runsOnSpringBoot4() {
        assertThat(SpringBootVersion.getVersion()).startsWith("4.");
    }

    @Test
    void importsFileListsTheAutoConfiguration() throws Exception {
        assertThat(fromImportsFile()).contains(PipelineAgentAutoConfiguration.class);
    }

    @Test
    void offByDefault() throws Exception {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(fromImportsFile()))
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx).doesNotHaveBean(PipelineAgent.class);
                    assertThat(ctx).doesNotHaveBean(SinkRegistry.class);
                });
    }
}
