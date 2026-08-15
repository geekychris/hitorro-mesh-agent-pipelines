/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.agent.pipelines;

import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot autoconfig for the agent-side pipeline executor. Activates
 * when this jar is on the classpath, jnats is available, and the
 * property {@code hitorro.mesh.pipelines.enabled=true}.
 *
 * <p>Wires a {@link SinkRegistry} rooted at {@code hitorro.pipelines.home}
 * and a {@link PipelineAgent} bound to the configured agentId. The
 * agent starts subscribing immediately after bean creation and cleanly
 * unsubscribes at container shutdown via {@link PipelineAgent#close}.</p>
 */
@AutoConfiguration
@ConditionalOnClass(name = "io.nats.client.Nats")
@ConditionalOnProperty(name = "hitorro.mesh.pipelines.enabled", havingValue = "true", matchIfMissing = false)
public class PipelineAgentAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConfigurationProperties("hitorro.mesh.pipelines")
    public PipelineAgentProperties pipelineAgentProperties() {
        return new PipelineAgentProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public SinkRegistry pipelinesSinkRegistry(PipelineAgentProperties props) {
        return props.getHome() == null
                ? SinkRegistry.withDefaultHome()
                : new SinkRegistry(java.nio.file.Path.of(props.getHome()));
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public PipelineAgent pipelineAgent(PipelineAgentProperties props, SinkRegistry reg) throws Exception {
        return new PipelineAgent(props.getAgentId(),
                props.getNatsUrl() == null ? "nats://localhost:4222" : props.getNatsUrl(),
                reg);
    }

    /** Bound to {@code hitorro.mesh.pipelines.*}. */
    @Configuration
    public static class PipelineAgentProperties {
        private String agentId;
        private String natsUrl;
        private String home;

        public String getAgentId() { return agentId; }
        public void   setAgentId(String v) { this.agentId = v; }
        public String getNatsUrl() { return natsUrl; }
        public void   setNatsUrl(String v) { this.natsUrl = v; }
        public String getHome()    { return home; }
        public void   setHome(String v) { this.home = v; }
    }
}
