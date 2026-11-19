package dev.lazarus.config;

import dev.lazarus.engine.LazarusEngine;
import dev.lazarus.engine.WorkflowDefinitionRegistry;
import dev.lazarus.engine.WorkflowDefinitionRegistry.WorkflowDefinition;
import dev.lazarus.engine.ZombieWorkerReaper;
import dev.lazarus.grpc.LazarusGrpcServerLifecycle;
import dev.lazarus.grpc.WorkflowOrchestratorServiceImpl;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.TaskQueueRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;

@Configuration
public class LazarusConfiguration {

    @Bean
    EventStoreRepository eventStoreRepository(DataSource dataSource) {
        return new EventStoreRepository(dataSource);
    }

    @Bean
    TaskQueueRepository taskQueueRepository(DataSource dataSource) {
        return new TaskQueueRepository(dataSource);
    }

    @Bean(destroyMethod = "close")
    LazarusEngine lazarusEngine(EventStoreRepository eventStoreRepository) {
        return new LazarusEngine(eventStoreRepository);
    }

    @Bean
    WorkflowDefinitionRegistry workflowDefinitionRegistry() {
        return new WorkflowDefinitionRegistry(List.of(
                new WorkflowDefinition("lazarus.example", List.of("step-1-init"))));
    }

    @Bean
    WorkflowOrchestratorServiceImpl workflowOrchestratorService(
            EventStoreRepository eventStoreRepository,
            TaskQueueRepository taskQueueRepository,
            WorkflowDefinitionRegistry workflowDefinitionRegistry
    ) {
        return new WorkflowOrchestratorServiceImpl(
                eventStoreRepository, taskQueueRepository, workflowDefinitionRegistry);
    }

    @Bean
    LazarusGrpcServerLifecycle lazarusGrpcServerLifecycle(
            WorkflowOrchestratorServiceImpl workflowOrchestratorService,
            @Value("${lazarus.grpc.port:9090}") int port
    ) {
        return new LazarusGrpcServerLifecycle(workflowOrchestratorService, port);
    }

    @Bean
    @ConditionalOnProperty(prefix = "lazarus.reaper", name = "enabled", havingValue = "true", matchIfMissing = true)
    ZombieWorkerReaper zombieWorkerReaper(
            TaskQueueRepository taskQueueRepository,
            @Value("${lazarus.reaper.interval-ms:5000}") long intervalMillis,
            @Value("${lazarus.reaper.batch-size:100}") int batchSize
    ) {
        return new ZombieWorkerReaper(taskQueueRepository, Duration.ofMillis(intervalMillis), batchSize);
    }
}
