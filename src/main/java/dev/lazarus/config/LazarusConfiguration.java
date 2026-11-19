package dev.lazarus.config;

import dev.lazarus.engine.LazarusEngine;
import dev.lazarus.engine.WorkflowDefinitionRegistry;
import dev.lazarus.engine.WorkflowDefinitionRegistry.WorkflowDefinition;
import dev.lazarus.engine.ZombieWorkerReaper;
import dev.lazarus.grpc.LazarusGrpcServerLifecycle;
import dev.lazarus.grpc.WorkflowOrchestratorServiceImpl;
import dev.lazarus.store.EventStoreRepository;
import dev.lazarus.store.TaskQueueRepository;
import dev.lazarus.workflow.InMemoryInventoryService;
import dev.lazarus.workflow.InMemoryPaymentGateway;
import dev.lazarus.workflow.InMemoryShippingService;
import dev.lazarus.workflow.PaymentSagaWorkflow;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Clock;
import java.util.List;

@Configuration
public class LazarusConfiguration {

    @Bean
    @ConditionalOnMissingBean(Clock.class)
    Clock lazarusClock() {
        return Clock.systemUTC();
    }

    @Bean
    LazarusDatabaseMode lazarusDatabaseMode(
            DataSource dataSource,
            @Value("${lazarus.database.mode:postgresql}") String databaseMode
    ) throws SQLException {
        return LazarusDatabaseMode.validate(dataSource, databaseMode);
    }

    @Bean
    EventStoreRepository eventStoreRepository(DataSource dataSource, Clock lazarusClock,
                                              LazarusDatabaseMode ignoredDatabaseMode) {
        return new EventStoreRepository(dataSource, lazarusClock);
    }

    @Bean
    TaskQueueRepository taskQueueRepository(DataSource dataSource, Clock lazarusClock,
                                            LazarusDatabaseMode ignoredDatabaseMode) {
        return new TaskQueueRepository(dataSource, lazarusClock);
    }

    @Bean(destroyMethod = "close")
    LazarusEngine lazarusEngine(EventStoreRepository eventStoreRepository) {
        return new LazarusEngine(eventStoreRepository);
    }

    @Bean
    InMemoryPaymentGateway paymentGateway() {
        return new InMemoryPaymentGateway();
    }

    @Bean
    InMemoryInventoryService inventoryService() {
        return new InMemoryInventoryService();
    }

    @Bean
    InMemoryShippingService shippingService() {
        return new InMemoryShippingService();
    }

    @Bean
    PaymentSagaWorkflow paymentSagaWorkflow(InMemoryPaymentGateway paymentGateway,
                                           InMemoryInventoryService inventoryService,
                                           InMemoryShippingService shippingService) {
        return new PaymentSagaWorkflow(paymentGateway, inventoryService, shippingService);
    }

    @Bean
    WorkflowDefinitionRegistry workflowDefinitionRegistry() {
        return new WorkflowDefinitionRegistry(List.of(
                new WorkflowDefinition("lazarus.example", List.of("step-1-init")),
                PaymentSagaWorkflow.workflowDefinition()));
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
            Clock lazarusClock,
            @Value("${lazarus.reaper.interval-ms:5000}") long intervalMillis,
            @Value("${lazarus.reaper.batch-size:100}") int batchSize
    ) {
        return new ZombieWorkerReaper(taskQueueRepository, Duration.ofMillis(intervalMillis), batchSize, lazarusClock);
    }
}
