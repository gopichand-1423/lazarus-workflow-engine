package dev.lazarus.engine;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Registry of ordered task steps for each workflow type. */
public final class WorkflowDefinitionRegistry {

    public record WorkflowDefinition(String workflowType, List<String> stepKeys) {
        public WorkflowDefinition {
            if (workflowType == null || workflowType.isBlank()) {
                throw new IllegalArgumentException("Workflow type must not be blank");
            }
            if (stepKeys == null || stepKeys.isEmpty()) {
                throw new IllegalArgumentException("A workflow definition must contain at least one step");
            }
            stepKeys = List.copyOf(stepKeys);
            if (stepKeys.stream().anyMatch(key -> key == null || key.isBlank())) {
                throw new IllegalArgumentException("Workflow step keys must not be blank");
            }
            if (new HashSet<>(stepKeys).size() != stepKeys.size()) {
                throw new IllegalArgumentException("Workflow step keys must be unique");
            }
        }

        public Optional<String> nextStep(String currentStepKey) {
            int index = stepKeys.indexOf(currentStepKey);
            if (index < 0) {
                return Optional.empty();
            }
            return index + 1 < stepKeys.size() ? Optional.of(stepKeys.get(index + 1)) : Optional.empty();
        }

        public boolean containsStep(String stepKey) {
            return stepKeys.contains(stepKey);
        }
    }

    private final Map<String, WorkflowDefinition> definitions = new ConcurrentHashMap<>();

    public WorkflowDefinitionRegistry() {}

    public WorkflowDefinitionRegistry(List<WorkflowDefinition> initialDefinitions) {
        initialDefinitions.forEach(this::register);
    }

    public void register(WorkflowDefinition definition) {
        WorkflowDefinition previous = definitions.putIfAbsent(definition.workflowType(), definition);
        if (previous != null) {
            throw new IllegalArgumentException("Workflow type is already registered: " + definition.workflowType());
        }
    }

    public Optional<WorkflowDefinition> find(String workflowType) {
        return Optional.ofNullable(definitions.get(workflowType));
    }
}
