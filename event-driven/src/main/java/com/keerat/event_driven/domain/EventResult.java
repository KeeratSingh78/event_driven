package com.keerat.event_driven.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/**
 * Persisted outcome of a processed event.
 */
@Entity
@Table(name = "event_results")
public class EventResult {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "event_id", nullable = false, unique = true)
    private UUID eventId;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "result_payload", columnDefinition = "jsonb")
    private JsonNode resultPayload;

    @Column(name = "result_message", length = 1000)
    private String resultMessage;

    @Column(name = "processed_by", length = 255)
    private String processedBy;

    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt;

    public EventResult() {
    }

    public EventResult(UUID id, UUID eventId, JsonNode resultPayload, String resultMessage, String processedBy) {
        this.id = id != null ? id : UUID.randomUUID();
        this.eventId = eventId;
        this.resultPayload = resultPayload;
        this.resultMessage = resultMessage;
        this.processedBy = processedBy;
    }

    @PrePersist
    protected void onCreate() {
        if (id == null) {
            id = UUID.randomUUID();
        }
        if (processedAt == null) {
            processedAt = Instant.now();
        }
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getEventId() {
        return eventId;
    }

    public void setEventId(UUID eventId) {
        this.eventId = eventId;
    }

    public JsonNode getResultPayload() {
        return resultPayload;
    }

    public void setResultPayload(JsonNode resultPayload) {
        this.resultPayload = resultPayload;
    }

    public String getResultMessage() {
        return resultMessage;
    }

    public void setResultMessage(String resultMessage) {
        this.resultMessage = resultMessage;
    }

    public String getProcessedBy() {
        return processedBy;
    }

    public void setProcessedBy(String processedBy) {
        this.processedBy = processedBy;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(Instant processedAt) {
        this.processedAt = processedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventResult other)) return false;
        return id != null && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
