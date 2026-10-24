package com.keerat.event_driven.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcType;
import org.hibernate.dialect.type.PostgreSQLEnumJdbcType;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable audit trail record for state transitions and operations on an event.
 */
@Entity
@Table(name = "event_audit_logs")
public class EventAuditLog {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Enumerated(EnumType.STRING)
    @JdbcType(PostgreSQLEnumJdbcType.class)
    @Column(name = "status", nullable = false, columnDefinition = "event_status")
    private EventStatus status;

    @Column(name = "attempt_number")
    private Short attemptNumber;

    @Column(name = "message")
    private String message;

    @Column(name = "worker_id", length = 255)
    private String workerId;

    @Column(name = "occurred_at", nullable = false, updatable = false)
    private Instant occurredAt;

    public EventAuditLog() {
    }

    public EventAuditLog(UUID id, UUID eventId, EventStatus status, Short attemptNumber, String message, String workerId) {
        this.id = id != null ? id : UUID.randomUUID();
        this.eventId = eventId;
        this.status = status;
        this.attemptNumber = attemptNumber;
        this.message = message;
        this.workerId = workerId;
    }

    @PrePersist
    protected void onCreate() {
        if (id == null) {
            id = UUID.randomUUID();
        }
        if (occurredAt == null) {
            occurredAt = Instant.now();
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

    public EventStatus getStatus() {
        return status;
    }

    public void setStatus(EventStatus status) {
        this.status = status;
    }

    public Short getAttemptNumber() {
        return attemptNumber;
    }

    public void setAttemptNumber(Short attemptNumber) {
        this.attemptNumber = attemptNumber;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventAuditLog other)) return false;
        return id != null && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
