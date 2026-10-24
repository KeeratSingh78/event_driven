package com.keerat.event_driven.repository;

import com.keerat.event_driven.domain.EventAuditLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface EventAuditLogRepository extends JpaRepository<EventAuditLog, UUID> {

    List<EventAuditLog> findByEventIdOrderByOccurredAtAsc(UUID eventId);
}
