package com.keerat.event_driven.repository;

import com.keerat.event_driven.domain.EventResult;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface EventResultRepository extends JpaRepository<EventResult, UUID> {

    Optional<EventResult> findByEventId(UUID eventId);
}
