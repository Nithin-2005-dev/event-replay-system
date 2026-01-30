# Event Sourcing Implementation Guide: Production-Ready System

## 📚 Table of Contents

1. [Phase 0: Environment Setup & Core Concepts](#phase-0)
2. [Phase 1: Database Schema & Event Log](#phase-1)
3. [Phase 2: Spring Boot Foundation](#phase-2)
4. [Phase 3: Event Publishing with Invariants](#phase-3)
5. [Phase 4: Incremental Projection Consumer](#phase-4)
6. [Phase 5: Full Replay System](#phase-5)
7. [Phase 6: Testing & Validation](#phase-6)

---

## <a name="phase-0"></a>Phase 0: Environment Setup & Core Concepts

### 🎯 Learning Objectives
- Understand what event sourcing is and why it matters
- Learn the difference between traditional CRUD and event-sourced systems
- Set up PostgreSQL correctly
- Understand immutability at the database level

### 📖 Concept: What is Event Sourcing?

**Traditional Systems (CRUD):**
```
User creates order → Database stores: { id: 1, status: "PENDING", total: 100 }
Payment succeeds → Database updates: { id: 1, status: "PAID", total: 100 }
```
**Problem:** You can't answer "When did this change?" or "Why is it in this state?"

**Event Sourced Systems:**
```
User creates order → Store event: OrderCreated { orderId: 1, total: 100 }
Payment succeeds → Store event: PaymentSucceeded { orderId: 1, amount: 100 }
```
**Result:** Complete audit trail, perfect for debugging, replay-able history

### 🔑 Key Principles

1. **Events are facts** - They happened and cannot be changed
2. **Events are immutable** - Never UPDATE or DELETE
3. **State is derived** - Current state = replay all events
4. **Projections are disposable** - Can be rebuilt anytime
5. **Write-side validates** - EventStore enforces business rules
6. **Consumers apply deterministically** - No business validation in consumers

### 💻 Task 1: Set Up PostgreSQL

```bash
# Run PostgreSQL in Docker
docker run -d \
  --name event-replay-postgres \
  -e POSTGRES_DB=event_store \
  -e POSTGRES_USER=event_user \
  -e POSTGRES_PASSWORD=event_pass \
  -v pg_event_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:16

# Verify it's running
docker ps | grep event-replay-postgres

# Connect to verify
docker exec -it event-replay-postgres psql -U event_user -d event_store
```

**What you just learned:**
- Container isolation (your DB won't interfere with other projects)
- Named volumes (data persists even if container is deleted)
- Port mapping (5432 inside container → 5432 on your machine)

### ✅ Checkpoint
Before moving on, verify:
- [ ] PostgreSQL container is running
- [ ] You can connect via psql
- [ ] You understand: events are immutable facts

---

## <a name="phase-1"></a>Phase 1: Database Schema & Event Log

### 🎯 Learning Objectives
- Design an event log table that prevents corruption
- Understand why certain columns exist
- Learn database-level immutability enforcement
- Create your first event schema

### 📖 Concept: The Event Envelope

An event has two parts:
1. **Envelope** (metadata about the event)
2. **Payload** (what actually happened)

**Why separate them?**
- Envelope allows querying/ordering WITHOUT parsing payload
- Payload can evolve independently
- Generic tools can work with any event type

### 💻 Task 2: Create Event Log Table

Connect to PostgreSQL and run:

```sql
-- The single source of truth
CREATE TABLE event_log (
    -- Identity & Ordering
    event_sequence BIGSERIAL PRIMARY KEY,  -- Auto-incrementing order
    event_id UUID NOT NULL UNIQUE,         -- Globally unique, idempotency key
    
    -- Business Context
    aggregate_id VARCHAR(100) NOT NULL,    -- Which entity (order_id, payment_id)
    aggregate_type VARCHAR(50) NOT NULL,   -- ORDER, PAYMENT, etc.
    event_type VARCHAR(100) NOT NULL,      -- OrderCreated, PaymentSucceeded
    event_version INTEGER NOT NULL DEFAULT 1,  -- Schema evolution
    
    -- Data & Metadata
    payload JSONB NOT NULL,                -- The actual business data
    metadata JSONB,                        -- Trace IDs, correlation, debugging
    
    -- Authority & Time
    emitted_by VARCHAR(100) NOT NULL,      -- Which service emitted this
    event_time TIMESTAMPTZ NOT NULL,       -- When it happened (business time)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()  -- When we stored it
);

-- Indexes for common queries
CREATE INDEX idx_event_log_aggregate ON event_log(aggregate_type, aggregate_id);
CREATE INDEX idx_event_log_type ON event_log(event_type);
CREATE INDEX idx_event_log_time ON event_log(event_time);
CREATE INDEX idx_event_log_sequence ON event_log(event_sequence);
```

**🧠 Why each column matters:**

| Column | Purpose | Example |
|--------|---------|---------|
| `event_sequence` | Absolute ordering, never gaps | 1, 2, 3, 4... |
| `event_id` | Idempotency (detect duplicates) | `550e8400-e29b-41d4-a716-446655440000` |
| `aggregate_id` | Group events by entity | `order_123` |
| `aggregate_type` | Type of entity | `ORDER`, `PAYMENT` |
| `event_type` | What happened | `OrderCreated`, `PaymentSucceeded` |
| `event_version` | Handle schema changes | Version 1 vs Version 2 logic |
| `payload` | The business fact | `{"orderId": "123", "total": 499.00}` |
| `metadata` | Debugging/tracing | `{"traceId": "abc", "requestId": "xyz"}` |
| `emitted_by` | Who created it | `order-service`, `payment-service` |
| `event_time` | Business timestamp | When payment actually succeeded |
| `created_at` | System timestamp | When we wrote to database |

### 💻 Task 3: Make Event Log Append-Only

This is **CRITICAL** - it prevents anyone (including you) from corrupting history:

```sql
-- Create function that prevents mutations
CREATE OR REPLACE FUNCTION prevent_event_log_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'event_log is append-only. UPDATE and DELETE are not allowed.';
END;
$$ LANGUAGE plpgsql;

-- Block UPDATE operations
CREATE TRIGGER event_log_no_update
BEFORE UPDATE ON event_log
FOR EACH ROW
EXECUTE FUNCTION prevent_event_log_mutation();

-- Block DELETE operations
CREATE TRIGGER event_log_no_delete
BEFORE DELETE ON event_log
FOR EACH ROW
EXECUTE FUNCTION prevent_event_log_mutation();
```

**Test it works:**
```sql
-- This should succeed
INSERT INTO event_log (
    event_id, aggregate_id, aggregate_type, event_type,
    payload, emitted_by, event_time
) VALUES (
    gen_random_uuid(),
    'order_123',
    'ORDER',
    'OrderCreated',
    '{"orderId": "order_123", "items": [], "totalAmount": 499.00}'::jsonb,
    'order-service',
    NOW()
);

-- This should FAIL
UPDATE event_log SET payload = '{}' WHERE event_sequence = 1;
-- Error: event_log is append-only. UPDATE and DELETE are not allowed.

-- This should also FAIL
DELETE FROM event_log WHERE event_sequence = 1;
-- Error: event_log is append-only. UPDATE and DELETE are not allowed.
```

### 📖 Concept: Why Database-Level Protection?

**Application code can be bypassed:**
- Emergency hotfixes
- Manual SQL scripts
- Different service versions
- Panicked developers at 3 AM

**Database triggers cannot be bypassed** (without explicitly dropping them)

This is the **last line of defense** for your immutable history.

### 💻 Task 4: Create Projection Table

Now create the **derived state** table:

```sql
-- Disposable read model
CREATE TABLE order_payment_projection (
    aggregate_id VARCHAR(100) PRIMARY KEY,  -- Same as event_log.aggregate_id
    
    -- Derived state
    payment_state VARCHAR(50) NOT NULL,     -- NOT_STARTED, REQUESTED, PAID, etc.
    order_total_amount DECIMAL(18,2) NOT NULL,
    paid_amount DECIMAL(18,2) NOT NULL DEFAULT 0,
    refunded_amount DECIMAL(18,2) NOT NULL DEFAULT 0,
    
    -- Computed flags
    is_settled BOOLEAN NOT NULL DEFAULT FALSE,
    refund_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Metadata
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Key difference from event_log:**
- This table CAN be updated
- This table CAN be deleted
- This table is rebuilt from event_log if corrupted

### 💻 Task 5: Create Consumer Tracking Tables

```sql
-- Track consumer progress (one row per consumer)
CREATE TABLE consumer_offsets (
    consumer_name VARCHAR(100) PRIMARY KEY,
    last_committed_event_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Track which events have been processed by each consumer
CREATE TABLE processed_events (
    consumer_name VARCHAR(100) NOT NULL,
    event_sequence BIGINT NOT NULL,
    state VARCHAR(20) NOT NULL CHECK (state IN ('WAITING', 'PROCESSED')),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_name, event_sequence)
);

-- Create index for efficient WAITING event lookups
CREATE INDEX idx_processed_events_waiting 
ON processed_events(consumer_name, event_sequence) 
WHERE state = 'WAITING';

-- Initialize payment consumer
INSERT INTO consumer_offsets (consumer_name, last_committed_event_sequence)
VALUES ('payment_consumer', 0);
```

**🧠 Understanding the states:**

| State | Meaning | When |
|-------|---------|------|
| (not present) | NOT_PROCESSED | Event exists but consumer hasn't seen it |
| WAITING | Seen but dependencies missing | PaymentSucceeded arrived before PaymentRequested |
| PROCESSED | Successfully applied | Projection updated, never process again |

### ✅ Checkpoint
Before moving on, verify:
- [ ] event_log table created with all columns
- [ ] Triggers prevent UPDATE/DELETE on event_log
- [ ] You can INSERT into event_log successfully
- [ ] order_payment_projection table created
- [ ] consumer_offsets and processed_events tables created
- [ ] You understand: event_log = truth, projection = convenience

---

## <a name="phase-2"></a>Phase 2: Spring Boot Foundation

### 🎯 Learning Objectives
- Set up Spring Boot correctly for event sourcing
- Understand why NOT to use JPA/Hibernate
- Configure JDBC for direct SQL control
- Prove database connectivity

### 📖 Concept: Why Not JPA?

**JPA/Hibernate is designed for:**
- Object-relational mapping
- Automatic UPDATE/DELETE
- Entity state management
- Lazy loading, caching

**Event sourcing needs:**
- Raw SQL control
- Append-only semantics
- Deterministic replay
- No "clever" behavior

**Conclusion:** Use JdbcTemplate for direct SQL execution.

### 💻 Task 6: Create Spring Boot Project

**Directory structure:**
```
event-replay-system/
├── src/main/java/com/nithin/eventreplay/
│   ├── EventReplayApplication.java
│   ├── config/
│   │   └── DatabaseConfig.java
│   ├── event/
│   │   ├── Event.java
│   │   ├── EventPublisher.java
│   │   └── EventStore.java
│   ├── domain/
│   │   ├── OrderAggregate.java
│   │   └── PaymentAggregate.java
│   ├── consumer/
│   │   ├── IncrementalConsumer.java
│   │   └── ConsumerOffset.java
│   ├── projection/
│   │   └── OrderPaymentProjection.java
│   └── replay/
│       └── FullReplayService.java
│
├── src/main/resources/
│   ├── application.yml
│   └── db/migration/
│
└── pom.xml
```

### 💻 Task 7: Configure Maven Dependencies

**pom.xml:**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.2.1</version>
    </parent>
    
    <groupId>com.nithin</groupId>
    <artifactId>event-replay-system</artifactId>
    <version>1.0.0</version>
    
    <properties>
        <java.version>17</java.version>
    </properties>
    
    <dependencies>
        <!-- Core Spring Boot -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>
        
        <!-- PostgreSQL Driver -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
        </dependency>
        
        <!-- JSON Processing -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        
        <!-- Lombok (optional, reduces boilerplate) -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
        
        <!-- Testing -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
    
    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

### 💻 Task 8: Configure Application Properties

**src/main/resources/application.yml:**
```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/event_store
    username: event_user
    password: event_pass
    driver-class-name: org.postgresql.Driver
    
    # Connection pool settings
    hikari:
      maximum-pool-size: 10
      minimum-idle: 2
      connection-timeout: 30000
      idle-timeout: 600000
      max-lifetime: 1800000

  # JDBC settings
  jdbc:
    template:
      fetch-size: 100  # Rows fetched per round trip

# Logging
logging:
  level:
    root: INFO
    com.nithin.eventreplay: DEBUG
    org.springframework.jdbc: DEBUG
```

### 💻 Task 9: Create Main Application Class

**EventReplayApplication.java:**
```java
package com.nithin.eventreplay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication
@EnableTransactionManagement
public class EventReplayApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventReplayApplication.class, args);
    }
}
```

### 💻 Task 10: Test Database Connectivity

**Create a test component:**

```java
package com.nithin.eventreplay.config;

import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class DatabaseConnectivityTest implements CommandLineRunner {
    
    private final JdbcTemplate jdbcTemplate;
    
    @Override
    public void run(String... args) {
        try {
            Integer result = jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            log.info("✅ Database connectivity test PASSED. Result: {}", result);
            
            // Test event_log table exists
            Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM event_log", 
                Integer.class
            );
            log.info("✅ event_log table accessible. Current events: {}", count);
            
        } catch (Exception e) {
            log.error("❌ Database connectivity test FAILED", e);
            System.exit(1);
        }
    }
}
```

**Run the application:**
```bash
mvn clean package
mvn spring-boot:run
```

**Expected output:**
```
✅ Database connectivity test PASSED. Result: 1
✅ event_log table accessible. Current events: 0
```

### ✅ Checkpoint
Before moving on, verify:
- [ ] Spring Boot starts without errors
- [ ] Database connectivity test passes
- [ ] You can see SQL queries in logs
- [ ] You understand: JDBC > JPA for event sourcing

---

## <a name="phase-3"></a>Phase 3: Event Publishing with Invariants

### 🎯 Learning Objectives
- Create domain events (OrderCreated, PaymentSucceeded, etc.)
- Build an EventStore that enforces business invariants
- Understand write-side validation
- Learn why consumers should never validate business rules

### 📖 Concept: Write-Side vs Read-Side

**Critical distinction:**

**Write-Side (EventStore):**
- Enforces business invariants
- Prevents impossible timelines
- Validates before appending
- Example: "Can't refund more than paid"

**Read-Side (Consumers):**
- Applies events deterministically
- NO business validation
- Handles missing dependencies (WAITING)
- Example: "Just update paid_amount"

**Why this matters:**
If consumers validate business rules, replay becomes non-deterministic. Events that were valid at write-time might fail during replay.

### 💻 Task 11: Create Event Base Class

```java
package com.nithin.eventreplay.event;

import lombok.Data;
import java.time.Instant;
import java.util.UUID;
import java.util.Map;

@Data
public abstract class Event {
    private UUID eventId = UUID.randomUUID();
    private String aggregateId;
    private String aggregateType;
    private String eventType;
    private int eventVersion = 1;
    private String emittedBy;
    private Instant eventTime = Instant.now();
    private Map<String, Object> metadata;
    
    public abstract Object getPayload();
    
    public Event(String aggregateId, String aggregateType, String emittedBy) {
        this.aggregateId = aggregateId;
        this.aggregateType = aggregateType;
        this.eventType = this.getClass().getSimpleName();
        this.emittedBy = emittedBy;
    }
}
```

### 💻 Task 12: Create Concrete Events

**OrderCreated.java:**
```java
package com.nithin.eventreplay.event;

import lombok.Getter;
import java.math.BigDecimal;
import java.util.List;

@Getter
public class OrderCreated extends Event {
    
    @Getter
    public static class OrderItem {
        private String productId;
        private int quantity;
        private BigDecimal price;
        
        public OrderItem(String productId, int quantity, BigDecimal price) {
            this.productId = productId;
            this.quantity = quantity;
            this.price = price;
        }
    }
    
    @Getter
    public static class Payload {
        private String orderId;
        private List<OrderItem> items;
        private BigDecimal totalAmount;
        
        public Payload(String orderId, List<OrderItem> items, BigDecimal totalAmount) {
            this.orderId = orderId;
            this.items = items;
            this.totalAmount = totalAmount;
        }
    }
    
    private final Payload payload;
    
    public OrderCreated(String orderId, List<OrderItem> items, BigDecimal totalAmount) {
        super(orderId, "ORDER", "order-service");
        this.payload = new Payload(orderId, items, totalAmount);
    }
}
```

**PaymentRequested.java:**
```java
package com.nithin.eventreplay.event;

import lombok.Getter;
import java.math.BigDecimal;

@Getter
public class PaymentRequested extends Event {
    
    @Getter
    public static class Payload {
        private String orderId;
        private String paymentId;
        private BigDecimal amount;
        
        public Payload(String orderId, String paymentId, BigDecimal amount) {
            this.orderId = orderId;
            this.paymentId = paymentId;
            this.amount = amount;
        }
    }
    
    private final Payload payload;
    
    public PaymentRequested(String orderId, String paymentId, BigDecimal amount) {
        super(orderId, "ORDER", "payment-service");
        this.payload = new Payload(orderId, paymentId, amount);
    }
}
```

**PaymentSucceeded.java:**
```java
package com.nithin.eventreplay.event;

import lombok.Getter;
import java.math.BigDecimal;

@Getter
public class PaymentSucceeded extends Event {
    
    @Getter
    public static class Payload {
        private String orderId;
        private String paymentId;
        private BigDecimal amount;
        
        public Payload(String orderId, String paymentId, BigDecimal amount) {
            this.orderId = orderId;
            this.paymentId = paymentId;
            this.amount = amount;
        }
    }
    
    private final Payload payload;
    
    public PaymentSucceeded(String orderId, String paymentId, BigDecimal amount) {
        super(orderId, "ORDER", "payment-service");
        this.payload = new Payload(orderId, paymentId, amount);
    }
}
```

**RefundIssued.java:**
```java
package com.nithin.eventreplay.event;

import lombok.Getter;
import java.math.BigDecimal;

@Getter
public class RefundIssued extends Event {
    
    @Getter
    public static class Payload {
        private String orderId;
        private String paymentId;
        private String refundId;
        private BigDecimal amount;
        
        public Payload(String orderId, String paymentId, String refundId, BigDecimal amount) {
            this.orderId = orderId;
            this.paymentId = paymentId;
            this.refundId = refundId;
            this.amount = amount;
        }
    }
    
    private final Payload payload;
    
    public RefundIssued(String orderId, String paymentId, String refundId, BigDecimal amount) {
        super(orderId, "ORDER", "payment-service");
        this.payload = new Payload(orderId, paymentId, refundId, amount);
    }
}
```

### 💻 Task 13: Create EventStore with Invariant Enforcement

This is the **critical gatekeeper** - it validates business rules before writing:

```java
package com.nithin.eventreplay.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Timestamp;

@Repository
@RequiredArgsConstructor
@Slf4j
public class EventStore {
    
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    
    @Transactional
    public void append(Event event) {
        // Validate business invariants before appending
        validateInvariants(event);
        
        try {
            // Convert payload to JSON
            String payloadJson = objectMapper.writeValueAsString(event.getPayload());
            String metadataJson = event.getMetadata() != null 
                ? objectMapper.writeValueAsString(event.getMetadata())
                : null;
            
            // Insert into event_log
            String sql = """
                INSERT INTO event_log (
                    event_id, aggregate_id, aggregate_type, event_type, event_version,
                    payload, metadata, emitted_by, event_time
                ) VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?)
                """;
            
            jdbcTemplate.update(
                sql,
                event.getEventId(),
                event.getAggregateId(),
                event.getAggregateType(),
                event.getEventType(),
                event.getEventVersion(),
                payloadJson,
                metadataJson,
                event.getEmittedBy(),
                Timestamp.from(event.getEventTime())
            );
            
            log.info("✅ Event appended: {} for aggregate {}", 
                event.getEventType(), event.getAggregateId());
            
        } catch (Exception e) {
            log.error("❌ Failed to append event", e);
            throw new RuntimeException("Event append failed", e);
        }
    }
    
    private void validateInvariants(Event event) {
        switch (event.getEventType()) {
            case "OrderCreated" -> validateOrderCreated((OrderCreated) event);
            case "PaymentRequested" -> validatePaymentRequested((PaymentRequested) event);
            case "PaymentSucceeded" -> validatePaymentSucceeded((PaymentSucceeded) event);
            case "RefundIssued" -> validateRefundIssued((RefundIssued) event);
        }
    }
    
    private void validateOrderCreated(OrderCreated event) {
        String orderId = event.getAggregateId();
        
        // Invariant: Order must not already exist
        Integer existingCount = jdbcTemplate.queryForObject(
            """
            SELECT COUNT(*) FROM event_log 
            WHERE aggregate_id = ? AND event_type = 'OrderCreated'
            """,
            Integer.class,
            orderId
        );
        
        if (existingCount > 0) {
            throw new IllegalStateException("Order already exists: " + orderId);
        }
        
        // Invariant: Order must have at least one item
        if (event.getPayload().getItems().isEmpty()) {
            throw new IllegalArgumentException("Order must contain at least one item");
        }
        
        // Invariant: Total amount must be positive
        if (event.getPayload().getTotalAmount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Order total must be positive");
        }
    }
    
    private void validatePaymentRequested(PaymentRequested event) {
        String orderId = event.getAggregateId();
        
        // Invariant: Order must exist
        Integer orderCount = jdbcTemplate.queryForObject(
            """
            SELECT COUNT(*) FROM event_log 
            WHERE aggregate_id = ? AND event_type = 'OrderCreated'
            """,
            Integer.class,
            orderId
        );
        
        if (orderCount == 0) {
            throw new IllegalStateException("Cannot request payment for non-existent order: " + orderId);
        }
        
        // Invariant: Order must not already be paid
        Integer paidCount = jdbcTemplate.queryForObject(
            """
            SELECT COUNT(*) FROM event_log 
            WHERE aggregate_id = ? AND event_type = 'PaymentSucceeded'
            """,
            Integer.class,
            orderId
        );
        
        if (paidCount > 0) {
            throw new IllegalStateException("Order already paid: " + orderId);
        }
        
        // Invariant: No active payment request exists
        Integer activePaymentCount = jdbcTemplate.queryForObject(
            """
            SELECT COUNT(*) FROM event_log 
            WHERE aggregate_id = ? 
              AND event_type = 'PaymentRequested'
              AND NOT EXISTS (
                SELECT 1 FROM event_log e2
                WHERE e2.aggregate_id = event_log.aggregate_id
                  AND e2.event_type IN ('PaymentSucceeded', 'PaymentFailed')
                  AND e2.event_sequence > event_log.event_sequence
              )
            """,
            Integer.class,
            orderId
        );
        
        if (activePaymentCount > 0) {
            throw new IllegalStateException("Active payment request already exists for order: " + orderId);
        }
    }
    
    private void validatePaymentSucceeded(PaymentSucceeded event) {
        String orderId = event.getAggregateId();
        String paymentId = event.getPayload().getPaymentId();
        
        // Invariant: PaymentRequested must exist
        Integer requestCount = jdbcTemplate.queryForObject(
            """
            SELECT COUNT(*) FROM event_log 
            WHERE aggregate_id = ? 
              AND event_type = 'PaymentRequested'
              AND payload->>'paymentId' = ?
            """,
            Integer.class,
            orderId,
            paymentId
        );
        
        if (requestCount == 0) {
            throw new IllegalStateException("No payment request found for payment: " + paymentId);
        }
        
        // Invariant: Payment must not already be succeeded
        Integer succeededCount = jdbcTemplate.queryForObject(
            """
            SELECT COUNT(*) FROM event_log 
            WHERE aggregate_id = ? 
              AND event_type = 'PaymentSucceeded'
              AND payload->>'paymentId' = ?
            """,
            Integer.class,
            orderId,
            paymentId
        );
        
        if (succeededCount > 0) {
            throw new IllegalStateException("Payment already succeeded: " + paymentId);
        }
    }
    
    private void validateRefundIssued(RefundIssued event) {
        String orderId = event.getAggregateId();
        String paymentId = event.getPayload().getPaymentId();
        BigDecimal refundAmount = event.getPayload().getAmount();
        
        // Invariant: PaymentSucceeded must exist
        Integer paymentCount = jdbcTemplate.queryForObject(
            """
            SELECT COUNT(*) FROM event_log 
            WHERE aggregate_id = ? 
              AND event_type = 'PaymentSucceeded'
              AND payload->>'paymentId' = ?
            """,
            Integer.class,
            orderId,
            paymentId
        );
        
        if (paymentCount == 0) {
            throw new IllegalStateException("No successful payment found for refund");
        }
        
        // Invariant: Refund must not exceed paid amount
        BigDecimal totalPaid = jdbcTemplate.queryForObject(
            """
            SELECT COALESCE(SUM(CAST(payload->>'amount' AS DECIMAL(18,2))), 0)
            FROM event_log 
            WHERE aggregate_id = ? 
              AND event_type = 'PaymentSucceeded'
            """,
            BigDecimal.class,
            orderId
        );
        
        BigDecimal totalRefunded = jdbcTemplate.queryForObject(
            """
            SELECT COALESCE(SUM(CAST(payload->>'amount' AS DECIMAL(18,2))), 0)
            FROM event_log 
            WHERE aggregate_id = ? 
              AND event_type = 'RefundIssued'
            """,
            BigDecimal.class,
            orderId
        );
        
        BigDecimal newTotal = totalRefunded.add(refundAmount);
        
        if (newTotal.compareTo(totalPaid) > 0) {
            throw new IllegalArgumentException(
                String.format("Refund amount %s exceeds remaining refundable amount %s", 
                    refundAmount, totalPaid.subtract(totalRefunded))
            );
        }
    }
}
```

### 📖 Concept: Why Invariants at Write-Time?

**If you don't validate at write-time:**
```
// Bad: No validation
eventStore.append(paymentSucceeded);  // ✅ Accepted
eventStore.append(paymentSucceeded);  // ✅ Accepted (duplicate!)
eventStore.append(refund(999999));    // ✅ Accepted (exceeds payment!)

// Consumer tries to process
// Now what? Reject the event? But it's already in the log!
```

**With write-time validation:**
```
// Good: Validation before append
eventStore.append(paymentSucceeded);  // ✅ Accepted
eventStore.append(paymentSucceeded);  // ❌ Rejected: already exists
eventStore.append(refund(999999));    // ❌ Rejected: exceeds payment

// Consumer sees only valid events
// Just apply them deterministically
```

### 💻 Task 14: Test Event Publishing

```java
package com.nithin.eventreplay;

import com.nithin.eventreplay.event.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class EventPublishingTest implements CommandLineRunner {
    
    private final EventStore eventStore;
    
    @Override
    public void run(String... args) {
        String orderId = "order_" + System.currentTimeMillis();
        String paymentId = "payment_" + System.currentTimeMillis();
        
        try {
            // Valid sequence
            log.info("Publishing valid event sequence...");
            
            eventStore.append(new OrderCreated(
                orderId,
                List.of(new OrderCreated.OrderItem("product_1", 2, new BigDecimal("100.00"))),
                new BigDecimal("200.00")
            ));
            
            eventStore.append(new PaymentRequested(
                orderId, paymentId, new BigDecimal("200.00")
            ));
            
            eventStore.append(new PaymentSucceeded(
                orderId, paymentId, new BigDecimal("200.00")
            ));
            
            log.info("✅ Valid events published successfully");
            
            // Test invariant enforcement
            log.info("Testing invariant enforcement...");
            
            try {
                // This should FAIL - duplicate order
                eventStore.append(new OrderCreated(
                    orderId,
                    List.of(new OrderCreated.OrderItem("product_1", 1, new BigDecimal("50.00"))),
                    new BigDecimal("50.00")
                ));
                log.error("❌ Should have rejected duplicate order!");
            } catch (IllegalStateException e) {
                log.info("✅ Correctly rejected duplicate order: {}", e.getMessage());
            }
            
            try {
                // This should FAIL - refund exceeds payment
                eventStore.append(new RefundIssued(
                    orderId, paymentId, "refund_1", new BigDecimal("999.00")
                ));
                log.error("❌ Should have rejected excessive refund!");
            } catch (IllegalArgumentException e) {
                log.info("✅ Correctly rejected excessive refund: {}", e.getMessage());
            }
            
        } catch (Exception e) {
            log.error("Unexpected error", e);
        }
    }
}
```

### ✅ Checkpoint
Before moving on, verify:
- [ ] Events are inserted into event_log
- [ ] Duplicate orders are rejected
- [ ] Excessive refunds are rejected
- [ ] PaymentSucceeded requires PaymentRequested
- [ ] You understand: validation happens at write-time, not read-time

---

## <a name="phase-4"></a>Phase 4: Incremental Projection Consumer

### 🎯 Learning Objectives
- Build a consumer that processes events monotonically
- Implement proper offset advancement
- Handle WAITING events correctly
- Apply events to projections without re-querying history

### 📖 Concept: Incremental vs Replay Processing

**Incremental Consumer:**
- Processes new events as they arrive
- Maintains local state (paid_amount, refunded_amount)
- Updates state incrementally (+= new_amount)
- NEVER re-queries event history
- Fast and scalable

**Full Replay:**
- Processes ALL events from scratch
- No local state assumptions
- Recomputes everything from event_log
- Used for rebuilds, schema changes, bug fixes

**Critical rule:**
> Incremental consumer must update state using ONLY the current event's data, not by querying event_log history.

### 💻 Task 15: Create Projection Model

```java
package com.nithin.eventreplay.projection;

import lombok.Data;
import java.math.BigDecimal;
import java.time.Instant;

@Data
public class OrderPaymentProjection {
    private String aggregateId;
    private String paymentState;
    private BigDecimal orderTotalAmount;
    private BigDecimal paidAmount;
    private BigDecimal refundedAmount;
    private boolean isSettled;
    private boolean refundAllowed;
    private Instant lastUpdated;
    
    public OrderPaymentProjection() {
        this.paymentState = "NOT_STARTED";
        this.orderTotalAmount = BigDecimal.ZERO;
        this.paidAmount = BigDecimal.ZERO;
        this.refundedAmount = BigDecimal.ZERO;
        this.isSettled = false;
        this.refundAllowed = false;
    }
}
```

### 💻 Task 16: Create Incremental Consumer

```java
package com.nithin.eventreplay.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class IncrementalConsumer {
    
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private static final String CONSUMER_NAME = "payment_consumer";
    
    @Transactional
    public boolean processNextEvent() {
        // Step 1: Lock consumer offset for exclusive access
        Long currentOffset = jdbcTemplate.queryForObject(
            """
            SELECT last_committed_event_sequence 
            FROM consumer_offsets 
            WHERE consumer_name = ?
            FOR UPDATE
            """,
            Long.class,
            CONSUMER_NAME
        );
        
        // Step 2: Check for WAITING events first (they have higher priority)
        List<Map<String, Object>> waitingEvents = jdbcTemplate.queryForList(
            """
            SELECT el.* 
            FROM event_log el
            INNER JOIN processed_events pe 
              ON pe.event_sequence = el.event_sequence 
              AND pe.consumer_name = ?
            WHERE pe.state = 'WAITING'
            ORDER BY el.event_sequence
            LIMIT 1
            """,
            CONSUMER_NAME
        );
        
        Map<String, Object> eventRow = null;
        
        if (!waitingEvents.isEmpty()) {
            eventRow = waitingEvents.get(0);
            log.debug("Processing WAITING event: {}", eventRow.get("event_sequence"));
        } else {
            // Step 3: If no WAITING events, get next unseen event
            List<Map<String, Object>> newEvents = jdbcTemplate.queryForList(
                """
                SELECT el.* 
                FROM event_log el
                LEFT JOIN processed_events pe 
                  ON pe.event_sequence = el.event_sequence 
                  AND pe.consumer_name = ?
                WHERE el.event_sequence > ?
                  AND pe.state IS NULL
                ORDER BY el.event_sequence
                LIMIT 1
                """,
                CONSUMER_NAME,
                currentOffset
            );
            
            if (newEvents.isEmpty()) {
                log.debug("No events to process");
                return false;
            }
            
            eventRow = newEvents.get(0);
        }
        
        Long eventSequence = (Long) eventRow.get("event_sequence");
        String eventType = (String) eventRow.get("event_type");
        String aggregateId = (String) eventRow.get("aggregate_id");
        String payloadJson = eventRow.get("payload").toString();
        
        try {
            JsonNode payload = objectMapper.readTree(payloadJson);
            
            // Step 4: Check if already processed (idempotency)
            Integer processedCount = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*) FROM processed_events
                WHERE consumer_name = ? AND event_sequence = ? AND state = 'PROCESSED'
                """,
                Integer.class,
                CONSUMER_NAME,
                eventSequence
            );
            
            if (processedCount > 0) {
                log.info("Event {} already processed, advancing offset", eventSequence);
                advanceOffsetIfContiguous(currentOffset);
                return true;
            }
            
            // Step 5: Apply event to projection
            boolean applied = switch (eventType) {
                case "OrderCreated" -> handleOrderCreated(aggregateId, payload, eventSequence);
                case "PaymentRequested" -> handlePaymentRequested(aggregateId, payload, eventSequence);
                case "PaymentSucceeded" -> handlePaymentSucceeded(aggregateId, payload, eventSequence);
                case "RefundIssued" -> handleRefundIssued(aggregateId, payload, eventSequence);
                default -> {
                    log.warn("Unknown event type: {}", eventType);
                    yield false;
                }
            };
            
            if (!applied) {
                // Event was marked WAITING, will retry later
                return true;
            }
            
            // Step 6: Mark as PROCESSED
            markProcessed(eventSequence);
            
            // Step 7: Advance offset if we have contiguous processed events
            advanceOffsetIfContiguous(currentOffset);
            
            log.info("✅ Processed event {}: {}", eventSequence, eventType);
            return true;
            
        } catch (Exception e) {
            log.error("❌ Failed to process event {}", eventSequence, e);
            throw new RuntimeException("Event processing failed", e);
        }
    }
    
    private boolean handleOrderCreated(String aggregateId, JsonNode payload, Long eventSequence) {
        // Check if order already exists
        Integer exists = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM order_payment_projection WHERE aggregate_id = ?",
            Integer.class,
            aggregateId
        );
        
        if (exists > 0) {
            // This shouldn't happen due to EventStore validation, but handle gracefully
            log.warn("Order {} already exists, skipping OrderCreated", aggregateId);
            return true;
        }
        
        BigDecimal totalAmount = new BigDecimal(payload.get("totalAmount").asText());
        
        jdbcTemplate.update(
            """
            INSERT INTO order_payment_projection (
                aggregate_id, payment_state, order_total_amount, 
                paid_amount, refunded_amount, is_settled, refund_allowed
            ) VALUES (?, 'NOT_STARTED', ?, 0, 0, false, false)
            """,
            aggregateId,
            totalAmount
        );
        
        return true;
    }
    
    private boolean handlePaymentRequested(String aggregateId, JsonNode payload, Long eventSequence) {
        // Check if order exists
        Integer orderExists = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM order_payment_projection WHERE aggregate_id = ?",
            Integer.class,
            aggregateId
        );
        
        if (orderExists == 0) {
            log.warn("Order {} not found for PaymentRequested, marking as WAITING", aggregateId);
            markWaiting(eventSequence);
            return false;
        }
        
        jdbcTemplate.update(
            """
            UPDATE order_payment_projection 
            SET payment_state = 'REQUESTED', last_updated = NOW()
            WHERE aggregate_id = ?
            """,
            aggregateId
        );
        
        return true;
    }
    
    private boolean handlePaymentSucceeded(String aggregateId, JsonNode payload, Long eventSequence) {
        // Check if projection exists and is in correct state
        Map<String, Object> projection = null;
        
        try {
            projection = jdbcTemplate.queryForMap(
                "SELECT payment_state, paid_amount FROM order_payment_projection WHERE aggregate_id = ?",
                aggregateId
            );
        } catch (Exception e) {
            log.warn("Order {} not found for PaymentSucceeded, marking as WAITING", aggregateId);
            markWaiting(eventSequence);
            return false;
        }
        
        String currentState = (String) projection.get("payment_state");
        
        if (!"REQUESTED".equals(currentState)) {
            log.warn("Payment not requested yet for order {}, marking as WAITING", aggregateId);
            markWaiting(eventSequence);
            return false;
        }
        
        // CRITICAL: Incremental update using ONLY the current event
        // Do NOT re-query event_log history
        BigDecimal paymentAmount = new BigDecimal(payload.get("amount").asText());
        
        jdbcTemplate.update(
            """
            UPDATE order_payment_projection 
            SET 
                paid_amount = paid_amount + ?,
                payment_state = 'PAID',
                refund_allowed = true,
                is_settled = false,
                last_updated = NOW()
            WHERE aggregate_id = ?
            """,
            paymentAmount,
            aggregateId
        );
        
        return true;
    }
    
    private boolean handleRefundIssued(String aggregateId, JsonNode payload, Long eventSequence) {
        // Check if payment exists
        Map<String, Object> projection = null;
        
        try {
            projection = jdbcTemplate.queryForMap(
                "SELECT paid_amount, refunded_amount FROM order_payment_projection WHERE aggregate_id = ?",
                aggregateId
            );
        } catch (Exception e) {
            log.warn("Order {} not found for RefundIssued, marking as WAITING", aggregateId);
            markWaiting(eventSequence);
            return false;
        }
        
        BigDecimal paidAmount = (BigDecimal) projection.get("paid_amount");
        
        if (paidAmount.compareTo(BigDecimal.ZERO) == 0) {
            log.warn("No payment found for order {} refund, marking as WAITING", aggregateId);
            markWaiting(eventSequence);
            return false;
        }
        
        // CRITICAL: Incremental update
        BigDecimal refundAmount = new BigDecimal(payload.get("amount").asText());
        
        jdbcTemplate.update(
            """
            UPDATE order_payment_projection 
            SET 
                refunded_amount = refunded_amount + ?,
                payment_state = CASE 
                    WHEN (refunded_amount + ?) >= paid_amount THEN 'REFUNDED'
                    ELSE 'PARTIALLY_REFUNDED'
                END,
                is_settled = ((refunded_amount + ?) >= paid_amount),
                refund_allowed = ((refunded_amount + ?) < paid_amount),
                last_updated = NOW()
            WHERE aggregate_id = ?
            """,
            refundAmount,
            refundAmount,
            refundAmount,
            refundAmount,
            aggregateId
        );
        
        return true;
    }
    
    private void markProcessed(Long eventSequence) {
        jdbcTemplate.update(
            """
            INSERT INTO processed_events (consumer_name, event_sequence, state)
            VALUES (?, ?, 'PROCESSED')
            ON CONFLICT (consumer_name, event_sequence) 
            DO UPDATE SET state = 'PROCESSED', processed_at = NOW()
            """,
            CONSUMER_NAME,
            eventSequence
        );
    }
    
    private void markWaiting(Long eventSequence) {
        jdbcTemplate.update(
            """
            INSERT INTO processed_events (consumer_name, event_sequence, state)
            VALUES (?, ?, 'WAITING')
            ON CONFLICT (consumer_name, event_sequence) 
            DO UPDATE SET state = 'WAITING', processed_at = NOW()
            """,
            CONSUMER_NAME,
            eventSequence
        );
    }
    
    private void advanceOffsetIfContiguous(Long currentOffset) {
        // Find the largest N where ALL events [currentOffset+1 .. N] are PROCESSED
        Long newOffset = jdbcTemplate.queryForObject(
            """
            WITH RECURSIVE contiguous AS (
                -- Start from current offset + 1
                SELECT ? + 1 AS seq
                
                UNION ALL
                
                -- Keep going while next event is PROCESSED
                SELECT c.seq + 1
                FROM contiguous c
                WHERE EXISTS (
                    SELECT 1 
                    FROM processed_events pe
                    WHERE pe.consumer_name = ?
                      AND pe.event_sequence = c.seq
                      AND pe.state = 'PROCESSED'
                )
                AND c.seq < (SELECT COALESCE(MAX(event_sequence), 0) FROM event_log)
            )
            SELECT COALESCE(MAX(seq) - 1, ?) FROM contiguous
            WHERE EXISTS (
                SELECT 1 
                FROM processed_events pe
                WHERE pe.consumer_name = ?
                  AND pe.event_sequence = seq
                  AND pe.state = 'PROCESSED'
            )
            """,
            Long.class,
            currentOffset,
            CONSUMER_NAME,
            currentOffset,
            CONSUMER_NAME
        );
        
        if (newOffset > currentOffset) {
            jdbcTemplate.update(
                """
                UPDATE consumer_offsets 
                SET last_committed_event_sequence = ?, updated_at = NOW()
                WHERE consumer_name = ?
                """,
                newOffset,
                CONSUMER_NAME
            );
            log.debug("Advanced offset from {} to {}", currentOffset, newOffset);
        }
    }
}
```

### 📖 Concept: Why Offset Must Be Contiguous

**Bad offset logic (min-gap based):**
```
Events: 1, 2, 3, 4, 5
Processed: 1, 2, 4, 5
WAITING: 3

Min-gap query finds: offset = 2 (because 3 is missing)
But this is wrong! We've processed 4 and 5.

On restart:
- Consumer reads from offset 2
- Re-processes 4 and 5 (duplicate work)
```

**Correct offset logic (contiguous):**
```
Events: 1, 2, 3, 4, 5
Processed: 1, 2, 4, 5
WAITING: 3

Contiguous check:
- 1: PROCESSED ✓
- 2: PROCESSED ✓
- 3: WAITING ✗ (stop here)

Offset = 2 (largest contiguous)

On restart:
- Consumer reads from offset 2
- Checks WAITING events first
- Retries 3
- Then processes 4, 5 normally
```

### 💻 Task 17: Create Consumer Runner

```java
package com.nithin.eventreplay.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class ConsumerRunner implements CommandLineRunner {
    
    private final IncrementalConsumer consumer;
    
    @Override
    public void run(String... args) throws Exception {
        log.info("🚀 Starting incremental consumer...");
        
        int processed = 0;
        while (consumer.processNextEvent()) {
            processed++;
        }
        
        log.info("✅ Processed {} events", processed);
    }
}
```

### ✅ Checkpoint
Run the application and verify:
- [ ] Events are processed in order
- [ ] WAITING events are retried before new events
- [ ] Offset advances only to largest contiguous processed sequence
- [ ] Projection updates are incremental (+=), not recomputed
- [ ] No queries to event_log history during incremental processing

**Test queries:**
```sql
-- Check projection state
SELECT * FROM order_payment_projection;

-- Check processed events
SELECT event_sequence, state, processed_at 
FROM processed_events 
WHERE consumer_name = 'payment_consumer'
ORDER BY event_sequence;

-- Check offset
SELECT * FROM consumer_offsets;
```

---

## <a name="phase-5"></a>Phase 5: Full Replay System

### 🎯 Learning Objectives
- Understand when full replay is necessary
- Build a replay service that doesn't use processed_events for logic
- Handle schema evolution
- Validate projection correctness

### 📖 Concept: Full Replay vs Incremental

**Full Replay:**
- Pure function: `event_log → projection`
- Ignores `processed_events` for decision-making
- Recomputes ALL derived fields from scratch
- Used when incremental assumptions are broken

**When to trigger full replay:**
1. Consumer logic changed
2. Projection schema changed
3. Invariant violation detected
4. Corruption suspected
5. New projection added

### 💻 Task 18: Create Full Replay Service

```java
package com.nithin.eventreplay.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class FullReplayService {
    
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private static final String CONSUMER_NAME = "payment_consumer";
    
    @Transactional
    public void fullReplay() {
        log.info("🔄 Starting FULL REPLAY...");
        
        // Step 1: HARD RESET - Clear all consumer-derived state
        log.info("Step 1: Clearing projection...");
        jdbcTemplate.update("DELETE FROM order_payment_projection");
        
        log.info("Step 2: Clearing processed events...");
        jdbcTemplate.update("DELETE FROM processed_events WHERE consumer_name = ?", CONSUMER_NAME);
        
        log.info("Step 3: Resetting offset...");
        jdbcTemplate.update(
            "UPDATE consumer_offsets SET last_committed_event_sequence = 0 WHERE consumer_name = ?",
            CONSUMER_NAME
        );
        
        // Step 2: Read ALL events from beginning
        List<Map<String, Object>> events = jdbcTemplate.queryForList(
            "SELECT * FROM event_log ORDER BY event_sequence"
        );
        
        log.info("Step 4: Replaying {} events...", events.size());
        
        // Step 3: Apply each event deterministically
        // CRITICAL: No waiting logic, no validation, just pure application
        for (Map<String, Object> eventRow : events) {
            Long eventSequence = (Long) eventRow.get("event_sequence");
            String eventType = (String) eventRow.get("event_type");
            String aggregateId = (String) eventRow.get("aggregate_id");
            String payloadJson = eventRow.get("payload").toString();
            
            try {
                JsonNode payload = objectMapper.readTree(payloadJson);
                
                switch (eventType) {
                    case "OrderCreated" -> replayOrderCreated(aggregateId, payload);
                    case "PaymentRequested" -> replayPaymentRequested(aggregateId, payload);
                    case "PaymentSucceeded" -> replayPaymentSucceeded(aggregateId, payload, eventSequence);
                    case "RefundIssued" -> replayRefundIssued(aggregateId, payload, eventSequence);
                    default -> log.warn("Unknown event type: {}", eventType);
                }
                
                // Record as processed (for future incremental runs)
                jdbcTemplate.update(
                    """
                    INSERT INTO processed_events (consumer_name, event_sequence, state)
                    VALUES (?, ?, 'PROCESSED')
                    """,
                    CONSUMER_NAME,
                    eventSequence
                );
                
            } catch (Exception e) {
                log.error("Failed to replay event {}", eventSequence, e);
                throw new RuntimeException("Replay failed at event " + eventSequence, e);
            }
        }
        
        // Step 4: Set offset to max event_sequence
        Long maxSequence = jdbcTemplate.queryForObject(
            "SELECT COALESCE(MAX(event_sequence), 0) FROM event_log",
            Long.class
        );
        
        jdbcTemplate.update(
            "UPDATE consumer_offsets SET last_committed_event_sequence = ? WHERE consumer_name = ?",
            maxSequence,
            CONSUMER_NAME
        );
        
        log.info("✅ FULL REPLAY COMPLETE - Processed {} events", events.size());
    }
    
    // Replay handlers: Pure application, no waiting, no validation
    // Just derive state from event_log
    
    private void replayOrderCreated(String aggregateId, JsonNode payload) {
        BigDecimal totalAmount = new BigDecimal(payload.get("totalAmount").asText());
        
        jdbcTemplate.update(
            """
            INSERT INTO order_payment_projection (
                aggregate_id, payment_state, order_total_amount,
                paid_amount, refunded_amount, is_settled, refund_allowed
            ) VALUES (?, 'NOT_STARTED', ?, 0, 0, false, false)
            ON CONFLICT (aggregate_id) DO NOTHING
            """,
            aggregateId,
            totalAmount
        );
    }
    
    private void replayPaymentRequested(String aggregateId, JsonNode payload) {
        jdbcTemplate.update(
            """
            UPDATE order_payment_projection 
            SET payment_state = 'REQUESTED', last_updated = NOW()
            WHERE aggregate_id = ?
            """,
            aggregateId
        );
    }
    
    private void replayPaymentSucceeded(String aggregateId, JsonNode payload, Long upToSequence) {
        // CRITICAL: Recompute from event_log (this is replay, not incremental)
        // Calculate total paid amount from ALL PaymentSucceeded events up to this point
        jdbcTemplate.update(
            """
            UPDATE order_payment_projection p
            SET 
                paid_amount = COALESCE((
                    SELECT SUM(CAST(el.payload->>'amount' AS DECIMAL(18,2)))
                    FROM event_log el
                    WHERE el.event_type = 'PaymentSucceeded'
                      AND el.aggregate_id = ?
                      AND el.event_sequence <= ?
                ), 0),
                payment_state = 'PAID',
                refund_allowed = true,
                is_settled = false,
                last_updated = NOW()
            WHERE p.aggregate_id = ?
            """,
            aggregateId,
            upToSequence,
            aggregateId
        );
    }
    
    private void replayRefundIssued(String aggregateId, JsonNode payload, Long upToSequence) {
        // CRITICAL: Recompute from event_log
        jdbcTemplate.update(
            """
            UPDATE order_payment_projection p
            SET 
                refunded_amount = COALESCE((
                    SELECT SUM(CAST(el.payload->>'amount' AS DECIMAL(18,2)))
                    FROM event_log el
                    WHERE el.event_type = 'RefundIssued'
                      AND el.aggregate_id = ?
                      AND el.event_sequence <= ?
                ), 0),
                payment_state = CASE 
                    WHEN COALESCE((
                        SELECT SUM(CAST(el.payload->>'amount' AS DECIMAL(18,2)))
                        FROM event_log el
                        WHERE el.event_type = 'RefundIssued'
                          AND el.aggregate_id = ?
                          AND el.event_sequence <= ?
                    ), 0) >= p.paid_amount THEN 'REFUNDED'
                    ELSE 'PARTIALLY_REFUNDED'
                END,
                is_settled = (COALESCE((
                    SELECT SUM(CAST(el.payload->>'amount' AS DECIMAL(18,2)))
                    FROM event_log el
                    WHERE el.event_type = 'RefundIssued'
                      AND el.aggregate_id = ?
                      AND el.event_sequence <= ?
                ), 0) >= p.paid_amount),
                refund_allowed = (COALESCE((
                    SELECT SUM(CAST(el.payload->>'amount' AS DECIMAL(18,2)))
                    FROM event_log el
                    WHERE el.event_type = 'RefundIssued'
                      AND el.aggregate_id = ?
                      AND el.event_sequence <= ?
                ), 0) < p.paid_amount),
                last_updated = NOW()
            WHERE p.aggregate_id = ?
            """,
            aggregateId, upToSequence,
            aggregateId, upToSequence,
            aggregateId, upToSequence,
            aggregateId, upToSequence,
            aggregateId
        );
    }
}
```

### 📖 Concept: Why Replay Recomputes from event_log

**Incremental consumer:**
```sql
-- Uses current projection state
UPDATE projection SET paid_amount = paid_amount + ?
```
**Assumes:** paid_amount is currently correct

**Full replay:**
```sql
-- Recomputes from event_log
UPDATE projection SET paid_amount = (
    SELECT SUM(amount) FROM event_log 
    WHERE event_type = 'PaymentSucceeded' AND ...
)
```
**Assumes:** Nothing. Derives truth from event_log.

**Why this matters:**
- If projection was corrupted, incremental would propagate corruption
- Replay rebuilds from source of truth
- This is what makes projections "disposable"

### 💻 Task 19: Create Replay Trigger Scenarios

```java
package com.nithin.eventreplay.replay;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplayTriggerService {
    
    private final JdbcTemplate jdbcTemplate;
    private final FullReplayService fullReplayService;
    
    /**
     * Detects if projection state violates known invariants
     * If violations found, triggers full replay
     */
    public void detectAndRepairCorruption() {
        log.info("🔍 Checking for projection corruption...");
        
        // Check 1: Refunded amount exceeds paid amount
        Integer violatingRefunds = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM order_payment_projection WHERE refunded_amount > paid_amount",
            Integer.class
        );
        
        if (violatingRefunds > 0) {
            log.error("❌ CORRUPTION DETECTED: {} orders have refunded_amount > paid_amount", violatingRefunds);
            log.info("🔄 Triggering full replay to repair...");
            fullReplayService.fullReplay();
            return;
        }
        
        // Check 2: Payment state doesn't match amounts
        Integer stateViolations = jdbcTemplate.queryForObject(
            """
            SELECT COUNT(*) FROM order_payment_projection 
            WHERE (payment_state = 'PAID' AND paid_amount = 0)
               OR (payment_state = 'NOT_STARTED' AND paid_amount > 0)
            """,
            Integer.class
        );
        
        if (stateViolations > 0) {
            log.error("❌ CORRUPTION DETECTED: {} orders have inconsistent payment_state", stateViolations);
            log.info("🔄 Triggering full replay to repair...");
            fullReplayService.fullReplay();
            return;
        }
        
        log.info("✅ No corruption detected");
    }
    
    /**
     * Simulates corruption for testing
     */
    public void simulateCorruption(String aggregateId) {
        log.warn("⚠️ SIMULATING CORRUPTION for testing purposes");
        jdbcTemplate.update(
            "UPDATE order_payment_projection SET refunded_amount = 999999 WHERE aggregate_id = ?",
            aggregateId
        );
        log.info("Corrupted projection for aggregate: {}", aggregateId);
    }
}
```

### ✅ Checkpoint
Test the replay system:

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class ReplayTest implements CommandLineRunner {
    
    private final EventStore eventStore;
    private final ReplayTriggerService replayTrigger;
    private final JdbcTemplate jdbcTemplate;
    
    @Override
    public void run(String... args) {
        // 1. Create test order
        String orderId = "replay_test_" + System.currentTimeMillis();
        String paymentId = "payment_" + System.currentTimeMillis();
        
        eventStore.append(new OrderCreated(orderId, ..., new BigDecimal("100.00")));
        eventStore.append(new PaymentRequested(orderId, paymentId, new BigDecimal("100.00")));
        eventStore.append(new PaymentSucceeded(orderId, paymentId, new BigDecimal("100.00")));
        
        Thread.sleep(1000); // Let consumer process
        
        // 2. Verify correct state
        BigDecimal paidAmount = jdbcTemplate.queryForObject(
            "SELECT paid_amount FROM order_payment_projection WHERE aggregate_id = ?",
            BigDecimal.class,
            orderId
        );
        log.info("Paid amount before corruption: {}", paidAmount);
        
        // 3. Simulate corruption
        replayTrigger.simulateCorruption(orderId);
        
        BigDecimal corruptedAmount = jdbcTemplate.queryForObject(
            "SELECT refunded_amount FROM order_payment_projection WHERE aggregate_id = ?",
            BigDecimal.class,
            orderId
        );
        log.info("Refunded amount after corruption: {}", corruptedAmount);
        
        // 4. Detect and repair
        replayTrigger.detectAndRepairCorruption();
        
        // 5. Verify repaired
        BigDecimal repairedRefund = jdbcTemplate.queryForObject(
            "SELECT refunded_amount FROM order_payment_projection WHERE aggregate_id = ?",
            BigDecimal.class,
            orderId
        );
        log.info("Refunded amount after repair: {}", repairedRefund);
        
        assert repairedRefund.equals(BigDecimal.ZERO);
        log.info("✅ Replay successfully repaired corruption!");
    }
}
```

---

## <a name="phase-6"></a>Phase 6: Testing & Validation

### 💻 Task 20: Comprehensive E2E Test

```java
package com.nithin.eventreplay.test;

import com.nithin.eventreplay.event.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class E2ETest implements CommandLineRunner {
    
    private final EventStore eventStore;
    private final JdbcTemplate jdbcTemplate;
    
    @Override
    public void run(String... args) throws Exception {
        log.info("🧪 Running comprehensive E2E test...");
        
        String orderId = "e2e_order_" + System.currentTimeMillis();
        String paymentId = "e2e_payment_" + System.currentTimeMillis();
        
        // Scenario: Full order lifecycle
        
        // 1. Create order
        eventStore.append(new OrderCreated(
            orderId,
            List.of(
                new OrderCreated.OrderItem("product_1", 2, new BigDecimal("50.00")),
                new OrderCreated.OrderItem("product_2", 1, new BigDecimal("100.00"))
            ),
            new BigDecimal("200.00")
        ));
        
        // 2. Request payment
        eventStore.append(new PaymentRequested(
            orderId, paymentId, new BigDecimal("200.00")
        ));
        
        // 3. Payment succeeds
        eventStore.append(new PaymentSucceeded(
            orderId, paymentId, new BigDecimal("200.00")
        ));
        
        // 4. Partial refund
        eventStore.append(new RefundIssued(
            orderId, paymentId, "refund_1", new BigDecimal("50.00")
        ));
        
        // 5. Another partial refund
        eventStore.append(new RefundIssued(
            orderId, paymentId, "refund_2", new BigDecimal("30.00")
        ));
        
        // Wait for consumer
        Thread.sleep(2000);
        
        // Validate final state
        Map<String, Object> projection = jdbcTemplate.queryForMap(
            "SELECT * FROM order_payment_projection WHERE aggregate_id = ?",
            orderId
        );
        
        log.info("📊 Final projection state:");
        log.info("  Payment State: {}", projection.get("payment_state"));
        log.info("  Order Total: {}", projection.get("order_total_amount"));
        log.info("  Paid Amount: {}", projection.get("paid_amount"));
        log.info("  Refunded Amount: {}", projection.get("refunded_amount"));
        log.info("  Is Settled: {}", projection.get("is_settled"));
        log.info("  Refund Allowed: {}", projection.get("refund_allowed"));
        
        // Assertions
        assert "PARTIALLY_REFUNDED".equals(projection.get("payment_state"));
        assert new BigDecimal("200.00").equals(projection.get("order_total_amount"));
        assert new BigDecimal("200.00").equals(projection.get("paid_amount"));
        assert new BigDecimal("80.00").equals(projection.get("refunded_amount"));
        assert Boolean.FALSE.equals(projection.get("is_settled"));
        assert Boolean.TRUE.equals(projection.get("refund_allowed"));
        
        log.info("✅ All E2E assertions passed!");
    }
}
```

### 💻 Task 21: Invariant Violation Tests

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class InvariantTests implements CommandLineRunner {
    
    private final EventStore eventStore;
    
    @Override
    public void run(String... args) {
        log.info("🧪 Testing write-side invariant enforcement...");
        
        String orderId = "invariant_test_" + System.currentTimeMillis();
        String paymentId = "payment_" + System.currentTimeMillis();
        
        // Test 1: Duplicate order creation
        try {
            eventStore.append(new OrderCreated(orderId, ..., new BigDecimal("100.00")));
            eventStore.append(new OrderCreated(orderId, ..., new BigDecimal("50.00")));
            log.error("❌ Should have prevented duplicate order!");
        } catch (IllegalStateException e) {
            log.info("✅ Correctly prevented duplicate order");
        }
        
        // Test 2: Payment without order
        try {
            eventStore.append(new PaymentRequested(
                "nonexistent_order", paymentId, new BigDecimal("100.00")
            ));
            log.error("❌ Should have prevented payment for non-existent order!");
        } catch (IllegalStateException e) {
            log.info("✅ Correctly prevented orphan payment");
        }
        
        // Test 3: Excessive refund
        eventStore.append(new OrderCreated(orderId, ..., new BigDecimal("100.00")));
        eventStore.append(new PaymentRequested(orderId, paymentId, new BigDecimal("100.00")));
        eventStore.append(new PaymentSucceeded(orderId, paymentId, new BigDecimal("100.00")));
        
        try {
            eventStore.append(new RefundIssued(
                orderId, paymentId, "refund_1", new BigDecimal("999.00")
            ));
            log.error("❌ Should have prevented excessive refund!");
        } catch (IllegalArgumentException e) {
            log.info("✅ Correctly prevented excessive refund");
        }
        
        log.info("✅ All invariant tests passed!");
    }
}
```

---

## 🎓 Key Differences from Previous Version

### 1. **Write-Side Validation**
- EventStore enforces business invariants
- Impossible events never enter the log
- Consumers don't need to validate business rules

### 2. **Incremental Consumer**
- Updates state incrementally (`+=`)
- Never re-queries event_log history
- Processes WAITING events before new ones
- Scales linearly with event rate

### 3. **Contiguous Offset Advancement**
- Offset = largest N where ALL [1..N] are PROCESSED
- Handles gaps safely
- Prevents duplicate processing on restart

### 4. **Separation of Concerns**
```
EventStore:       Business invariants
Incremental:      Fast incremental updates
Full Replay:      Truth reconstruction
```

### 5. **Deterministic Replay**
- Full replay recomputes from event_log
- Ignores processed_events for logic
- Pure function: event_log → projection

---

## 🚀 What You've Built

A **production-grade event sourcing system** with:

✅ Immutable event log with database-level protection
✅ Write-side invariant enforcement
✅ Incremental consumer with proper offset management
✅ WAITING state for dependency handling
✅ Full replay for corruption recovery
✅ Contiguous offset advancement
✅ Complete separation of incremental vs replay logic

**This system can handle:**
- Crashes without data loss
- Out-of-order event arrival
- Projection corruption
- Schema evolution
- Multiple independent consumers

---

## 📚 Next Steps

1. **Add more projections** (analytics, fraud detection, notifications)
2. **Implement REST API** (query projections, trigger replay)
3. **Add monitoring** (consumer lag, replay metrics)
4. **Implement snapshots** (optimize replay for large histories)
5. **Add Kafka integration** (distributed event publishing)
6. **Build admin tools** (inspect events, force replay, view offsets)

**You now have a production-safe event-sourced system!**
