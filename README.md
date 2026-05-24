<div align="center">

<!-- Animated Banner -->
<img src="https://capsule-render.vercel.app/api?type=waving&color=0:0f0c29,50:302b63,100:24243e&height=200&section=header&text=iTrader&fontSize=80&fontColor=ffffff&fontAlignY=38&desc=Enterprise%20Cryptocurrency%20Exchange%20Platform&descAlignY=60&descAlign=50&animation=fadeIn" width="100%"/>

<br/>

<!-- Animated Typing -->
<a href="https://git.io/typing-svg">
  <img src="https://readme-typing-svg.demolab.com?font=JetBrains+Mono&weight=700&size=22&pause=1000&color=6C63FF&center=true&vCenter=true&width=700&lines=Real-Time+Cryptocurrency+Exchange;Event-Driven+Microservices+Architecture;Millisecond+Order+Matching+Engine;Enterprise-Grade+FinTech+Platform;Built+for+Scale%2C+Security+%26+Speed" alt="Typing SVG" />
</a>

<br/><br/>

<!-- Badges Row 1 -->
[![NestJS](https://img.shields.io/badge/NestJS-E0234E?style=for-the-badge&logo=nestjs&logoColor=white)](https://nestjs.com/)
[![Next.js](https://img.shields.io/badge/Next.js-000000?style=for-the-badge&logo=nextdotjs&logoColor=white)](https://nextjs.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-3178C6?style=for-the-badge&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Redis](https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io/)

<!-- Badges Row 2 -->
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-47A248?style=for-the-badge&logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white)](https://kubernetes.io/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)](https://www.terraform.io/)

<!-- Badges Row 3 -->
[![AWS](https://img.shields.io/badge/AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/)
[![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)](https://github.com/features/actions)
[![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com/)
[![License](https://img.shields.io/badge/License-MIT-22c55e?style=for-the-badge)](LICENSE)

<br/>

<!-- Stats Row -->
![Architecture](https://img.shields.io/badge/Microservices-15%2B-6C63FF?style=flat-square)
![Latency](https://img.shields.io/badge/Order%20Latency-%3C5ms-00d4aa?style=flat-square)
![Throughput](https://img.shields.io/badge/Throughput-100K%2B%20TPS-ff6b6b?style=flat-square)
![Availability](https://img.shields.io/badge/Availability-99.99%25-4ade80?style=flat-square)
![Real-Time](https://img.shields.io/badge/Real--Time-WebSocket%20%2B%20Kafka-f59e0b?style=flat-square)

</div>

---

## 📋 Table of Contents

- [Executive Summary](#-executive-summary)
- [System Vision](#-system-vision)
- [High-Level Architecture](#-high-level-architecture)
- [Technology Stack](#-technology-stack)
- [Microservices Breakdown](#-microservices-breakdown)
- [Kafka Event Architecture](#-kafka-event-architecture)
- [Real-Time Trading Engine](#-real-time-trading-engine)
- [WebSocket Streaming Design](#-websocket-streaming-design)
- [Frontend Architecture](#-frontend-architecture)
- [Security Architecture](#-security-architecture)
- [Database Strategy](#-database-strategy)
- [Scalability & High Availability](#-scalability--high-availability)
- [DevOps & Infrastructure](#-devops--infrastructure)
- [Production Deployment Strategy](#-production-deployment-strategy)
- [Future Enhancements](#-future-enhancements)

---

## 🚀 Executive Summary

**iTrader** is a production-grade, enterprise-class centralized cryptocurrency exchange platform engineered for ultra-high performance, regulatory compliance, and horizontal scalability. Architected on a fully event-driven microservices foundation, iTrader is designed to handle **100,000+ transactions per second** while maintaining **sub-5ms order matching latency** and **99.99% uptime SLA** — achieving performance parity with industry leaders such as Binance, Coinbase Pro, and Kraken.

The platform leverages a distributed, cloud-native architecture built on **Apache Kafka** for durable event streaming, **NestJS microservices** for the backend, **Next.js** for the frontend trading terminal, **Redis** for sub-millisecond caching and pub/sub, and **Kubernetes** for elastic, auto-scaling container orchestration.

> **iTrader is not a toy project. It is a blueprint for a production-ready, enterprise financial trading system.**

---

## 🌐 System Vision

```
iTrader Platform Vision
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ┌─────────────────────────────────────────────────────────┐
  │                  BUSINESS OBJECTIVES                    │
  │  • Real-time crypto spot trading (BTC, ETH, USDT…)     │
  │  • Institutional-grade order matching engine           │
  │  • Sub-millisecond WebSocket market data feeds          │
  │  • Enterprise compliance, audit, and KYC workflows      │
  │  • Horizontally scalable to millions of concurrent      │
  │    users without architectural rework                   │
  └─────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────┐
  │               ENGINEERING PHILOSOPHY                    │
  │  • Event-First: Every state change is a Kafka event     │
  │  • Domain Isolation: Single ownership per service       │
  │  • Resilience by Design: Circuit breakers everywhere    │
  │  • Observability: Metrics, traces, and logs built-in    │
  │  • Zero Trust Security: Defense-in-depth at every layer │
  └─────────────────────────────────────────────────────────┘
```

---

## 🏗️ High-Level Architecture

```
                          ┌──────────────────────────────────────┐
                          │           CLIENT LAYER               │
                          │  Browser / Mobile / Desktop App      │
                          │  Next.js Trading Terminal            │
                          └──────────────┬───────────────────────┘
                                         │ HTTPS / WSS
                          ┌──────────────▼───────────────────────┐
                          │          INGRESS LAYER               │
                          │   NGINX Ingress + Cloudflare CDN     │
                          │   DDoS Protection + TLS Termination  │
                          └──────────────┬───────────────────────┘
                                         │
                          ┌──────────────▼───────────────────────┐
                          │         API GATEWAY SERVICE          │
                          │  Rate Limiting · Auth · Routing      │
                          │  Request Validation · Load Balance   │
                          └───┬──────────────────────┬───────────┘
                              │ REST / gRPC           │ WebSocket
         ┌────────────────────▼──────┐      ┌─────────▼──────────────────┐
         │    MICROSERVICES MESH     │      │   WEBSOCKET GATEWAY        │
         │  (Kubernetes Cluster)     │      │   Real-Time Streaming      │
         │                           │      └────────────────────────────┘
         │  ┌──────────┐ ┌────────┐  │
         │  │   Auth   │ │  User  │  │      ┌──────────────────────────────┐
         │  │ Service  │ │Service │  │      │     KAFKA EVENT BUS          │
         │  └──────────┘ └────────┘  │◄────►│  Distributed Event Streaming │
         │  ┌──────────┐ ┌────────┐  │      │  15+ Topics · 3 Partitions   │
         │  │ Wallet   │ │Trading │  │      └──────────────────────────────┘
         │  │ Service  │ │Engine  │  │
         │  └──────────┘ └────────┘  │      ┌──────────────────────────────┐
         │  ┌──────────┐ ┌────────┐  │      │       DATA LAYER             │
         │  │  Order   │ │Order   │  │      │  PostgreSQL · MongoDB · Redis │
         │  │  Mgmt    │ │ Book   │  │◄────►│  Distributed · Replicated    │
         │  └──────────┘ └────────┘  │      └──────────────────────────────┘
         │  ┌──────────┐ ┌────────┐  │
         │  │  Market  │ │ Risk   │  │      ┌──────────────────────────────┐
         │  │  Data    │ │  Mgmt  │  │      │   OBSERVABILITY STACK        │
         │  └──────────┘ └────────┘  │      │  Prometheus · Grafana · ELK  │
         └───────────────────────────┘      └──────────────────────────────┘
```

---

## 🛠️ Technology Stack

<div align="center">

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Backend Framework** | NestJS + TypeScript | Modular microservices with dependency injection |
| **Message Broker** | Apache Kafka | Durable, high-throughput event streaming |
| **Cache / PubSub** | Redis Cluster | Sub-millisecond caching, WebSocket fanout |
| **Primary DB** | PostgreSQL 16 | ACID-compliant transactional data |
| **Document Store** | MongoDB Atlas | Time-series market data, trade history |
| **Service Mesh** | Kubernetes + Istio | Container orchestration, mTLS, traffic control |
| **API Gateway** | Custom NestJS + NGINX | Rate limiting, auth proxy, request routing |
| **Frontend** | Next.js 14 + Tailwind | SSR/CSR hybrid trading terminal |
| **State Management** | Zustand + React Query | Real-time reactive UI state |
| **IaC** | Terraform + Helm | Reproducible cloud infrastructure |
| **CI/CD** | GitHub Actions | Automated test, build, deploy pipelines |
| **Monitoring** | Prometheus + Grafana | Real-time platform observability |
| **Logging** | ELK Stack | Centralized structured log aggregation |
| **Tracing** | Jaeger / OpenTelemetry | Distributed request tracing |

</div>

---

## 🔧 Microservices Breakdown

> Each microservice is an independently deployable, domain-bounded unit with its own database, Kafka topics, and failure domain.

---

### 1. 🔀 API Gateway Service

```
Responsibility: Central ingress point for all external client traffic
Pattern:       Synchronous (REST) + WebSocket Upgrade
```

The API Gateway is the single entry point for all client interactions. It is **not** a simple reverse proxy — it implements application-layer logic including JWT validation, request fingerprinting, rate limiting via Redis sliding-window counters, and intelligent routing to downstream services.

**Core Capabilities:**
- **JWT Pre-validation** — validates token signatures before forwarding requests, offloading auth computation from downstream services
- **Rate Limiting** — per-user, per-IP, and per-endpoint limits using Redis token-bucket algorithm (e.g., 1,000 req/min for trading endpoints, 10,000 req/min for market data)
- **DDoS Mitigation** — integrates with Cloudflare's layer-7 WAF and implements challenge-based responses for anomalous traffic patterns
- **Request Routing** — routes to services based on path prefixes and HTTP method semantics; `/api/v1/orders` → Order Management, `/api/v1/wallet` → Wallet Service
- **Circuit Breaking** — wraps downstream calls with Hystrix-pattern circuit breakers; degrades gracefully under partial failure
- **Canary Routing** — supports traffic splitting for blue-green and A/B deployments via header-based routing rules

**Kafka Topics Produced:** `gateway.request.logged`

---

### 2. 🔐 Authentication Service

```
Responsibility: Identity, session lifecycle, and multi-factor authentication
Pattern:       Synchronous (gRPC internal) + Event (Kafka)
Database:      PostgreSQL (sessions, credentials) + Redis (token blacklist)
```

The Authentication Service is the security kernel of the platform. It manages the complete identity lifecycle — registration, login, MFA enforcement, token issuance, and revocation — while publishing security events for downstream audit consumption.

**Core Capabilities:**
- **JWT Architecture** — short-lived access tokens (15 minutes) paired with rotating refresh tokens (7 days) stored in HttpOnly cookies, preventing XSS token theft
- **Refresh Token Rotation** — each refresh operation issues a new refresh token and invalidates the previous one; replay detection via Redis-backed nonce tracking
- **MFA / 2FA** — TOTP-based (Google Authenticator) and SMS-based second factor enforcement; mandatory for withdrawal operations
- **OAuth 2.0 / OIDC** — supports federated identity via Google, Apple, and enterprise SSO providers
- **Argon2id Hashing** — industry-leading password hashing with memory-hard parameters to resist GPU brute-force attacks
- **Session Fingerprinting** — binds sessions to device fingerprint, IP range, and user agent; alerts on anomalous session reuse

**Kafka Topics Produced:** `user.authenticated`, `user.login.failed`, `user.mfa.bypassed`, `user.session.revoked`

---

### 3. 👤 User Service

```
Responsibility: User profile management, KYC workflow, and account preferences
Pattern:       Synchronous (REST/gRPC) + Event (Kafka)
Database:      PostgreSQL (profiles, KYC) + MongoDB (activity logs)
```

**Core Capabilities:**
- **KYC Tier Management** — three-tier KYC model (Tier 0: unverified, Tier 1: email verified, Tier 2: ID + liveness verified) with withdrawal limit enforcement per tier
- **KYC Document Processing** — integrates with third-party identity verification APIs (Jumio, Onfido) via async webhook pipeline; KYC events published to Kafka for wallet service to enforce limits
- **Account Preferences** — stores notification preferences, trading UI configurations, API key management, and security settings
- **GDPR Compliance** — implements data export, right-to-erasure workflows, and consent tracking

**Kafka Topics Produced:** `user.kyc.updated`, `user.profile.changed`, `user.api_key.created`

---

### 4. 💰 Wallet Service

```
Responsibility: Multi-asset ledger, deposits, withdrawals, and internal transfers
Pattern:       Synchronous (gRPC) + Event (Kafka)
Database:      PostgreSQL (ledger entries — double-entry accounting)
```

The Wallet Service is the financial nerve center of iTrader. It implements a **double-entry bookkeeping ledger** ensuring that every balance mutation is atomic, traceable, and auditable. No balance is modified without a corresponding ledger journal entry.

**Core Capabilities:**
- **Double-Entry Ledger** — every credit has a corresponding debit; total sum of all ledger entries always equals zero, providing a built-in audit mechanism
- **Asset Accounting** — tracks `available_balance`, `locked_balance`, and `total_balance` per asset per user; funds are locked (not deducted) on order placement and released on cancellation or settlement
- **On-Chain Integration** — deposit listeners monitor blockchain confirmations (configurable per asset: BTC requires 3 confirmations, ETH requires 12) before crediting user wallets
- **Withdrawal Pipeline** — multi-step withdrawal flow: user request → risk screening → 2FA confirmation → hot wallet signing → on-chain broadcast → confirmation tracking
- **Hot/Cold Wallet Segregation** — maintains ≤5% of total assets in hot wallets; remainder in cold storage via hardware security modules (HSM)

**Kafka Topics Produced:** `wallet.deposit.confirmed`, `wallet.withdrawal.initiated`, `wallet.balance.updated`, `wallet.funds.locked`, `wallet.funds.released`

---

### 5. ⚡ Trading Engine Service

```
Responsibility: Core order matching, trade execution, and settlement
Pattern:       Event-driven (Kafka) + Internal gRPC
Database:      PostgreSQL (trades) + Redis (in-flight order state)
```

The Trading Engine is the performance-critical core of iTrader. It implements a **price-time priority matching algorithm** — the same fundamental mechanism used by institutional exchanges — capable of processing thousands of order events per second with deterministic, sub-millisecond matching latency.

**Matching Algorithm:**
- **Limit Orders** — matched against the opposite side of the order book using price-time priority (best price first, then oldest first)
- **Market Orders** — execute aggressively against available liquidity, consuming depth until filled or liquidity exhausted
- **Partial Fills** — supported natively; remainder placed back as residual resting order
- **Atomic Settlement** — matched trades trigger atomic wallet adjustments via transactional Kafka messages; no partial settlement states possible

**Performance Architecture:**
- Single-threaded matching per trading pair to eliminate concurrency overhead and ensure deterministic execution order
- In-memory order book backed by sorted data structures (Red-Black Tree / Skip List) for O(log n) insertion and cancellation
- Kafka consumer group with one partition per trading pair symbol ensures ordered, sequential processing
- Horizontal scaling achieved by distributing symbols across partitions and consumer instances

**Kafka Topics Consumed:** `order.created`, `order.cancel.requested`  
**Kafka Topics Produced:** `order.matched`, `trade.completed`, `order.partially.filled`

---

### 6. 📋 Order Management Service

```
Responsibility: Order lifecycle management, validation, and state tracking
Pattern:       Synchronous (REST) + Event (Kafka)
Database:      PostgreSQL (order records) + Redis (active order cache)
```

**Core Capabilities:**
- **Order Types** — Market, Limit, Stop-Limit, and Stop-Market orders with full lifecycle state management (`PENDING` → `OPEN` → `PARTIALLY_FILLED` → `FILLED` / `CANCELLED`)
- **Pre-Trade Validation** — validates sufficient wallet balance before order acceptance; locks funds atomically before publishing to Kafka
- **Idempotency** — client-provided `clientOrderId` used for deduplication; prevents duplicate order submission on network retries
- **Order History** — maintains queryable order history with full audit trail including all state transitions and timestamps

**Kafka Topics Produced:** `order.created`, `order.cancelled`, `order.expired`

---

### 7. 📊 Order Book Service

```
Responsibility: Real-time bid/ask aggregation, depth calculation, and snapshot management
Pattern:       Event-driven (Kafka) + WebSocket fanout via Redis Pub/Sub
Database:      Redis (live order book state — no persistent DB)
```

**Core Capabilities:**
- **In-Memory Order Book** — maintains live bid and ask depth in Redis sorted sets, enabling sub-millisecond depth queries
- **Depth Aggregation** — publishes aggregated depth snapshots at configurable price level granularities (e.g., top-5, top-20, full depth)
- **Delta Updates** — publishes incremental diff events rather than full snapshots on every change, dramatically reducing WebSocket bandwidth
- **Sequence Numbering** — each update carries a monotonic sequence number enabling clients to detect and recover from missed updates by requesting a fresh snapshot

**Kafka Topics Consumed:** `order.created`, `order.matched`, `order.cancelled`  
**Kafka Topics Produced:** `orderbook.depth.updated`, `orderbook.snapshot.requested`

---

### 8. 📈 Trade Book Service

```
Responsibility: Executed trade history, real-time trade stream, and aggregated trade data
Pattern:       Event-driven (Kafka)
Database:      MongoDB (trade documents — time-series optimized)
```

**Core Capabilities:**
- **Real-Time Trade Feed** — consumes `trade.completed` events and fans out to WebSocket subscribers within single-digit milliseconds
- **Trade Aggregation** — maintains rolling aggregations (last price, 24h volume, 24h price change, 24h high/low) per symbol in Redis
- **Historical Queries** — MongoDB time-series collections provide efficient range queries for trade history with TTL-based archiving

**Kafka Topics Consumed:** `trade.completed`  
**Kafka Topics Produced:** `tradebook.trade.published`

---

### 9. 💹 Market Data Service

```
Responsibility: OHLCV candlestick generation, ticker aggregation, and price feed
Pattern:       Event-driven (Kafka)
Database:      MongoDB (OHLCV candles) + Redis (live ticker cache)
```

**Core Capabilities:**
- **Multi-Timeframe Candlesticks** — generates OHLCV bars for 1m, 5m, 15m, 1h, 4h, 1d, 1w from raw trade events using streaming aggregation
- **Ticker Engine** — maintains per-symbol real-time tickers including last price, bid/ask, 24h stats, and open interest
- **Price Oracle** — publishes authoritative `market.price.updated` events consumed by the Risk Management Service for position valuation
- **External Feed Integration** — optional integration with external price oracles (Chainlink, CoinGecko) for reference pricing and index calculations

**Kafka Topics Consumed:** `trade.completed`  
**Kafka Topics Produced:** `market.price.updated`, `market.candle.updated`, `market.ticker.updated`

---

### 10. 🔌 WebSocket Gateway Service

```
Responsibility: Real-time bi-directional streaming to all connected clients
Pattern:       WebSocket (client-facing) + Redis Pub/Sub (internal fanout)
Database:      Redis (subscription registry, connection state)
```

The WebSocket Gateway manages tens of thousands of concurrent client connections, delivering real-time market data, order updates, and trade executions with sub-100ms end-to-end latency.

**Core Capabilities:**
- **Subscription Channels** — clients subscribe to specific channels: `orderbook@{symbol}`, `trades@{symbol}`, `ticker@{symbol}`, `orders@{userId}`, `balances@{userId}`
- **Redis Fanout** — backend services publish events to Redis Pub/Sub channels; WebSocket Gateway instances subscribe and fan out to connected clients
- **Heartbeat Management** — bidirectional ping/pong with configurable timeout; stale connections pruned automatically
- **Connection Sharding** — multiple gateway instances handle different connection pools; Redis subscription registry ensures correct routing of user-specific events

**Kafka Topics Consumed:** `websocket.broadcast`

---

### 11. 🔔 Notification Service

```
Responsibility: Multi-channel user notification delivery
Pattern:       Event-driven (Kafka consumer)
Database:      MongoDB (notification history) + PostgreSQL (preferences)
```

**Core Capabilities:**
- **Multi-Channel Delivery** — Email (AWS SES / SendGrid), Push (Firebase FCM), SMS (Twilio), and in-app notifications
- **Templating Engine** — Handlebars-based templates with i18n support for multi-language notifications
- **Rate Limiting** — prevents notification flooding; deduplicates identical events within configurable windows
- **Delivery Tracking** — tracks open rates and delivery status; retries failed deliveries with exponential backoff

**Kafka Topics Consumed:** `trade.completed`, `wallet.deposit.confirmed`, `wallet.withdrawal.initiated`, `risk.alert.triggered`, `user.authenticated`

---

### 12. 🛡️ Risk Management Service

```
Responsibility: Real-time position risk scoring, fraud detection, and trading controls
Pattern:       Event-driven (Kafka) + Synchronous pre-trade validation
Database:      Redis (risk scores, exposure counters) + PostgreSQL (risk rules)
```

**Core Capabilities:**
- **Pre-Trade Risk Checks** — validates orders against configurable risk rules before acceptance: daily trading limits, single-order size limits, and suspicious pattern flags
- **Anomaly Detection** — statistical models identify wash trading, layering, spoofing, and pump-and-dump patterns in real time
- **Exposure Monitoring** — tracks aggregate open exposure per user and asset; automatically triggers risk alerts when thresholds are breached
- **Regulatory Controls** — supports trading halts, circuit breakers, and kill-switch capabilities for compliance and market integrity

**Kafka Topics Consumed:** `order.created`, `trade.completed`, `market.price.updated`  
**Kafka Topics Produced:** `risk.alert.triggered`, `order.risk.rejected`

---

### 13. 📡 Kafka Event Streaming Service

```
Responsibility: Platform-wide event backbone, topic governance, and schema registry
Pattern:       Infrastructure service (not a runtime microservice)
```

This represents the governance layer over the Kafka deployment — schema registry management, topic provisioning, consumer group monitoring, and dead-letter queue handling.

**Core Capabilities:**
- **Schema Registry** — Confluent Schema Registry enforces Avro/Protobuf schemas, preventing schema drift across producer/consumer boundaries
- **Dead Letter Queue (DLQ)** — malformed or unprocessable messages are routed to DLQ topics for out-of-band investigation and replay
- **Event Replay** — Kafka's durable log enables replay of any event stream for audit, debugging, and reprocessing scenarios
- **Consumer Group Lag Monitoring** — Prometheus integration tracks consumer lag per group; auto-scaling triggers when lag exceeds SLA thresholds

---

### 14. 📝 Audit & Logging Service

```
Responsibility: Immutable audit trail, compliance logging, and activity records
Pattern:       Event-driven (Kafka consumer — all topics)
Database:      PostgreSQL (audit records — append-only) + Elasticsearch (searchable logs)
```

**Core Capabilities:**
- **Immutable Audit Log** — append-only audit records with cryptographic hash chaining (each record includes hash of previous record), making retroactive tampering detectable
- **Compliance Tracking** — captures all financial operations, authentication events, and administrative actions with full context for regulatory reporting (MiCA, FinCEN, FATF)
- **Structured Logging** — standardized JSON log schema across all services; shipped to ELK stack via Filebeat
- **Retention Policy** — tiered storage: hot logs in Elasticsearch (30 days), warm in S3 Glacier-compatible (7 years)

---

### 15. 🔧 Admin Service

```
Responsibility: Back-office operations, user management, trading controls, and platform monitoring
Pattern:       Synchronous (REST) — internal network only
Database:      PostgreSQL (admin actions) + all service databases (read models)
```

**Core Capabilities:**
- **User Management** — KYC approval/rejection, account suspension, balance adjustments with mandatory dual-approval workflow
- **Trading Controls** — market suspension per symbol, trading halt, circuit breaker activation
- **Risk Dashboard** — real-time aggregate risk exposure visualization, suspicious account flagging
- **Role-Based Admin Access** — granular permission model (Viewer, Operator, Supervisor, Super Admin) with all actions logged to audit trail

---

## 📨 Kafka Event Architecture

### Why Kafka?

Apache Kafka is the connective tissue of iTrader. Traditional synchronous request-response inter-service communication creates tight coupling, cascading failure risk, and throughput bottlenecks under load. Kafka solves these fundamental distributed systems problems:

```
Traditional Synchronous Model          Event-Driven Kafka Model
━━━━━━━━━━━━━━━━━━━━━━━━━━━━           ━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Service A ──HTTP──► Service B          Service A ──Kafka──► Topic
                                                              │
  ✗ Tight coupling                      Service B ◄──────────┤
  ✗ Cascading failures                  Service C ◄──────────┤
  ✗ Synchronous blocking                Service D ◄──────────┘
  ✗ No replay capability
                                        ✓ Full decoupling
                                        ✓ Independent scaling
                                        ✓ Failure isolation
                                        ✓ Event replay for DR
```

### Topic Architecture

```
╔══════════════════════════════════════════════════════════════════╗
║                    KAFKA TOPIC REGISTRY                         ║
╠══════════════════════╦═══════════════════════════╦══════════════╣
║ Topic Name           ║ Producer(s)               ║ Consumer(s)  ║
╠══════════════════════╬═══════════════════════════╬══════════════╣
║ order.created        ║ Order Management          ║ Trading Eng  ║
║                      ║                           ║ Risk Mgmt    ║
║                      ║                           ║ Audit        ║
╠══════════════════════╬═══════════════════════════╬══════════════╣
║ order.matched        ║ Trading Engine            ║ Order Book   ║
║                      ║                           ║ Trade Book   ║
║                      ║                           ║ Wallet       ║
╠══════════════════════╬═══════════════════════════╬══════════════╣
║ trade.completed      ║ Trading Engine            ║ Market Data  ║
║                      ║                           ║ Notification ║
║                      ║                           ║ Trade Book   ║
║                      ║                           ║ Audit        ║
╠══════════════════════╬═══════════════════════════╬══════════════╣
║ market.price.updated ║ Market Data               ║ WebSocket GW ║
║                      ║                           ║ Risk Mgmt    ║
╠══════════════════════╬═══════════════════════════╬══════════════╣
║ wallet.balance.upd.  ║ Wallet Service            ║ WebSocket GW ║
║                      ║                           ║ Notification ║
║                      ║                           ║ Audit        ║
╠══════════════════════╬═══════════════════════════╬══════════════╣
║ risk.alert.triggered ║ Risk Management           ║ Admin        ║
║                      ║                           ║ Notification ║
║                      ║                           ║ Audit        ║
╠══════════════════════╬═══════════════════════════╬══════════════╣
║ websocket.broadcast  ║ All services              ║ WebSocket GW ║
╠══════════════════════╬═══════════════════════════╬══════════════╣
║ user.authenticated   ║ Auth Service              ║ Notification ║
║                      ║                           ║ Audit        ║
╚══════════════════════╩═══════════════════════════╩══════════════╝
```

### Partitioning Strategy

All order and trade topics are partitioned by **trading symbol** (e.g., `BTCUSDT`, `ETHUSDT`). This ensures:
1. All events for a given symbol arrive at the same partition in order
2. The Trading Engine processes one symbol on one consumer thread — no concurrent mutation races
3. New symbols horizontally scale by adding Kafka partitions without rebalancing existing ones

---

## ⚡ Real-Time Trading Engine

### Complete Order Lifecycle

```
User Places Order
       │
       ▼
┌─────────────────────┐
│  API Gateway        │  ← JWT validation, rate limit check
│  Order Validation   │
└─────────┬───────────┘
          │ REST POST /api/v1/orders
          ▼
┌─────────────────────┐
│  Order Management   │  ← Idempotency check (clientOrderId)
│  Service            │  ← Pre-trade balance validation (gRPC → Wallet)
│                     │  ← Fund locking (wallet.funds.locked event)
└─────────┬───────────┘
          │ Kafka: order.created
          ▼
┌─────────────────────┐
│  Trading Engine     │  ← Receives order from Kafka topic
│  (Matching Engine)  │  ← Price-time priority matching
│                     │  ← Partial / full fill determination
└─────────┬───────────┘
          │ Kafka: order.matched / trade.completed
          ├──────────────────────────────────────────┐
          ▼                                          ▼
┌─────────────────────┐                  ┌───────────────────────┐
│  Wallet Service     │                  │  Order Book Service   │
│  Settlement         │                  │  Depth Update         │
│  (atomic transfer)  │                  │  (delta broadcast)    │
└─────────┬───────────┘                  └───────────┬───────────┘
          │                                          │
          ▼                                          ▼
┌─────────────────────┐                  ┌───────────────────────┐
│  Market Data        │                  │  WebSocket Gateway    │
│  OHLCV Update       │                  │  Client Broadcast     │
│  Ticker Update      │                  │  (<50ms end-to-end)   │
└─────────────────────┘                  └───────────────────────┘
```

### Matching Engine Design

```
Order Book State (BTCUSDT)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    ASKS (Sell Orders — sorted ascending)
    ┌────────────────────────────────────┐
    │  $45,200  │  0.50 BTC  │ 14:32:01 │  ← Best Ask
    │  $45,205  │  1.20 BTC  │ 14:31:55 │
    │  $45,210  │  0.75 BTC  │ 14:31:48 │
    └────────────────────────────────────┘
                   SPREAD: $5
    ┌────────────────────────────────────┐
    │  $45,195  │  0.30 BTC  │ 14:32:05 │  ← Best Bid
    │  $45,190  │  2.00 BTC  │ 14:31:22 │
    │  $45,185  │  0.80 BTC  │ 14:30:59 │
    └────────────────────────────────────┘
    BIDS (Buy Orders — sorted descending)

Incoming: BUY LIMIT 0.40 BTC @ $45,200
→ Matches against Best Ask (0.50 BTC @ $45,200)
→ Fills 0.40 BTC, residual 0.10 BTC remains as Ask
→ Trade generated: 0.40 BTC @ $45,200
→ Wallet settlement: Buyer +0.40 BTC, Seller +$18,080
```

---

## 📡 WebSocket Streaming Design

### Connection Architecture

```
Client Browser
     │ WSS Connection
     ▼
┌─────────────────────────────────────┐
│     WebSocket Gateway (Pod 1)       │
│  Connection Pool: 50,000 sessions   │
│                                     │
│  Channel Subscriptions:             │
│   • orderbook@BTCUSDT               │
│   • trades@ETHUSDT                  │
│   • orders@{userId}                 │
│   • balances@{userId}               │
└───────────┬─────────────────────────┘
            │ Redis Pub/Sub Subscribe
            ▼
┌─────────────────────────────────────┐
│     Redis Cluster                   │
│  Pub/Sub Channels mirror Kafka      │
│  topics; all WS pods subscribe      │
└───────────┬─────────────────────────┘
            │ Redis Publish
            ▼
   Backend Services (Market Data,
   Order Book, Trade Book, Wallet)
   publish events to Redis channels
   after consuming from Kafka
```

### Message Protocol

```jsonc
// Server → Client: Order Book Update
{
  "type": "orderbook.update",
  "symbol": "BTCUSDT",
  "seq": 1847392,          // Monotonic sequence for gap detection
  "bids": [
    ["45195.00", "0.30"],   // [price, quantity]
    ["45190.00", "2.00"]
  ],
  "asks": [
    ["45200.00", "0.10"],
    ["45205.00", "1.20"]
  ],
  "timestamp": 1716547200000
}

// Server → Client: Trade Execution
{
  "type": "trade.executed",
  "symbol": "BTCUSDT",
  "tradeId": "trd_9f3k2m",
  "price": "45200.00",
  "quantity": "0.40",
  "side": "BUY",
  "timestamp": 1716547200123
}
```

---

## 🖥️ Frontend Architecture

### Application Structure

```
apps/web (Next.js 14)
├── app/
│   ├── (auth)/
│   │   ├── login/          ← SSR — SEO + fast initial load
│   │   └── register/
│   ├── (trading)/
│   │   ├── markets/        ← SSR — market listings
│   │   └── trade/[symbol]/ ← CSR — full real-time terminal
│   ├── wallet/             ← SSR + CSR hybrid
│   ├── portfolio/          ← SSR — analytics dashboard
│   └── admin/              ← CSR — back-office SPA
├── components/
│   ├── TradingChart/       ← TradingView Lightweight Charts
│   ├── OrderBook/          ← Virtualized list (react-window)
│   ├── OrderForm/          ← Trading form with live validation
│   ├── TradeHistory/       ← Paginated + real-time append
│   └── DepthChart/         ← Bid/ask depth visualization
├── hooks/
│   ├── useWebSocket.ts     ← Managed WS connection with reconnect
│   ├── useOrderBook.ts     ← Delta-patching order book state
│   └── useMarketData.ts    ← Live ticker subscriptions
└── store/
    ├── trading.store.ts    ← Zustand — order form, selected symbol
    └── wallet.store.ts     ← React Query — balance data
```

### Rendering Strategy

| Page | Strategy | Rationale |
|------|----------|-----------|
| `/markets` | SSR | SEO-indexable, fast first paint, data prefetched on server |
| `/trade/[symbol]` | CSR (after hydration) | Full real-time interactivity; SSR for initial snapshot |
| `/wallet` | SSR + Client Hydration | Balance data from server; real-time updates via WebSocket |
| `/portfolio` | ISR (5 min revalidation) | Analytics are read-heavy; stale-while-revalidate acceptable |
| `/admin` | CSR | No SEO required; SPA behavior preferred |

### Real-Time UI Optimization

- **Virtualized Order Book** — `react-window` renders only visible rows; 500-level order books render at 60fps with zero jank
- **Throttled State Updates** — order book delta patches batched and applied at 100ms intervals to prevent React render thrashing
- **Web Workers** — heavy chart data processing offloaded to dedicated Web Worker threads, keeping main thread free for interactions
- **Optimistic Updates** — order placements reflected immediately in UI with pending state; reconciled on Kafka confirmation

---

## 🔒 Security Architecture

### Defense-in-Depth Model

```
Layer 0: Network Edge
  • Cloudflare WAF + DDoS mitigation
  • TLS 1.3 everywhere; HSTS enforced
  • IP allowlisting for admin endpoints

Layer 1: API Gateway
  • JWT signature verification
  • Rate limiting (sliding window, per user/IP)
  • Request size limits and payload validation
  • CORS policy enforcement

Layer 2: Service Authentication
  • mTLS between all microservices (Istio service mesh)
  • Service account tokens with minimal scope
  • Zero-trust: services cannot directly access other services' databases

Layer 3: Application
  • Role-Based Access Control (RBAC) enforced per endpoint
  • Input validation with class-validator (NestJS)
  • SQL parameterization — zero raw query construction
  • Secrets injected via Kubernetes Secrets + HashiCorp Vault

Layer 4: Data
  • PostgreSQL encryption at rest (AWS RDS encrypted volumes)
  • Column-level encryption for PII (AES-256)
  • Database access only via service account credentials
  • Regular automated backups with point-in-time recovery

Layer 5: Operational
  • Immutable audit logs with hash chaining
  • All admin actions require dual approval + audit entry
  • Security incident runbooks automated in PagerDuty
```

### Kafka Security Configuration

```yaml
# Kafka TLS + SASL Configuration
security.protocol: SASL_SSL
sasl.mechanism: SCRAM-SHA-512
ssl.truststore.location: /certs/kafka.truststore.jks
ssl.keystore.location: /certs/kafka.keystore.jks
# Topic-level ACLs: producers only write to their owned topics
# Consumers only read from their subscribed consumer groups
```

---

## 🗄️ Database Strategy

### Multi-Model Persistence

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATABASE SELECTION RATIONALE                 │
├──────────────┬──────────────────────┬───────────────────────────┤
│  Database    │  Used For            │  Why                      │
├──────────────┼──────────────────────┼───────────────────────────┤
│ PostgreSQL   │ Orders, Trades,      │ ACID guarantees for       │
│ (Primary)    │ Users, Wallets,      │ financial operations;     │
│              │ Audit Logs           │ strong consistency;       │
│              │                      │ complex joins supported   │
├──────────────┼──────────────────────┼───────────────────────────┤
│ MongoDB      │ OHLCV Candles,       │ Document model fits       │
│              │ Trade History,       │ time-series; flexible     │
│              │ Activity Logs        │ schema for market data;   │
│              │                      │ efficient range queries   │
├──────────────┼──────────────────────┼───────────────────────────┤
│ Redis        │ Order Books,         │ Sub-millisecond reads;    │
│ Cluster      │ Live Tickers,        │ sorted sets for order     │
│              │ Session Tokens,      │ book; pub/sub for WS      │
│              │ Rate Limit Counters  │ fanout; atomic ops        │
└──────────────┴──────────────────────┴───────────────────────────┘
```

### PostgreSQL Optimization

- **Read Replicas** — all read-heavy queries (order history, trade history) routed to read replicas; writes go to primary
- **Table Partitioning** — `orders` and `trades` tables partitioned by `created_at` month; historical partitions archived to cold storage
- **Connection Pooling** — PgBouncer in transaction-mode pooling reduces connection overhead for high-throughput services
- **Indexing Strategy** — composite indexes on `(user_id, symbol, created_at)` for user order history; partial indexes for `status = 'OPEN'` for active order queries

### MongoDB Time-Series

```javascript
// MongoDB time-series collection for OHLCV data
db.createCollection("candles_1m", {
  timeseries: {
    timeField: "timestamp",
    metaField: "symbol",
    granularity: "minutes"
  },
  expireAfterSeconds: 7776000  // 90 days hot retention
})
```

---

## 📐 Scalability & High Availability

### Horizontal Scaling Architecture

```
                    Load Balancer (NGINX / AWS ALB)
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         Gateway-1     Gateway-2    Gateway-3
         (Pod)         (Pod)        (Pod)
              │
    ┌─────────┼─────────┐
    ▼         ▼         ▼
 Trading   Trading   Trading       ← Scaled per symbol throughput
 Engine-1  Engine-2  Engine-3
 (BTC,ETH) (BNB,SOL) (DOGE,XRP)
```

### Resilience Patterns

| Pattern | Implementation | Purpose |
|---------|---------------|---------|
| Circuit Breaker | NestJS + `cockatiel` library | Prevent cascade failures |
| Retry with Backoff | Exponential backoff + jitter | Handle transient errors |
| Bulkhead | K8s resource limits per pod | Isolate failure domains |
| Timeout | Per-service gRPC deadlines | Prevent thread starvation |
| Health Checks | Kubernetes liveness/readiness | Automatic pod replacement |
| Graceful Degradation | Feature flags + fallback data | Partial outage handling |

### Redis High Availability

```yaml
# Redis Cluster: 3 Master + 3 Replica shards
# Automatic failover via Redis Sentinel
# Data sharded by hash slot across masters
redis-cluster:
  nodes: 6          # 3 master + 3 replica
  replicas: 1
  maxmemory-policy: allkeys-lru
  persistence: AOF (appendonly yes, fsync everysec)
```

---

## 🚀 DevOps & Infrastructure

### CI/CD Pipeline

```yaml
# GitHub Actions Pipeline
on: [push, pull_request]

jobs:
  test:
    - Unit Tests (Jest)
    - Integration Tests (Testcontainers)
    - Contract Tests (Pact)
    - SAST Security Scan (Snyk)

  build:
    - Docker multi-stage build
    - Image vulnerability scan (Trivy)
    - Push to ECR with semantic version tag

  deploy-staging:
    - Helm upgrade --install (staging namespace)
    - Smoke tests via k6
    - Performance regression check

  deploy-production:
    - Blue-Green deployment via Argo Rollouts
    - 10% canary → 50% → 100% over 30 minutes
    - Automatic rollback on error rate spike
```

### Kubernetes Deployment Model

```yaml
# HorizontalPodAutoscaler — Trading Engine
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: trading-engine-hpa
spec:
  scaleTargetRef:
    name: trading-engine
  minReplicas: 3
  maxReplicas: 20
  metrics:
    - type: External
      external:
        metric:
          name: kafka_consumer_lag    # Scale on Kafka lag
        target:
          type: AverageValue
          averageValue: "1000"        # Scale when lag > 1000 messages
```

### Observability Stack

```
Metrics:  Prometheus → Grafana dashboards
          • Order throughput (orders/sec per symbol)
          • Matching latency (p50, p95, p99)
          • Kafka consumer lag per consumer group
          • WebSocket connection count per pod
          • Wallet settlement success rate

Logging:  Filebeat → Logstash → Elasticsearch → Kibana
          • Structured JSON logs with correlation IDs
          • Full distributed trace ID propagation

Tracing:  OpenTelemetry → Jaeger
          • End-to-end order lifecycle traces
          • Cross-service latency attribution

Alerting: Prometheus Alertmanager → PagerDuty
          • P1: Matching engine down (immediate page)
          • P2: Kafka lag > 10,000 (5-min page)
          • P3: Error rate > 1% (Slack notification)
```

---

## 🏭 Production Deployment Strategy

### Infrastructure as Code

```
terraform/
├── modules/
│   ├── eks/          ← EKS cluster definition
│   ├── rds/          ← PostgreSQL Multi-AZ
│   ├── elasticache/  ← Redis cluster
│   ├── msk/          ← Amazon MSK (Kafka)
│   └── networking/   ← VPC, subnets, security groups
└── environments/
    ├── staging/
    └── production/
```

### Multi-Region Active-Passive Setup

```
Region: us-east-1 (Primary — Active)
  ├── EKS Cluster (all 15 microservices)
  ├── RDS PostgreSQL Multi-AZ (primary + standby)
  ├── MSK Kafka (3 broker, 3 AZ)
  └── ElastiCache Redis Cluster

Region: eu-west-1 (DR — Passive)
  ├── EKS Cluster (read-only services)
  ├── RDS Read Replica (cross-region replication)
  └── ElastiCache Redis (passive replica)

RTO: < 15 minutes    │    RPO: < 30 seconds
```

---

## 🔮 Future Enhancements

| Roadmap Item | Description | Priority |
|--------------|-------------|----------|
| **Futures Trading** | Perpetual contracts with funding rate mechanism | P1 |
| **Options Engine** | European/American options with Black-Scholes pricing | P2 |
| **Margin Trading** | Cross and isolated margin with auto-liquidation engine | P1 |
| **DeFi Bridge** | Cross-chain asset bridging via on-chain smart contracts | P2 |
| **FIX Protocol** | Industry-standard FIX 4.4 gateway for institutional clients | P1 |
| **Copy Trading** | Social trading layer with strategy replication | P3 |
| **ML Fraud Detection** | Real-time ML model for anomaly detection (Flink + TensorFlow) | P2 |
| **Multi-Region Active-Active** | Conflict-free replicated data types (CRDTs) for global deployment | P2 |
| **Dark Pool** | Institutional block trading with RFQ (Request for Quote) matching | P3 |

---

## 📊 System Performance Targets

<div align="center">

| Metric | Target | Industry Benchmark |
|--------|--------|-------------------|
| Order Matching Latency | < 5ms (p99) | Binance: ~1-2ms |
| API Response Time | < 50ms (p95) | Coinbase: ~20-80ms |
| WebSocket Update Delay | < 100ms end-to-end | Kraken: ~50-100ms |
| System Throughput | 100,000 orders/sec | Binance peak: ~1.4M/sec |
| Platform Availability | 99.99% (52 min/year) | Industry standard: 99.9% |
| RTO (Recovery Time) | < 15 minutes | Exchange standard: < 1hr |
| RPO (Recovery Point) | < 30 seconds | Exchange standard: < 5min |

</div>

---

## 👨‍💻 Author

<div align="center">

**Designed & Architected as a Senior Staff Engineer Portfolio Project**

*This document represents enterprise-level system design thinking applied to one of the most technically demanding domains in software engineering — real-time financial trading systems.*

<br/>

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com)
[![Email](https://img.shields.io/badge/Email-Contact-EA4335?style=for-the-badge&logo=gmail&logoColor=white)](mailto:your@email.com)

</div>

---

<div align="center">

**⭐ If this architecture blueprint helped you, consider starring the repo!**

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:0f0c29,50:302b63,100:24243e&height=120&section=footer&animation=fadeIn" width="100%"/>

</div>
