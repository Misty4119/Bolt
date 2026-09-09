# Bolt 資料庫與跨伺服器部署

## 支援邊界

- PostgreSQL 18+：正式共享 authority 首選。
- MySQL 9+：正式共享 authority，同樣走 normalized schema 與 optimistic CAS。
- SQLite：單伺服器、開發與 PlugDev 驗收用途；共享 SQLite 檔案不適合作為生產多服資料庫。
- Redis 8+：L1/L2 之外的共享快取、Pub/Sub invalidation 與後續 outbox transport；Redis 不是權限 authority。

目前 Bolt 會在啟動時建立新的 schema。此分支不含舊 Bolt/LWC migration、export 或 import；正式部署前請準備全新的 SQL database/schema。

## PostgreSQL 18+

```yaml
database:
  type: postgres
  hostname: db.example.internal:5432
  database: bolt
  username: bolt
  password: change-me
  prefix: ""
  properties:
    sslmode: require
```

## MySQL 9+

```yaml
database:
  type: mysql
  hostname: db.example.internal:3306
  database: bolt
  username: bolt
  password: change-me
  prefix: ""
  properties:
    useSSL: "true"
```

## SQLite（單服／測試）

```yaml
database:
  type: sqlite
  path: plugins/Bolt/bolt.db
  prefix: ""
  properties: { }
```

## Redis 8+

```yaml
redis:
  enabled: true
  required: true
  uri: rediss://:password@redis.example.internal:6380
  namespace: "bolt-production:"
  network-id: survival-network
  server-id: survival-01
```

同一個 Velocity/Velocity-CTD 網路的每個 backend 必須使用相同 `database.*` authority、Redis `namespace` 與 `network-id`，但使用不同的 `server-id`。每個後端也必須使用相同的邏輯 world name；world identity registry／watermark worker 是下一階段補強項目。

## 一致性規則

1. 寫入先提交 SQL transaction，成功後才更新 Redis 與發布 invalidation。
2. 其他後端收到 invalidation 後清除較舊 L1；L2 miss 會回源 SQL。
3. Redis 故障不能取代 SQL 判定；required 模式會拒絕啟動，非 required 模式退回單服 SQL/L1 行為。
4. shared SQLite 不視為跨服 production topology；要跨服請使用 PostgreSQL 或 MySQL。
