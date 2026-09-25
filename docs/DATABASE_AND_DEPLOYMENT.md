# Bolt 資料庫與跨伺服器部署

## 支援邊界

- PostgreSQL 18+：正式共享 authority 首選。
- MySQL 9+：正式共享 authority，同樣走 normalized schema 與 optimistic CAS。
- SQLite：單伺服器、開發與 PlugDev 驗收用途；共享 SQLite 檔案不適合作為生產多服資料庫。
- Redis 8+：L1/L2 之外的共享快取、Pub/Sub invalidation 與 durable outbox transport；Redis 不是權限 authority。

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
  # Shared network requires Sentinel; use rediss-sentinel:// for TLS.
  uri: rediss-sentinel://:password@sentinel-a:26379,sentinel-b:26379?sentinelMasterId=bolt-master
  namespace: "bolt-production:"
  network-id: survival-network
  server-id: survival-01
  server-group: survival
```

同一個 Velocity/Velocity-CTD 網路的每個 backend 必須使用相同 `database.*` authority、Redis `namespace`、`network-id` 與 `server-group`，但使用不同的 `server-id`。world UUID registry 會在 plugin 啟動及 WorldLoad 時註冊；watermark 與 durable outbox 由 SQL authority 驅動跨服修復。

## 一致性規則

1. 寫入先提交 SQL transaction，成功後才更新 Redis 與發布 invalidation。
2. 其他後端收到 invalidation 後清除較舊 L1；L2 miss 會回源 SQL。
3. Redis 故障不能取代 SQL 判定；單服可在 `redis.enabled=false` 下使用 SQL/L1，指定非預設 `network-id` 或 `server-id` 時則會強制 Redis 啟動成功。
4. shared SQLite 不視為跨服 production topology；要跨服請使用 PostgreSQL 或 MySQL。
5. network mode 建議使用至少兩個 Sentinel endpoint；Redis Sentinel URI 必須帶 `sentinelMasterId`，不會退回 standalone。

## 訊息色盤

`messages.palette` 支援 `light`／`dark`。色盤使用天空藍、淡紫、板岩灰與夜語白的 hex 色彩；既有 MiniMessage 語系中的 `<yellow>`、`<red>`、`<gray>` 等 legacy tags 會由 renderer 映射，不需要一次改寫所有語系檔。自訂翻譯若遺漏固定 placeholder，會被內建語系 fallback 保護。
