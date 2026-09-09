# Bolt 跨伺服器保護與資料層改造計劃

## 1. 目標與已確認決策

本計劃針對 Canvas 26.2+ / Folia 優先環境，將 Bolt 改造成可在 Velocity-CTD 網路中執行的保護系統。

已確認的產品決策：

- 支援 PostgreSQL 18+、MySQL 9+；SQLite 保留給單伺服器、測試與部署過渡用途。
- Redis 8+ 作為 L2 快取、跨服失效通知及可靠事件傳輸；Redis 永遠不是權限權威資料源。
- 權威資料源是 SQL；成功寫入 SQL transaction 後才向玩家回報成功。
- 跨服拓撲是同一個 Velocity/Velocity-CTD 網路、同一邏輯伺服器群組、共用 SQL + Redis。
- 本機 L1 快取 + Redis L2 快取 + SQL authority；正常情況下失效約 1 秒內傳播。
- 快取不確定或 SQL/Redis 無法確認時，鎖定、解鎖、ACL 變更及敏感保護操作 fail closed；普通非保護操作不受影響。
- 使用 record version / optimistic CAS，禁止 GUI 或跨服更新靜默覆蓋較新的 ACL。
- 保留既有 commands、events、permissions 與 API 行為；新增能力使用 additive API，必要時以 deprecation 過渡。
- 不保留舊 Bolt/LWC 的資料遷移、匯出、匯入功能；新 schema 以全新部署開始。這是明確的 breaking change。
- 第一版 GUI 是 Inventory GUI：保護資訊、擁有者、玩家/群組 ACL、細粒度權限；不把遠端倉庫與 Audit 全部塞入第一個畫面。
- 傳送門建立只要事件涉及任一受保護方塊，預設整個取消；擁有者與管理員第一版也沒有例外。

## 2. 現況基線

目前程式已有：

- `Store` 作為資料存取 seam，`SimpleProtectionCache` 作為全量本機快取。
- `SQLStore` 的 SQLite/MySQL 路徑與 JSON ACL 儲存。
- Adventure Component + MiniMessage 訊息管線。
- 多個方塊、容器、漏斗、紅石、活塞、爆炸、流體及 End Portal 防護事件。
- `PortalCreateEvent` 取消邏輯已從 Bolt 參考專案加入，並以 `anyMatch` 短路檢查。

目前主要缺口：

- SQLStore 將非 `mysql` 類型錯誤地當成 SQLite，沒有 PostgreSQL dialect。
- 單一 JDBC Connection、批次寫入與 `DriverManager` 連線不適合多伺服器併發。
- 本機快取沒有跨服失效通知、版本水位 reconciliation 與 fail-closed 狀態。
- ACL 仍是 protection 上的 JSON map，無法支援查詢、版本控制、群組成員與 audit 關聯。
- `zh.properties` 的 placeholder 契約不完整；所有語系必須保留 `<player>`、`<count>` 等固定 token。
- PlugDev 測試環境雖能啟動 Canvas/Velocity-CTD，但仍需加入兩個 backend、共享資料服務與可重複情境腳本。

工作區有大量既有使用者修改。本分支只提交本次功能與文件；不還原、覆蓋或混入無關既有修改。

## 3. 目標模組與 seam

```text
ProtectionService
  ├─ ProtectionRepository       SQL authority adapter
  ├─ CacheCoordinator           L1/L2 cache + version/invalidation
  ├─ AccessEvaluator            action model and precedence
  ├─ AuditRecorder              async outbox/batch writer
  └─ WorldIdentity              canonical UUID/name mapping

Platform adapter (Bukkit/Canvas/Folia)
  ├─ event listeners
  ├─ Inventory GUI
  ├─ MiniMessage renderer
  └─ scheduler / thread policy
```

外部測試只通過這些 seam：

1. `ProtectionRepository`：建立、讀取、更新、刪除與 CAS 版本結果。
2. `CacheCoordinator`：命中、失效、重建、Redis/SQL 故障行為。
3. `AccessEvaluator`：action、owner/member/group/admin、deny precedence。
4. `PortalProtectionPolicy`：事件方塊集合的取消判定。
5. `AuditRecorder`：可靠事件接收、批次提交與 retention。
6. 平台事件/指令/GUI：玩家可觀察的結果與原有相容行為。

每個 adapter 要能以 in-memory fake 替換；測試不得依賴私有欄位、資料庫實作細節或重新計算 production 結果。

## 4. 資料模型與 SQL

### 4.1 共用欄位

所有資料表使用：

- `schema_version`：啟動時由內部 migration runner 管理。
- `created_at`、`updated_at`：UTC epoch milliseconds 或 dialect 等價型別。
- `version`：從 1 開始遞增，供 optimistic CAS。
- UUID 使用字串/原生 UUID 的 dialect adapter，不讓 domain model 綁定資料庫。

### 4.2 主要資料表

- `bolt_worlds`：canonical `world_id`、world UUID、名稱、server group；同一 UUID 與名稱若映射不一致則拒絕啟動/寫入。
- `bolt_block_protections`：protection UUID、world ID、x/y/z、block type、owner UUID、protection type、timestamps、version、accessed time。
- `bolt_entity_protections`：entity UUID、type、owner、protection type、timestamps、version。
- `bolt_access_entries`：protection UUID、subject type、subject ID、action、effect、version。
- `bolt_groups`：group ID/name/owner/version。
- `bolt_group_members`：group ID、player UUID、role、version。
- `bolt_access_lists`：owner UUID、全域 ACL version/update metadata。
- `bolt_access_list_entries`：全域 ACL subject 与 access type 的 normalized entries。
- `bolt_hopper_rules`：protection UUID、direction、match kind/value、effect、priority、quantity limit、version。
- `bolt_audit_events`：actor/source、source type、action、target、world/position、item delta、metadata、created_at。
- `bolt_outbox`：event UUID、event type、aggregate ID/version、payload、published/attempted timestamps。

所有 protection location、owner、aggregate/version、audit retention 查詢建立索引；外鍵與 unique constraint 防止重複 ACL、群組成員與 world mapping。

### 4.3 dialect 與 migration

- `SqlDialect` 提供 placeholder、upsert、boolean、timestamp、UUID、DDL 差異。
- 分開提供 PostgreSQL、MySQL、SQLite schema/migration resources。
- 內部 migration runner 只做非破壞性版本升級，不提供舊 Bolt/LWC 匯入。
- PG/MySQL 使用 HikariCP；SQLite 使用單執行緒、單連線策略。
- 所有寫入在 repository 內完成 transaction；CAS 失敗回傳 `CONFLICT`，不能當成成功。
- 啟動時檢查資料庫產品、版本、schema 版本與 world identity；失敗時以清楚訊息停止 Bolt。

## 5. Redis 設計

Redis 8+ 支援 standalone 與 Sentinel；Cluster 留作後續版本。連線支援 TLS、ACL/password、timeout、namespace。

建議 key/channel：

- `bolt:{network}:cache:protection:{id}`：L2 protection 快取，value 包含 payload、SQL version、expiry。
- `bolt:{network}:cache:world:{worldId}`：canonical world identity。
- `bolt:{network}:invalidate`：Pub/Sub；事件包含 `serverId`、aggregate type/id、SQL version、operation、event ID。
- `bolt:{network}:audit`：後續使用 Redis Streams；不可把 Pub/Sub 當成唯一 audit delivery guarantee。
- `bolt:{network}:health`：可選節點健康與 server presence，不參與權限判定。

流程：

1. 讀取先查 L1；miss 查 L2；再 miss 查 SQL，成功後填入 L2/L1。
2. 寫入以 SQL transaction 為先，成功後發布 invalidation；本機先更新或失效自己的 L1。
3. 其他服收到 invalidation 後比較 version，只能刪除/更新較舊資料。
4. 定期以 SQL `updated_at`/version watermark reconciliation 修復 Pub/Sub 丟失。
5. Redis 不可用時，跨服安全操作拒絕；單服也不使用未確認的共享資料。

## 6. 權限與事件策略

### 6.1 action model

首批 action：`OPEN`、`INSERT`、`EXTRACT`、`INTERACT`、`BREAK`、`EDIT`、`REDSTONE`、`HOPPER_INSERT`、`HOPPER_EXTRACT`、`MOUNT`。

判定順序：

1. 明確 deny。
2. 擁有者。
3. protection ACL 的玩家/群組允許。
4. 擁有者全域 access list。
5. 管理員 bypass；必須寫 Audit。
6. plugin permission fallback（保留現有權限節點）。

所有同步判定必須在非主執行緒準備資料，但 Bukkit/Canvas 物件存取要回到正確 scheduler。事件在無法確認權限時取消。

### 6.2 漏斗與紅石

- 漏斗輸入/輸出獨立授權。
- 物品過濾支援 Material、Tag、數量限制；NBT/複合 predicate 延後。
- 未授權漏斗在 target validation 與 inventory transport 兩個 seam 都拒絕。
- 不實作不可靠的「按鈕玩家追溯」；紅石使用 `REDSTONE` 權限與來源白名單。

### 6.3 傳送門

保護檢查層次：

1. `PortalCreateEvent#getBlocks()` 任一新建 block state 對應保護即取消。
2. 點火/方塊變更前檢查 protected frame/target 幾何範圍。
3. End Portal frame 插眼時檢查整個 frame 與中心區域。
4. 之後的 block physics、fluid、explosion、piston 路徑繼續套用一般 protection evaluator。

第一版任何操作者都不能繞過；後續若要例外，必須是明確 permission 並留下 audit。

## 7. GUI、訊息與視覺回饋

- MiniMessage 只在 Adventure renderer 的最後一步 deserialize；翻譯檔保存固定 placeholder 名稱。
- GUI 使用 Inventory API，所有按鈕動作再次走 `AccessEvaluator`，不信任 inventory slot 狀態。
- 第一版 GUI：保護資訊、owner、玩家搜尋/加入/移除、群組、action checkboxes、上一頁/下一頁、衝突提示。
- Audit GUI 顯示最近 100 筆並分頁；遠端倉庫/Web dashboard 不列入第一個可用版本。
- Glow/packet 視覺回饋列為後續可選模組；若 Canvas API 能提供無外掛封包路徑，優先使用該 adapter。

## 8. PlugDev 驗證拓撲

測試工具：`C:\Users\margo\IdeaProjects\plugdev-main`。

目標情境：

```text
Velocity-CTD :25565
  ├─ Canvas backend-a :25566 (server-id=a)
  └─ Canvas backend-b :25567 (server-id=b)
       ├─ PostgreSQL 18+ / MySQL 9+ shared authority
       └─ Redis 8+ shared cache/pubsub
```

PlugDev fixture 需要：

- network config、Velocity forwarding secret、兩個 backend 的 Bolt plugin JAR。
- 每個 backend 不同 `server-id`，相同 `network-id`、SQL 與 Redis namespace。
- 測試玩家 A/B、管理員、非授權玩家；可用 BOT 或兩個客戶端。
- 可重複的 world/coordinate fixture，涵蓋所有 `config.yml` protectable block 與 matcher。
- RCON/console command 驅動 setup、assert、cleanup；不依賴人工點擊作為唯一驗收。

自動化測試層：

1. Java unit：action evaluator、dialect、CAS、portal policy、MiniMessage placeholder。
2. Java platform tests：事件 listeners、inventory transport、GUI action。
3. SQL integration：PG/MySQL/SQLite schema、transaction、concurrency、reconnect。
4. Redis integration：L1/L2、invalidations、reconciliation、standalone/Sentinel、故障。
5. PlugDev E2E：雙 Canvas 服經 Velocity-CTD 互相建立/修改/讀取/撤銷保護，檢查跨服約 1 秒內生效。
6. Block matrix：容器、門、活塞、流體、火、爆炸、紅石、漏斗、Nether Portal、End Portal 及 matcher 連體方塊。
7. Failure matrix：SQL down、Redis down、Pub/Sub 丟失、CAS conflict、backend restart、proxy reconnect。

驗收門檻：

- `compileJava`、`build`、全部單元/整合測試通過。
- 權限判定不阻塞主執行緒；快取命中不開 JDBC。
- 安全操作在資料不確定時一定拒絕，不能因快取 stale 而放行。
- 跨服 invalidation 正常狀態約 1 秒內完成，reconciliation 可修復事件遺失。
- 所有測試情境可由 PlugDev 重跑並產出 log/artifact。

## 9. 分層實作與 commit 計劃

每一層採「測試先行 → 最小實作 → 驗證 → commit」。commit 只包含該層檔案，不用 `git add .`，以免混入既有工作區修改。

1. `docs: record cross-server protection architecture`
   - 本文件、domain vocabulary、測試契約與 deployment notes。
2. `test: add portal policy and storage seam coverage`
   - 先建立傳送門、CAS、故障行為測試 seam。
3. `feat: add sql dialect and versioned schema`
   - Repository、PG/MySQL/SQLite dialect、Hikari、schema runner；移除 runtime 對舊 JSON schema 的依賴。
4. `feat: add redis cache coordinator and invalidation`
   - Redis client、L1/L2、Pub/Sub、watermark reconciliation、fail-closed。
5. `feat: add action evaluator and reliable portal guards`
   - action model、deny precedence、完整 portal/frame/target guard、audit hook。
6. `feat: add hopper rules and inventory gui`
   - 漏斗方向/物品規則、GUI 與 conflict handling。
7. `feat: add audit outbox and retention`
   - audit schema、async batch、outbox/stream、GUI 分頁與非玩家 source。
8. `test: add plugdev velocity ctd canvas network matrix`
   - PlugDev fixture、雙 backend、資料服務、block matrix、故障測試與報告。
9. `docs: finalize operations and breaking changes`
   - 設定範例、升級/全新部署說明、Redis/SQL 運維、MiniMessage 語系契約。

## 10. 合併與發布流程

1. 在本地功能分支完成全部分層 commit。
2. 每個 commit 執行最小相關驗證；最終執行完整 Gradle + PlugDev matrix。
3. 檢查 `git diff --check`、工作區是否只剩明確既有修改、測試 artifact 與 secrets 未入 Git。
4. 產生 review diff 與 PR 說明，確認 breaking change、資料庫需求與無 migration 的風險已列出。
5. 先將分支推送 GitHub，建立 PR 到 `main`。
6. PR 驗收完成且所有檢查通過後才合併 `main`；合併後更新本地 `main` 並確認遠端狀態。

本次執行不得在測試未完成、PR 未通過或偵測到未授權資料破壞時直接推送/合併。

## 11. 風險與處理

- Canvas 26.2 API 可能與標準 Paper/Folia API 有差異：所有平台差異集中在 adapter，不在 domain module 擴散。
- MySQL Connector/J 採 shade 前需檢查授權；若不能合併進 JAR，改成明確 runtime driver package 並在啟動訊息提示。
- Redis Pub/Sub 不是可靠佇列：權限失效靠 SQL watermark 修復，Audit 另用 outbox/Streams。
- 全新 schema 會造成舊資料不可用：啟動拒絕舊 schema 並在 README/PR 明確警告，不自動刪除或轉換資料。
- 大量既有工作區修改增加合併風險：只選取本次檔案，必要時以 patch-level commit 保持 locality。
