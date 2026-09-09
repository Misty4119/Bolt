# 驗收紀錄

日期：2026-09-09（Asia/Taipei）

## Java 驗證

以下命令通過：

```text
./gradlew.bat :bolt-common:test
./gradlew.bat :bolt-bukkit:test
./gradlew.bat :bolt-bukkit:compileJava :bolt-common:test
./gradlew.bat :bolt-bukkit:test :bolt-common:test :bolt-bukkit:shadowJar
```

最後一次 shadow artifact：`bukkit/build/libs/Bolt-1.3.31.jar`。

涵蓋 SQL dialect/schema、SQLite round trip、Redis codec/cache invalidation、optimistic version、漏斗方向權限、PortalProtectionPolicy 與 MiniMessage renderer 契約。

## PlugDev 網路驗收

測試工具：`C:\Users\margo\IdeaProjects\plugdev-main`

拓撲：

```text
Velocity-CTD :25565
├─ Canvas 26.2 canvas-a :25566 / Bolt server-id=canvas-a
└─ Canvas 26.2 canvas-b :25567 / Bolt server-id=canvas-b
   ├─ shared SQLite test schema
   └─ Redis 8.10.1 :6389 / namespace bolt-e2e:
```

使用 fixture：

- `plugdev-bolt-network.yml`
- `plugdev-bolt-canvas-a.yml`
- `plugdev-bolt-canvas-b.yml`

Mineflayer 26.2 bot 已驗收：

- 玩家 `/bolt lock` 建立 protection，第二 backend 的 admin collection reconciliation 可見。
- owner 經 proxy 切換到 canvas-b 後，Redis L2 hydration 成功，`/bolt info` 與 Sneak + right-click protection GUI 可用。
- 非 OP 玩家無法開啟受保護箱子。
- 受保護 Nether portal frame 在 `BlockIgniteEvent` 路徑被阻止生成 portal。
- Hopper private protection 拒絕 automated insert；public protection 允許 automated insert。
- 從本專案 `config.yml` 解析出的 20 個 literal protectable blocks 全部成功 setblock、玩家互動與 force lock。

## PlugDev 自身測試

`npm test` 通過：CLI 296 tests、MCP 4 tests、testkit 5 passed，3 skipped。Skipped 是 PostgreSQL、MySQL、Redis container contract，因本機 Docker daemon unavailable 且沒有外部服務 URL；這不是 Bolt 程式測試失敗，仍需在 CI 或有 PG18/MySQL9/Redis8 服務的環境補跑。

Canvas 26.2 本身留下 OSHI Windows registry、SIMD suggestion 與 `/data get block` NPE 警告；這些不是 Bolt exception。漏斗內容驗收改由 Mineflayer 實際開啟容器並讀取 container slots。

## 已知尚未完成

- 本地驗收使用 shared SQLite 只驗證 schema／跨服流程；不能宣稱已完成 PostgreSQL 18 或 MySQL 9 真實 server acceptance。
- 本輪 block matrix 是 literal config entries；tag 展開、End Portal、活塞／流體／爆炸完整組合仍需按後續 matrix 逐項補測。
