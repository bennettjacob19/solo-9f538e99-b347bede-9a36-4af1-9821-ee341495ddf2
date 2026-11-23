# 分区盘演练 API（FastAPI + SQLite）

模拟一个 **4 物理区 / 3 逻辑组** 的 append-only 分区盘，支持按块号读写定长数据、
查看物理槽位占用，并内置满区搬迁（compaction）与崩溃恢复。交互式文档：
服务启动后访问 `http://localhost:8000/docs`（Swagger UI）。

## 1. 物理布局与规则

| 角色 | 数量 | 说明 |
| --- | --- | --- |
| 物理区 region | 4 | 大小相同，每区 `SLOTS_PER_REGION` 个定长槽位 |
| 逻辑组 group | 3 | 组号 `0..2`，每组固定块号 `0..SLOTS_PER_REGION-1`，各占一个 active 区 |
| 搬迁备用区 spare | 1 | 初始为空，满区搬迁时作为目标区 |

核心规则（与题目一一对应）：

1. **尾部追加**：写入只向本组 active 区的下一个空槽位 INSERT，物理槽位永不原地更新；
   读取取 `(组, 块)` 序列号最大的记录，因此覆盖写 = 追加一条新版本。
2. **整区重置才释放槽位**：搬迁收尾时对源区 `DELETE` 全部记录并翻转为 spare，
   槽位计数归零；此外没有任何回收路径（`/admin` 仅供演练）。
3. **满区覆盖也能写入**：active 区满后再写（即使区内全是有效块），系统把 spare
   区切成搬迁目标，**触发写入的新值第一个落盘**，再逐块拷贝源区有效块的最新版本，
   最后整区重置源区。搬迁完成后新区 active、源区 spare，始终保持一个空区。
4. **搬迁不阻塞读、不阻塞其他组**：搬迁分多步执行，每步一个独立事务并提交，
   步间释放写锁。已落盘的块立即可读；其他组在自己的槽位未满时照常写入。
   本组在搬迁期间写入返回 `503 + Retry-After`，客户端重试即可。
5. **崩溃安全**：
   - 每一步搬迁都已 `COMMIT`，停机不丢已确认写入，重启后仍可读到新值；
   - 未完成的搬迁状态持久化在 `migration` 表，重启后可 `POST /migrations/resume`
     幂等续跑（重复执行已完成的步骤不会产生重复槽位）；
   - 若恰好停在 finalize 事务中途（源记录已删、状态未翻转），启动时 `recover()`
     自动补完。

### 区状态机

```
unused ──首次写入──▶ active ──启动搬迁──▶ migrating_src（冻结只读）
                                   ▲              │ 拷贝完成
spare ◀────整区重置（释放全部槽位）──────────────┘
spare ──启动搬迁──▶ migrating（目标区，pending 先落盘）──完成──▶ active
```

## 2. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/config` | 布局参数（区数、组数、每区槽位、定长块大小） |
| GET | `/health` | 健康检查（Docker healthcheck 使用） |
| GET | `/groups/{gid}/blocks/{bid}` | 按组号+块号读取最新值（base64 + ASCII 表示） |
| PUT | `/groups/{gid}/blocks/{bid}` | 写入定长数据；满区自动触发整区搬迁 |
| GET | `/regions` | 各物理区状态与槽位占用（used/free/有效块数） |
| GET | `/regions/{rid}` | 某物理区每个槽位的实际内容 |
| GET | `/migrations` | 当前搬迁进度（无搬迁时 `in_progress=false`） |
| POST | `/migrations/resume` | 重启后续跑中断的搬迁 |
| POST | `/admin/regions/{rid}/reset` | 【演练】整区重置回收为 spare |
| POST | `/admin/reset-all` | 【演练】清空全部数据 |

写入请求体（二选一）：

```json
{ "data": "abcdefgh" }
```
```json
{ "data_base64": "AAECAwQFBgcICQ==" }
```

原始字符串按 UTF-8 编码，字节数必须恰好等于 `BLOCK_SIZE`；二进制内容用 base64。

### 响应约定

- `200` 写入成功（`status=written` 普通追加，`status=migrated` 触发了搬迁）
- `400` 数据长度不符 / base64 非法 / 两个字段同时出现
- `404` 逻辑块尚无数据
- `422` 组号、块号、区号越界
- `409` 存储冲突（如已有搬迁进行中）
- `503` 本组搬迁中暂无 active 区，或 spare 被占用；带 `Retry-After: 1`

## 3. docker compose 启动

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

数据落在命名卷 `partition-data`（容器内 `/data/partition.db`），容器重建后数据保留。
环境变量（见 `docker-compose.yml`）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SLOTS_PER_REGION` | 8 | 每个物理区槽位数（= 每组块数） |
| `BLOCK_SIZE` | 16 | 每条块数据的固定字节数 |
| `DB_PATH` | /data/partition.db | SQLite 文件路径 |
| `MIGRATION_STEP_DELAY` | 0.05 | 搬迁步间延迟秒数，便于在 /docs 观察半搬迁；设 0 全速 |
| `ENABLE_ADMIN` | 1 | 置 0 关闭管理接口 |

不用 Docker 时：

```bash
pip install -r requirements.txt
uvicorn --factory app.main:create_app --host 0.0.0.0 --port 8000
```

## 4. 快速演练（/docs 或 curl）

```bash
# 1. 写满组0（默认每区 8 槽，这里用 SLOTS_PER_REGION=3 演示）
for i in 0 1 2; do
  curl -X PUT localhost:8000/groups/0/blocks/$i \
       -H 'Content-Type: application/json' -d '{"data":"aaaaaaaa"}'
done

# 2. 查看槽位占用：区1 used=3 free=0
curl localhost:8000/regions

# 3. 覆盖满区的块 0 -> 自动搬迁，返回 status=migrated
curl -X PUT localhost:8000/groups/0/blocks/0 \
     -H 'Content-Type: application/json' -d '{"data":"zzzzzzzz"}'

# 4. 搬迁后：新区 active 且只含有效块，旧区整区重置为 spare（used=0）
curl localhost:8000/regions
curl localhost:8000/groups/0/blocks/0     # 读到 zzzzzzzz
```

## 5. 崩溃恢复演练

```bash
# 制造半搬迁（只跑第一步就退出）
python - <<'PY'
from app.store import PartitionStore
s = PartitionStore("/data/partition.db", 3, 8)
for b in range(3): s.try_write(0, b, b"a"*8)
s.start_migration(0, 0, b"z"*8)
print(s.migration_step())   # pending 新值已落盘并 COMMIT
s.close()                   # 模拟停机
PY

# 重启服务：新值仍可读，搬迁显示 in_progress=true
curl localhost:8000/groups/0/blocks/0
curl localhost:8000/migrations

# 续跑（也可直接重试原覆盖写入，接口会引导重试）
curl -X POST localhost:8000/migrations/resume
```

## 6. 存储设计要点

- **WAL + `synchronous=FULL` + `BEGIN IMMEDIATE`**：每步搬迁独立事务，提交即持久化。
- `records` 表只增不改；`(group_id, block_id, seq)` 索引保证读路径取最新版本。
- `migration` 单行表记录源/目标区、触发写入（pending）及拷贝进度；
  续跑时用“目标区已有的块号集合”判断进度，天然幂等。
- 应用层单把 `asyncio.Lock` 串行化写操作；读不加锁，搬迁步间让出事件循环。

## 7. 测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

覆盖：append-only 不原地更新、定长校验、满区搬迁（全有效块/含历史版本）、
每一步停机后的重启恢复、finalize 中途崩溃自愈、停机后重试覆盖、
搬迁中其他组可写与本组 503、spare 占用约束、管理接口与全部 HTTP 校验。
