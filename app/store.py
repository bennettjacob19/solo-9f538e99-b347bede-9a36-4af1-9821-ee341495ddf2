"""分区盘存储引擎。

物理模型
========
* 4 个等大物理区（region），每区 ``slots_per_region`` 个定长槽位。
* 3 个逻辑组（group 0..2），每组块号 0..slots_per_region-1，各占一个 active 区。
* 始终保留 1 个 spare 区，用于满区搬迁。

不变量
======
* 记录只能尾部追加（INSERT），物理槽位永不原地更新；读取取 (group, block)
  序列号最大的那条记录。
* 只有整区重置（搬迁收尾时的 DELETE + 状态翻转）才能释放槽位。
* 搬迁每一步都是独立事务，提交后即持久化，因此：
  - 已拷贝 / 已写入的块在搬迁中途即可读到新值；
  - 任意时刻停机，重启后状态都自洽，搬迁可幂等续跑。

区状态机：unused -> active -> migrating -> (源区) reset -> spare
          unused/spare 可在首次写入时被分配为 active
"""

from __future__ import annotations

import sqlite3
from typing import Any

REGION_COUNT = 4
GROUP_COUNT = 3

# 区状态
ST_UNUSED = "unused"
ST_SPARE = "spare"
ST_ACTIVE = "active"
ST_MIGRATING = "migrating"          # 搬迁目标区（新区）
ST_MIGRATING_SRC = "migrating_src"  # 搬迁源区（旧区，冻结只读）
ST_RESET = "reset"  # 仅在 finalize 事务内瞬间出现


class StoreError(Exception):
    """存储层基础异常。"""


class RegionFull(StoreError):
    """目标区槽位已满。"""


class NoSpare(StoreError):
    """没有可用的 spare 区，无法启动搬迁。"""


class NoActiveRegion(StoreError):
    """该组当前没有可写的 active 区（搬迁进行中）。"""


class NoMigration(StoreError):
    """当前没有进行中的搬迁。"""


class PartitionStore:
    """分区盘引擎；所有方法内部自带事务，线程安全依赖外层串行化。"""

    def __init__(
        self,
        db_path: str,
        slots_per_region: int,
        block_size: int,
    ) -> None:
        if slots_per_region <= 0 or block_size <= 0:
            raise ValueError("slots_per_region 与 block_size 必须为正数")
        self._slots = int(slots_per_region)
        self._block_size = int(block_size)
        # check_same_thread=False：FastAPI 端点线程与 lifespan 线程不同；
        # 写操作由应用层 asyncio.Lock 串行化，读为单条短事务，安全
        self._conn = sqlite3.connect(
            db_path, isolation_level=None, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    # ------------------------------------------------------------------ 基础
    def close(self) -> None:
        self._conn.close()

    @property
    def slots_per_region(self) -> int:
        return self._slots

    @property
    def block_size(self) -> int:
        return self._block_size

    def _init_schema(self) -> None:
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            # 注意：不能用 executescript，它会隐式提交当前事务
            for ddl in (
                """
                CREATE TABLE IF NOT EXISTS config (
                    key   TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS regions (
                    region_id INTEGER PRIMARY KEY,
                    status    TEXT NOT NULL,
                    group_id  INTEGER
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS records (
                    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
                    region_id  INTEGER NOT NULL REFERENCES regions(region_id),
                    slot_index INTEGER NOT NULL,
                    group_id   INTEGER NOT NULL,
                    block_id   INTEGER NOT NULL,
                    data       BLOB NOT NULL
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_records_lookup"
                " ON records(group_id, block_id, seq)",
                "CREATE INDEX IF NOT EXISTS idx_records_region"
                " ON records(region_id, slot_index)",
                """
                CREATE TABLE IF NOT EXISTS migration (
                    id                    INTEGER PRIMARY KEY CHECK (id = 1),
                    src_region_id         INTEGER NOT NULL REFERENCES regions(region_id),
                    dst_region_id         INTEGER NOT NULL REFERENCES regions(region_id),
                    group_id              INTEGER NOT NULL,
                    pending_block_id      INTEGER NOT NULL,
                    pending_data          BLOB NOT NULL,
                    pending_applied       INTEGER NOT NULL DEFAULT 0,
                    last_copied_block_id  INTEGER
                )
                """,
            ):
                c.execute(ddl)
            # 配置持久化：库已存在时以库内参数为准，保证重启布局不变
            cfg = {
                r["key"]: r["value"]
                for r in c.execute("SELECT key, value FROM config").fetchall()
            }
            if not cfg:
                c.executemany(
                    "INSERT INTO config(key, value) VALUES(?, ?)",
                    [
                        ("slots_per_region", self._slots),
                        ("block_size", self._block_size),
                    ],
                )
            else:
                self._slots = int(cfg["slots_per_region"])
                self._block_size = int(cfg["block_size"])

            if c.execute("SELECT COUNT(*) FROM regions").fetchone()[0] == 0:
                # 0 号区作为初始 spare，其余三个等待分配给三个组
                c.execute(
                    "INSERT INTO regions(region_id, status, group_id) VALUES (0, ?, NULL)",
                    (ST_SPARE,),
                )
                for rid in range(1, REGION_COUNT):
                    c.execute(
                        "INSERT INTO regions(region_id, status, group_id) VALUES (?, ?, NULL)",
                        (rid, ST_UNUSED),
                    )
            c.execute("COMMIT")
        except Exception:
            self._rollback_safely()
            raise

    def _check_data(self, data: bytes) -> None:
        if not isinstance(data, (bytes, bytearray)):
            raise StoreError("data 必须是 bytes")
        if len(data) != self._block_size:
            raise StoreError(
                f"数据长度必须等于 BLOCK_SIZE={self._block_size} 字节，"
                f"实际 {len(data)} 字节"
            )

    def _rollback_safely(self) -> None:
        try:
            self._conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass  # 事务已不存在（如隐式提交），原始异常照常向上抛

    def _append(self, region_id: int, group_id: int, block_id: int, data: bytes) -> dict:
        """在指定区尾部追加一条记录（调用方需已开启事务）。"""
        used = self._conn.execute(
            "SELECT COUNT(*) FROM records WHERE region_id=?", (region_id,)
        ).fetchone()[0]
        if used >= self._slots:
            raise RegionFull(f"物理区 {region_id} 槽位已满")
        cur = self._conn.execute(
            "INSERT INTO records(region_id, slot_index, group_id, block_id, data)"
            " VALUES (?, ?, ?, ?, ?)",
            (region_id, used, group_id, block_id, sqlite3.Binary(bytes(data))),
        )
        return {"seq": cur.lastrowid, "region_id": region_id, "slot_index": used}

    def _find_active(self, group_id: int) -> int | None:
        row = self._conn.execute(
            "SELECT region_id FROM regions WHERE group_id=? AND status=?",
            (group_id, ST_ACTIVE),
        ).fetchone()
        return None if row is None else row[0]

    # ------------------------------------------------------------------ 读路径
    def read_latest(self, group_id: int, block_id: int) -> dict[str, Any] | None:
        """返回逻辑块的最新值；搬迁期间新区已落盘的记录会自然胜出。"""
        row = self._conn.execute(
            "SELECT seq, region_id, slot_index, data FROM records"
            " WHERE group_id=? AND block_id=? ORDER BY seq DESC LIMIT 1",
            (group_id, block_id),
        ).fetchone()
        if row is None:
            return None
        return {
            "seq": row["seq"],
            "region_id": row["region_id"],
            "slot_index": row["slot_index"],
            "data": bytes(row["data"]),
        }

    def region_usage(self) -> list[dict[str, Any]]:
        """各区槽位占用：used=已追加槽位（含历史版本），valid=当前有效逻辑块数。"""
        rows = self._conn.execute(
            """
            SELECT r.region_id, r.status, r.group_id,
                   (SELECT COUNT(*) FROM records rec
                      WHERE rec.region_id = r.region_id) AS used_slots,
                   (SELECT COUNT(DISTINCT rec.block_id) FROM records rec
                      WHERE rec.region_id = r.region_id) AS valid_blocks
              FROM regions r
             ORDER BY r.region_id
            """
        ).fetchall()
        return [
            {
                "region_id": r["region_id"],
                "status": r["status"],
                "group_id": r["group_id"],
                "used_slots": r["used_slots"],
                "total_slots": self._slots,
                "free_slots": self._slots - r["used_slots"],
                "valid_blocks": r["valid_blocks"],
            }
            for r in rows
        ]

    def region_records(self, region_id: int) -> list[dict[str, Any]]:
        """查看某物理区每个槽位的实际内容（演练可视化用）。"""
        rows = self._conn.execute(
            "SELECT seq, slot_index, group_id, block_id, data FROM records"
            " WHERE region_id=? ORDER BY slot_index",
            (region_id,),
        ).fetchall()
        return [
            {
                "seq": r["seq"],
                "slot_index": r["slot_index"],
                "group_id": r["group_id"],
                "block_id": r["block_id"],
                "data": bytes(r["data"]),
            }
            for r in rows
        ]

    # ------------------------------------------------------------------ 写路径
    def try_write(self, group_id: int, block_id: int, data: bytes) -> dict[str, Any]:
        """尝试在本组 active 区尾部追加。

        返回 ``{"status": "written", ...}`` 或 ``{"status": "full", ...}``；
        若该组没有 active 区（搬迁中）抛 :class:`NoActiveRegion`。
        首次写入的组会自动分配一个 unused 区。
        """
        self._check_data(data)
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            region_id = self._find_active(group_id)
            if region_id is None:
                # 若本组正在搬迁，其源区已冻结、新区未转正：没有可写区。
                # unused 区是留给其他组的，绝不能挪用，否则破坏
                # “一组一区、一个 spare”的布局不变量。
                m = c.execute(
                    "SELECT 1 FROM migration WHERE id=1 AND group_id=?",
                    (group_id,),
                ).fetchone()
                if m is not None:
                    self._rollback_safely()
                    raise NoActiveRegion(f"组 {group_id} 搬迁进行中，暂无 active 区")
                unused = c.execute(
                    "SELECT region_id FROM regions WHERE status=? LIMIT 1",
                    (ST_UNUSED,),
                ).fetchone()
                if unused is None:
                    self._rollback_safely()
                    raise NoActiveRegion(f"组 {group_id} 暂无可写物理区")
                region_id = unused[0]
                c.execute(
                    "UPDATE regions SET status=?, group_id=? WHERE region_id=?",
                    (ST_ACTIVE, group_id, region_id),
                )
            used = c.execute(
                "SELECT COUNT(*) FROM records WHERE region_id=?", (region_id,)
            ).fetchone()[0]
            if used >= self._slots:
                self._rollback_safely()
                return {"status": "full", "region_id": region_id}
            meta = self._append(region_id, group_id, block_id, data)
            c.execute("COMMIT")
            return {"status": "written", **meta}
        except Exception:
            self._rollback_safely()
            raise

    # ------------------------------------------------------------------ 搬迁
    def get_migration(self) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM migration WHERE id=1").fetchone()
        if row is None:
            return None
        dst_used = self._conn.execute(
            "SELECT COUNT(*) FROM records WHERE region_id=?", (row["dst_region_id"],)
        ).fetchone()[0]
        src_blocks = self._conn.execute(
            "SELECT COUNT(DISTINCT block_id) FROM records WHERE region_id=?",
            (row["src_region_id"],),
        ).fetchone()[0]
        return {
            "src_region_id": row["src_region_id"],
            "dst_region_id": row["dst_region_id"],
            "group_id": row["group_id"],
            "pending_block_id": row["pending_block_id"],
            "pending_applied": bool(row["pending_applied"]),
            "last_copied_block_id": row["last_copied_block_id"],
            "dst_used_slots": dst_used,
            "src_distinct_blocks": src_blocks,
        }

    def start_migration(
        self, group_id: int, block_id: int, data: bytes
    ) -> dict[str, Any]:
        """把 spare 区切成 migrating 目标区并登记触发写入；

        本组 active 区冻结为 migrating_src（只读），pending 新值由
        下一步 :meth:`migration_step` 落盘到目标区。
        """
        self._check_data(data)
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            migration_active = (
                c.execute("SELECT 1 FROM migration WHERE id=1").fetchone() is not None
            )
            src = self._find_active(group_id)
            if src is None:
                self._rollback_safely()
                raise NoActiveRegion(f"组 {group_id} 没有可写的 active 区")
            spare = c.execute(
                "SELECT region_id FROM regions WHERE status=?", (ST_SPARE,)
            ).fetchone()
            if spare is None:
                self._rollback_safely()
                # 迁移进行中 spare 必然被占用：调用方应稍后重试
                hint = "（当前正有其他搬迁占用 spare）" if migration_active else ""
                raise NoSpare(f"spare 区已被占用，无法启动搬迁{hint}")
            if migration_active:
                self._rollback_safely()
                raise StoreError("已有搬迁进行中")
            dst = spare[0]
            # spare 区必须是空区（整区重置后才会回到 spare）
            assert c.execute(
                "SELECT COUNT(*) FROM records WHERE region_id=?", (dst,)
            ).fetchone()[0] == 0
            # 源区冻结为只读搬迁源，新区接管写入（pending 第一步即落盘）
            c.execute(
                "UPDATE regions SET status=? WHERE region_id=?",
                (ST_MIGRATING_SRC, src),
            )
            c.execute(
                "UPDATE regions SET status=?, group_id=? WHERE region_id=?",
                (ST_MIGRATING, group_id, dst),
            )
            c.execute(
                "INSERT INTO migration(id, src_region_id, dst_region_id, group_id,"
                " pending_block_id, pending_data, pending_applied)"
                " VALUES (1, ?, ?, ?, ?, ?, 0)",
                (src, dst, group_id, block_id, sqlite3.Binary(bytes(data))),
            )
            c.execute("COMMIT")
            return {
                "src_region_id": src,
                "dst_region_id": dst,
                "group_id": group_id,
                "pending_block_id": block_id,
            }
        except Exception:
            self._rollback_safely()
            raise

    def migration_step(self) -> dict[str, Any]:
        """推进一步搬迁，每步一个事务，返回步骤描述；done 表示搬迁完成。

        步骤顺序（天然幂等，崩溃重放不会产生重复槽位）：
        1. pending：触发覆盖的新值先落盘到新区，立刻可读；
        2. copied：把源区一个逻辑块的最新版本追加到新区；
        3. done：源区整区重置（释放全部槽位），翻转 active/spare。
        """
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            m = c.execute("SELECT * FROM migration WHERE id=1").fetchone()
            if m is None:
                self._rollback_safely()
                raise NoMigration("没有进行中的搬迁")
            src, dst = m["src_region_id"], m["dst_region_id"]
            group_id, pending_block = m["group_id"], m["pending_block_id"]

            # 步骤 1：触发写入的新值优先落盘
            if not m["pending_applied"]:
                exists = c.execute(
                    "SELECT 1 FROM records WHERE region_id=? AND group_id=? AND block_id=?",
                    (dst, group_id, pending_block),
                ).fetchone()
                if exists is None:
                    meta = self._append(dst, group_id, pending_block, m["pending_data"])
                    seq = meta["seq"]
                else:
                    # 上一轮在 append 后、更新 pending_applied 前崩溃
                    seq = c.execute(
                        "SELECT MAX(seq) FROM records WHERE region_id=? AND block_id=?",
                        (dst, pending_block),
                    ).fetchone()[0]
                c.execute(
                    "UPDATE migration SET pending_applied=1 WHERE id=1"
                )
                c.execute("COMMIT")
                return {
                    "kind": "pending",
                    "group_id": group_id,
                    "block_id": pending_block,
                    "src_region_id": src,
                    "dst_region_id": dst,
                    "seq": seq,
                }

            # 步骤 2：拷贝源区里尚未出现在新区的下一个逻辑块（按块号升序，确定性）
            nxt = c.execute(
                """
                SELECT DISTINCT block_id FROM records
                 WHERE region_id=?
                   AND block_id NOT IN (
                       SELECT block_id FROM records
                        WHERE region_id=? AND group_id=?
                   )
                 ORDER BY block_id LIMIT 1
                """,
                (src, dst, group_id),
            ).fetchone()
            if nxt is not None:
                block_id = nxt[0]
                latest = c.execute(
                    "SELECT data FROM records WHERE region_id=? AND block_id=?"
                    " ORDER BY seq DESC LIMIT 1",
                    (src, block_id),
                ).fetchone()
                meta = self._append(dst, group_id, block_id, latest["data"])
                c.execute(
                    "UPDATE migration SET last_copied_block_id=? WHERE id=1",
                    (block_id,),
                )
                c.execute("COMMIT")
                return {
                    "kind": "copied",
                    "group_id": group_id,
                    "block_id": block_id,
                    "src_region_id": src,
                    "dst_region_id": dst,
                    **meta,
                }

            # 步骤 3：源区整区重置，槽位全部释放；新区转正，源区变 spare
            c.execute(
                "UPDATE regions SET status=?, group_id=NULL WHERE region_id=?",
                (ST_RESET, src),
            )
            c.execute("DELETE FROM records WHERE region_id=?", (src,))
            c.execute(
                "UPDATE regions SET status=? WHERE region_id=?", (ST_SPARE, src)
            )
            c.execute(
                "UPDATE regions SET status=? WHERE region_id=?", (ST_ACTIVE, dst)
            )
            c.execute("DELETE FROM migration WHERE id=1")
            c.execute("COMMIT")
            return {
                "kind": "done",
                "group_id": group_id,
                "src_region_id": src,
                "dst_region_id": dst,
            }
        except Exception:
            self._rollback_safely()
            raise

    def recover(self) -> dict[str, Any] | None:
        """重启恢复。

        正常进行中的搬迁保持原状（可 resume 续跑）；需要补动作的是
        finalize 事务中途停机：源区记录已删（可能恰好为零条，也可能还没
        来得及删完），状态还没翻转完。判断依据是 dst 是否仍处于 migrating。
        """
        m = self.get_migration()
        if m is None:
            return None
        dst_status = self._conn.execute(
            "SELECT status FROM regions WHERE region_id=?", (m["dst_region_id"],)
        ).fetchone()[0]
        if dst_status != ST_MIGRATING:
            # dst 已转正，说明 finalize 的状态翻转已落盘，仅需清理残留行
            c = self._conn
            c.execute("BEGIN IMMEDIATE")
            try:
                c.execute("DELETE FROM migration WHERE id=1")
                c.execute("COMMIT")
            except Exception:
                self._rollback_safely()
                raise
            return {"status": "finalized", **m}
        src_remaining = self._conn.execute(
            "SELECT COUNT(*) FROM records WHERE region_id=?", (m["src_region_id"],)
        ).fetchone()[0]
        if src_remaining > 0:
            return {"status": "interrupted", **m}
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute(
                "UPDATE regions SET status=? WHERE region_id=?",
                (ST_ACTIVE, m["dst_region_id"]),
            )
            c.execute(
                "UPDATE regions SET status=?, group_id=NULL WHERE region_id=?",
                (ST_SPARE, m["src_region_id"]),
            )
            c.execute("DELETE FROM migration WHERE id=1")
            c.execute("COMMIT")
            return {"status": "finalized", **m}
        except Exception:
            self._rollback_safely()
            raise

    # ------------------------------------------------------------------ 管理
    def admin_reset_region(self, region_id: int) -> None:
        """演练用：整区重置并回收为 spare（搬迁进行中禁止）。"""
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            if c.execute("SELECT 1 FROM migration WHERE id=1").fetchone() is not None:
                self._rollback_safely()
                raise StoreError("搬迁进行中，不能重置物理区")
            exists = c.execute(
                "SELECT 1 FROM regions WHERE region_id=?", (region_id,)
            ).fetchone()
            if exists is None:
                self._rollback_safely()
                raise StoreError(f"物理区 {region_id} 不存在")
            c.execute("DELETE FROM records WHERE region_id=?", (region_id,))
            c.execute(
                "UPDATE regions SET status=?, group_id=NULL WHERE region_id=?",
                (ST_SPARE, region_id),
            )
            c.execute("COMMIT")
        except Exception:
            self._rollback_safely()
            raise

    def admin_reset_all(self) -> None:
        """演练用：清空全部数据，恢复初始四区布局。"""
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute("DELETE FROM records")
            c.execute("DELETE FROM migration")
            c.execute("UPDATE regions SET status=?, group_id=NULL", (ST_UNUSED,))
            c.execute(
                "UPDATE regions SET status=? WHERE region_id=0", (ST_SPARE,)
            )
            c.execute("COMMIT")
        except Exception:
            self._rollback_safely()
            raise
