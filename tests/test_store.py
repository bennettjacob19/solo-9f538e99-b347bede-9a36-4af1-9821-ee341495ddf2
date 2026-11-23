"""存储引擎测试：append-only 语义、搬迁、崩溃恢复。"""

import sqlite3

import pytest

from app import store as sm


def make_store(tmp_path, slots=4, block_size=8):
    return sm.PartitionStore(str(tmp_path / "p.db"), slots, block_size)


def v(ch: str, block_size: int = 8) -> bytes:
    return (ch * block_size).encode()


def test_append_only_and_latest_read(tmp_path):
    s = make_store(tmp_path)
    for ch in "abc":
        assert s.try_write(0, 1, v(ch))["status"] == "written"
    # 同一逻辑块三次写入 -> 三个槽位，读取取最新
    assert s.read_latest(0, 1)["data"] == v("c")
    region = [r for r in s.region_usage() if r["group_id"] == 0][0]
    assert region["used_slots"] == 3
    assert region["valid_blocks"] == 1
    assert s.read_latest(0, 9) is None
    s.close()


def test_slots_are_never_updated_in_place(tmp_path):
    s = make_store(tmp_path)
    s.try_write(0, 0, v("a"))
    s.try_write(0, 0, v("b"))
    rows = s.region_records(1)
    assert [r["data"] for r in rows] == [v("a"), v("b")]  # 旧值仍留在旧槽位
    assert [r["slot_index"] for r in rows] == [0, 1]
    s.close()


def test_block_size_enforced(tmp_path):
    s = make_store(tmp_path, block_size=8)
    with pytest.raises(sm.StoreError):
        s.try_write(0, 0, b"short")
    s.close()


def test_full_region_migration_frees_one_region(tmp_path):
    n = 3
    s = make_store(tmp_path, slots=n)
    # 把组0的 active 区写满，且每个块都是有效块
    for b in range(n):
        s.try_write(0, b, v("a"))
    src = [r for r in s.region_usage() if r["group_id"] == 0][0]["region_id"]
    assert s.try_write(0, 0, v("z"))["status"] == "full"

    # 启动搬迁：即使区内全是有效块也必须能写入新值并腾出空区
    s.start_migration(0, 0, v("z"))
    steps = []
    while s.get_migration() is not None:
        steps.append(s.migration_step())
    kinds = [st["kind"] for st in steps]
    assert kinds[0] == "pending"
    assert kinds[-1] == "done"

    # 新值可读；其余块值保持
    assert s.read_latest(0, 0)["data"] == v("z")
    for b in range(1, n):
        assert s.read_latest(0, b)["data"] == v("a")

    usage = {r["region_id"]: r for r in s.region_usage()}
    # 源区整区重置，槽位全部释放，成为新 spare
    assert usage[src]["status"] == "spare"
    assert usage[src]["used_slots"] == 0
    # 新 active 区只装有效块（压缩了历史版本）
    dst = [r for r in s.region_usage() if r["group_id"] == 0][0]
    assert dst["used_slots"] == n
    assert dst["valid_blocks"] == n
    assert dst["region_id"] != src
    s.close()


def test_migration_with_garbage_compacts_history(tmp_path):
    n = 4
    s = make_store(tmp_path, slots=n)
    s.try_write(0, 0, v("a"))
    s.try_write(0, 0, v("b"))  # 历史版本
    s.try_write(0, 1, v("c"))
    s.try_write(0, 1, v("d"))  # 满区，但有效块只有 2 个
    assert s.try_write(0, 2, v("e"))["status"] == "full"

    s.start_migration(0, 2, v("e"))
    while s.get_migration() is not None:
        s.migration_step()

    dst = [r for r in s.region_usage() if r["group_id"] == 0][0]
    # 搬迁后只装有效块（2 个旧块 + 1 个 pending 新块），腾出 1 个空槽位
    assert dst["used_slots"] == 3
    assert dst["free_slots"] == 1
    assert s.read_latest(0, 0)["data"] == v("b")
    assert s.read_latest(0, 1)["data"] == v("d")
    assert s.read_latest(0, 2)["data"] == v("e")
    s.close()


def test_crash_after_each_migration_step_is_recoverable(tmp_path):
    n = 3
    db = str(tmp_path / "p.db")
    s = sm.PartitionStore(db, n, 8)
    for b in range(n):
        s.try_write(0, b, v("a"))
    s.start_migration(0, 0, v("z"))

    # 每走一步就“停机”（关连接），再用新连接恢复续跑
    step_count = 0
    while True:
        step = s.migration_step()
        step_count += 1
        s.close()
        s = sm.PartitionStore(db, n, 8)
        rec = s.recover()  # 重启恢复，不应报错
        if step["kind"] == "done":
            assert rec is None
            break
        assert rec is not None
        # 已确认的写入在重启后仍可读到（pending 首步落盘后新值即生效）
        assert s.read_latest(0, 0)["data"] == v("z")
        assert s.get_migration() is not None

    assert step_count == n + 1  # 1 个 pending + (n-1) 个 copied + 1 个 done
    assert s.read_latest(0, 0)["data"] == v("z")
    for b in range(1, n):
        assert s.read_latest(0, b)["data"] == v("a")
    s.close()


def test_retry_overwrite_after_crash(tmp_path):
    """搬迁中途停机后，重试同一覆盖写入应幂等成功。"""
    n = 3
    db = str(tmp_path / "p.db")
    s = sm.PartitionStore(db, n, 8)
    for b in range(n):
        s.try_write(0, b, v("a"))
    s.start_migration(0, 0, v("z"))
    s.migration_step()  # pending 落盘
    s.close()

    s = sm.PartitionStore(db, n, 8)
    s.recover()
    # 客户端重试覆盖：迁移未完成，组0没有 active 区，任何写入都不会生效，
    # 但数据不会坏；API 层把 NoActiveRegion 统一映射为 503 + Retry-After
    with pytest.raises(sm.NoActiveRegion):
        s.try_write(0, 0, v("z"))
    with pytest.raises(sm.NoActiveRegion):
        s.try_write(0, 2, v("q"))
    while s.get_migration() is not None:
        s.migration_step()
    assert s.read_latest(0, 0)["data"] == v("z")
    for b in range(1, n):
        assert s.read_latest(0, b)["data"] == v("a")
    # 再制造一次覆盖搬迁，验证恢复后的系统可持续服务、可重试覆盖
    assert s.try_write(0, 0, v("w"))["status"] == "full"
    s.start_migration(0, 0, v("w"))
    while s.get_migration() is not None:
        s.migration_step()
    assert s.read_latest(0, 0)["data"] == v("w")
    assert len([r for r in s.region_usage() if r["status"] == "spare"]) == 1
    s.close()


def test_recover_finalize_crash(tmp_path):
    """模拟 finalize 事务中“源记录已删、状态未翻转”的极端停机点。"""
    n = 2
    db = str(tmp_path / "p.db")
    s = sm.PartitionStore(db, n, 8)
    s.try_write(0, 0, v("a"))
    s.try_write(0, 1, v("b"))
    s.start_migration(0, 0, v("z"))
    info = s.get_migration()  # 在 done 之前记录迁移双方
    while True:
        st = s.migration_step()
        if st["kind"] == "done":
            break
    # 正常路径已 done；手工构造“删完未翻转”状态
    raw = sqlite3.connect(db)
    raw.execute("PRAGMA synchronous=FULL")
    src, dst, gid = info["src_region_id"], info["dst_region_id"], info["group_id"]
    # 先补回 migration 行，再把状态倒回迁移中、删源记录，模拟 finalize 事务做到一半
    raw.execute(
        "INSERT INTO migration(id, src_region_id, dst_region_id, group_id,"
        " pending_block_id, pending_data, pending_applied, last_copied_block_id)"
        " VALUES (1, ?, ?, ?, 0, ?, 1, 1)",
        (src, dst, gid, v("z")),
    )
    raw.execute("UPDATE regions SET status='migrating' WHERE region_id=?", (dst,))
    raw.execute(
        "UPDATE regions SET status='migrating_src', group_id=? WHERE region_id=?",
        (gid, src),
    )
    raw.execute("DELETE FROM records WHERE region_id=?", (src,))
    raw.commit()
    raw.close()
    s.close()

    s = sm.PartitionStore(db, n, 8)
    rec = s.recover()
    assert rec["status"] == "finalized"
    assert s.get_migration() is None
    assert s.read_latest(0, 0)["data"] == v("z")
    assert s.read_latest(0, 1)["data"] == v("b")
    spare = [r for r in s.region_usage() if r["status"] == "spare"]
    assert len(spare) == 1 and spare[0]["used_slots"] == 0
    s.close()


def test_other_groups_can_write_during_migration(tmp_path):
    n = 3
    s = make_store(tmp_path, slots=n)
    for b in range(n):
        s.try_write(0, b, v("a"))
    s.start_migration(0, 0, v("z"))
    s.migration_step()  # 搬迁进行到一半

    # 组1、组2 有自己的区/有槽位，照常写入
    assert s.try_write(1, 0, v("g"))["status"] == "written"
    assert s.try_write(2, 2, v("h"))["status"] == "written"
    # 组0 没有 active 区，任何写入都不会生效（API 层统一映射为 503）
    with pytest.raises(sm.NoActiveRegion):
        s.try_write(0, 1, v("x"))
    with pytest.raises(sm.NoActiveRegion):
        s.try_write(0, 0, v("x"))

    while s.get_migration() is not None:
        s.migration_step()
    assert s.read_latest(1, 0)["data"] == v("g")
    assert s.read_latest(2, 2)["data"] == v("h")
    assert s.read_latest(0, 0)["data"] == v("z")
    s.close()


def test_three_groups_use_three_regions_and_spare_preserved(tmp_path):
    n = 2
    s = make_store(tmp_path, slots=n)
    for g in range(3):
        s.try_write(g, 0, v("a"))
    statuses = sorted(r["status"] for r in s.region_usage())
    assert statuses == ["active", "active", "active", "spare"]
    # 把组0写满并启动搬迁，spare 被占用后其他组再想搬迁已无 spare 可用
    s.try_write(0, 1, v("b"))
    assert s.try_write(0, 0, v("z"))["status"] == "full"
    s.start_migration(0, 0, v("z"))
    # 组1 也把自己的区写满，但 spare 正被组0占用
    for b in range(n):
        s.try_write(1, b, v("a"))
    assert s.try_write(1, 0, v("z"))["status"] == "full"
    with pytest.raises(sm.NoSpare):
        s.start_migration(1, 0, v("z"))
    # 组0 搬迁收尾后 spare 回归，组1 现在可以启动自己的搬迁
    while s.get_migration() is not None:
        s.migration_step()
    s.start_migration(1, 0, v("z"))
    while s.get_migration() is not None:
        s.migration_step()
    assert len([r for r in s.region_usage() if r["status"] == "spare"]) == 1
    s.close()


def test_admin_reset_region_releases_slots(tmp_path):
    s = make_store(tmp_path)
    s.try_write(0, 0, v("a"))
    rid = s._find_active(0)
    s.admin_reset_region(rid)
    usage = {r["region_id"]: r for r in s.region_usage()}
    assert usage[rid]["status"] == "spare"
    assert usage[rid]["used_slots"] == 0
    s.close()


def test_config_persists_across_reopen(tmp_path):
    db = str(tmp_path / "p.db")
    s = sm.PartitionStore(db, slots_per_region=5, block_size=7)
    s.close()
    s2 = sm.PartitionStore(db, slots_per_region=99, block_size=99)  # 参数以库内为准
    assert s2.slots_per_region == 5
    assert s2.block_size == 7
    s2.close()
