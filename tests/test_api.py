"""API 层测试（FastAPI TestClient，同步驱动）。"""

import base64
import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db = tmp_path / "api.db"
    monkeypatch.setenv("DB_PATH", str(db))
    monkeypatch.setenv("SLOTS_PER_REGION", "3")
    monkeypatch.setenv("BLOCK_SIZE", "8")
    monkeypatch.setenv("MIGRATION_STEP_DELAY", "0")
    monkeypatch.setenv("ENABLE_ADMIN", "1")
    import app.main as main_mod

    with TestClient(main_mod.create_app()) as c:
        yield c


def payload(ch: str, size: int = 8) -> dict:
    return {"data": ch * size}


def test_health_config_docs(client):
    assert client.get("/health").json() == {"status": "ok"}
    cfg = client.get("/config").json()
    assert cfg == {"regions": 4, "groups": 3, "slots_per_region": 3, "block_size": 8}
    assert client.get("/openapi.json").status_code == 200  # /docs 的 schema 来源


def test_write_read_cycle_and_regions(client):
    r = client.put("/groups/0/blocks/1", json=payload("a"))
    assert r.status_code == 200
    assert r.json()["status"] == "written"

    r = client.get("/groups/0/blocks/1")
    body = r.json()
    assert body["data_base64"] == base64.b64encode(b"a" * 8).decode()
    assert body["data_ascii"] == "a" * 8
    assert body["length"] == 8

    regions = client.get("/regions").json()
    assert len(regions) == 4
    used = [x for x in regions if x["group_id"] == 0][0]
    assert used["used_slots"] == 1 and used["free_slots"] == 2

    detail = client.get(f"/regions/{used['region_id']}").json()
    assert detail["records"][0]["block_id"] == 1


def test_validation_errors(client):
    assert client.get("/groups/9/blocks/0").status_code == 422
    assert client.get("/groups/0/blocks/99").status_code == 422
    assert client.put("/groups/0/blocks/0", json={"data": "short"}).status_code == 400
    assert client.put(
        "/groups/0/blocks/0", json={"data": "x", "data_base64": "eA=="}
    ).status_code == 400
    assert client.get("/groups/0/blocks/2").status_code == 404
    # 合法 base64
    ok = client.put(
        "/groups/0/blocks/2",
        json={"data_base64": base64.b64encode(b"\x00" * 8).decode()},
    )
    assert ok.status_code == 200


def test_full_region_triggers_migration_via_api(client):
    for b in range(3):
        assert client.put(f"/groups/0/blocks/{b}", json=payload("a")).status_code == 200
    # 第 4 次写满区的块 0：触发搬迁
    r = client.put("/groups/0/blocks/0", json=payload("z"))
    assert r.status_code == 200
    assert r.json()["status"] == "migrated"
    assert client.get("/groups/0/blocks/0").json()["data_ascii"] == "z" * 8

    regions = client.get("/regions").json()
    spare = [x for x in regions if x["status"] == "spare"][0]
    assert spare["used_slots"] == 0  # 整区重置释放了全部槽位
    active = [x for x in regions if x["group_id"] == 0][0]
    assert active["used_slots"] == 3


def test_503_while_migration_in_progress_and_resume(client, monkeypatch):
    # 放慢搬迁速度以便观察中间态：重新构建一个带步间延迟的 app
    monkeypatch.setenv("MIGRATION_STEP_DELAY", "0.2")
    import app.main as main_mod

    with TestClient(main_mod.create_app()) as slow:
        store = slow.app.state.store
        # 沿用同一布局参数写满组0
        for b in range(3):
            slow.put(f"/groups/0/blocks/{b}", json=payload("a"))

        # 后台发起触发搬迁的写请求
        import threading

        resp = {}

        def do_write():
            resp["r"] = slow.put("/groups/0/blocks/0", json=payload("z"))

        t = threading.Thread(target=do_write)
        t.start()
        # 等待搬迁进入进行态
        for _ in range(50):
            if slow.get("/migrations").json().get("in_progress"):
                break
            import time

            time.sleep(0.02)

        m = slow.get("/migrations").json()
        assert m["in_progress"] is True
        # 本组写入：503 + Retry-After
        r = slow.put("/groups/0/blocks/1", json=payload("x"))
        assert r.status_code == 503
        assert r.headers["retry-after"] == "1"
        # 其他组有槽位：照常写
        assert slow.put("/groups/1/blocks/0", json=payload("g")).status_code == 200
        # 搬迁进行中，新区已落盘的新值可以直接读到
        assert slow.get("/groups/0/blocks/0").json()["data_ascii"] == "z" * 8

        # 续跑接口在搬迁已完成时返回 no_migration
        t.join()
        resume = slow.post("/migrations/resume")
        assert resume.json()["status"] == "no_migration"
        assert resp["r"].json()["status"] == "migrated"


def test_crash_restart_resume_and_retry(tmp_path, monkeypatch):
    db = tmp_path / "crash.db"
    monkeypatch.setenv("DB_PATH", str(db))
    monkeypatch.setenv("SLOTS_PER_REGION", "3")
    monkeypatch.setenv("BLOCK_SIZE", "8")
    monkeypatch.setenv("MIGRATION_STEP_DELAY", "0")
    import app.main as main_mod

    # 第一生命周期：写满 -> 启动搬迁 -> 只跑两步后“停机”
    with TestClient(main_mod.create_app()) as c1:
        for b in range(3):
            c1.put(f"/groups/0/blocks/{b}", json=payload("a"))
        store = c1.app.state.store
        store.start_migration(0, 0, b"z" * 8)
        store.migration_step()
        store.migration_step()
        assert store.get_migration() is not None

    # 第二生命周期：重启，已确认的写入仍可读，续跑完成搬迁
    with TestClient(main_mod.create_app()) as c2:
        assert c2.get("/migrations").json()["in_progress"] is True
        assert c2.get("/groups/0/blocks/0").json()["data_ascii"] == "z" * 8
        done = c2.post("/migrations/resume")
        assert done.json()["status"] == "completed"
        assert c2.get("/migrations").json()["in_progress"] is False
        # 重试覆盖也没问题
        r = c2.put("/groups/0/blocks/0", json=payload("q"))
        assert r.status_code == 200
        assert c2.get("/groups/0/blocks/0").json()["data_ascii"] == "q" * 8


def test_admin_endpoints(client):
    client.put("/groups/0/blocks/0", json=payload("a"))
    rid = [r for r in client.get("/regions").json() if r["group_id"] == 0][0][
        "region_id"
    ]
    assert client.post(f"/admin/regions/{rid}/reset").status_code == 200
    assert client.get(f"/regions/{rid}").json()["status"] == "spare"
    assert client.post("/admin/reset-all").status_code == 204
    assert client.get("/groups/0/blocks/0").status_code == 404
