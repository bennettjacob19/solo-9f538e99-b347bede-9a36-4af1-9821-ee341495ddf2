"""FastAPI 路由层。

写操作由单把 asyncio.Lock 串行化（SQLite 单连接）；搬迁分步推进，
步与步之间 ``asyncio.sleep(0)`` 让出事件循环，因此：
* 读请求全程不被搬迁阻塞；
* 其他组有槽位时可以在搬迁间隙插入写入；
* 同一组在搬迁期间写入会得到 503 + Retry-After。
"""

from __future__ import annotations

import asyncio
import base64
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse

from . import store as store_mod
from .schemas import (
    BlockResponse,
    ConfigResponse,
    MigrationResponse,
    RegionDetailResponse,
    RegionResponse,
    SlotRecord,
    WriteRequest,
)

RETRY_AFTER_SECONDS = "1"


def _to_block_response(group_id: int, block_id: int, row: dict) -> BlockResponse:
    raw: bytes = row["data"]
    return BlockResponse(
        group_id=group_id,
        block_id=block_id,
        seq=row["seq"],
        region_id=row["region_id"],
        slot_index=row["slot_index"],
        length=len(raw),
        data_ascii="".join(chr(b) if 32 <= b < 127 else "." for b in raw),
        data_base64=base64.b64encode(raw).decode("ascii"),
    )


def create_app() -> FastAPI:
    db_path = os.environ.get("DB_PATH", "/data/partition.db")
    slots = int(os.environ.get("SLOTS_PER_REGION", "8"))
    block_size = int(os.environ.get("BLOCK_SIZE", "16"))
    admin_enabled = os.environ.get("ENABLE_ADMIN", "1") not in ("0", "false", "False")
    step_delay = float(os.environ.get("MIGRATION_STEP_DELAY", "0"))
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        directory = os.path.dirname(db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        app.state.store = store_mod.PartitionStore(db_path, slots, block_size)
        app.state.write_lock = asyncio.Lock()
        app.state.last_step = None
        # 重启恢复：补完可能停在 finalize 中途的搬迁
        app.state.recovery = app.state.store.recover()
        yield
        app.state.store.close()

    app = FastAPI(
        title="分区盘演练 API",
        version="1.0.0",
        description=(
            "4 个等大物理区 / 3 个逻辑组的 append-only 分区盘。"
            "物理槽位只能尾部追加，整区重置才释放；满区覆盖触发 spare 区搬迁，"
            "搬迁分步持久化，崩溃可恢复。交互式文档见 /docs。"
        ),
        lifespan=lifespan,
    )

    def get_store(req: Request) -> store_mod.PartitionStore:
        return req.app.state.store

    async def _drain_migration(req: Request) -> None:
        """把当前搬迁跑完。

        每一步单独取写锁、独立事务提交，步间释放锁并让出事件循环，
        因此其他组的写请求能在搬迁间隙插入，读请求则完全不受影响。
        调用方须保证搬迁已启动（至少 pending 行已登记）。
        """
        store = get_store(req)
        while store.get_migration() is not None:
            async with req.app.state.write_lock:
                step = store.migration_step()
            req.app.state.last_step = step
            if step_delay:
                await asyncio.sleep(step_delay)
            else:
                await asyncio.sleep(0)

    # ---------------------------------------------------------------- 异常映射
    @app.exception_handler(store_mod.StoreError)
    async def store_error_handler(_: Request, exc: store_mod.StoreError):
        code = status.HTTP_409_CONFLICT
        if isinstance(exc, store_mod.NoSpare):
            code = status.HTTP_503_SERVICE_UNAVAILABLE
        if isinstance(exc, store_mod.NoActiveRegion):
            code = status.HTTP_503_SERVICE_UNAVAILABLE
        return JSONResponse(status_code=code, content={"detail": str(exc)})

    # ---------------------------------------------------------------- 元信息
    @app.get("/config", response_model=ConfigResponse, tags=["meta"])
    def get_config(req: Request):
        store = get_store(req)
        return ConfigResponse(
            regions=store_mod.REGION_COUNT,
            groups=store_mod.GROUP_COUNT,
            slots_per_region=store.slots_per_region,
            block_size=store.block_size,
        )

    @app.get("/health", tags=["meta"])
    def health():
        return {"status": "ok"}

    # ---------------------------------------------------------------- 块读写
    @app.get(
        "/groups/{group_id}/blocks/{block_id}",
        response_model=BlockResponse,
        tags=["blocks"],
        summary="按组号+块号读取定长数据的最新值",
    )
    def read_block(group_id: int, block_id: int, req: Request):
        store = get_store(req)
        if not (0 <= group_id < store_mod.GROUP_COUNT):
            raise HTTPException(422, f"group_id 须在 0..{store_mod.GROUP_COUNT - 1}")
        if not (0 <= block_id < store.slots_per_region):
            raise HTTPException(422, f"block_id 须在 0..{store.slots_per_region - 1}")
        row = store.read_latest(group_id, block_id)
        if row is None:
            raise HTTPException(404, f"组 {group_id} 块 {block_id} 尚无数据")
        return _to_block_response(group_id, block_id, row)

    @app.put(
        "/groups/{group_id}/blocks/{block_id}",
        tags=["blocks"],
        summary="按组号+块号写入定长数据（尾部追加；满区覆盖触发搬迁）",
    )
    async def write_block(group_id: int, block_id: int, body: WriteRequest, req: Request):
        store = get_store(req)
        if not (0 <= group_id < store_mod.GROUP_COUNT):
            raise HTTPException(422, f"group_id 须在 0..{store_mod.GROUP_COUNT - 1}")
        if not (0 <= block_id < store.slots_per_region):
            raise HTTPException(422, f"block_id 须在 0..{store.slots_per_region - 1}")
        try:
            data = body.decode(store.block_size)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        if len(data) != store.block_size:
            raise HTTPException(
                400,
                f"数据长度须等于 BLOCK_SIZE={store.block_size} 字节，实际 {len(data)} 字节",
            )

        async with req.app.state.write_lock:
            if store.get_migration() is not None:
                m = store.get_migration()
                # 本组搬迁进行中：新区尚不可追加（done 之前不算 active）
                if m["group_id"] == group_id:
                    return JSONResponse(
                        status_code=503,
                        headers={"Retry-After": RETRY_AFTER_SECONDS},
                        content={
                            "detail": (
                                f"组 {group_id} 正在搬迁（物理区 {m['src_region_id']} "
                                f"-> {m['dst_region_id']}），请稍后重试"
                            )
                        },
                    )
                # 其他组的搬迁占用了 spare；本组有槽位仍可写，没槽位则等搬迁结束
                result = store.try_write(group_id, block_id, data)
                if result["status"] == "full":
                    return JSONResponse(
                        status_code=503,
                        headers={"Retry-After": RETRY_AFTER_SECONDS},
                        content={
                            "detail": (
                                f"组 {group_id} 的物理区 {result['region_id']} 已满，"
                                "spare 区正被其他组搬迁占用，请稍后重试"
                            )
                        },
                    )
                return {"status": "written", **result}

            result = store.try_write(group_id, block_id, data)
            if result["status"] == "written":
                return {"status": "written", **result}

            # 本组 active 区已满：启动搬迁，并在同一锁区段内落盘
            # pending 首步（触发写入的新值），消除“已启动未落盘”的并发窗口
            store.start_migration(group_id, block_id, data)
            req.app.state.last_step = store.migration_step()
            if step_delay:
                await asyncio.sleep(step_delay)

        # 锁外继续分步搬迁：每步重新取锁，其他组可在步间写入
        await _drain_migration(req)
        row = store.read_latest(group_id, block_id)
        return {
            "status": "migrated",
            "detail": "原 active 区已满，已完成整区搬迁并写入新值",
            "write": _to_block_response(group_id, block_id, row).model_dump(),
            "last_step": req.app.state.last_step,
        }

    # ---------------------------------------------------------------- 物理区
    @app.get(
        "/regions",
        response_model=list[RegionResponse],
        tags=["regions"],
        summary="查看各物理区状态与槽位占用",
    )
    def list_regions(req: Request):
        return get_store(req).region_usage()

    @app.get(
        "/regions/{region_id}",
        response_model=RegionDetailResponse,
        tags=["regions"],
        summary="查看某物理区每个槽位的实际内容",
    )
    def get_region(region_id: int, req: Request):
        store = get_store(req)
        if not (0 <= region_id < store_mod.REGION_COUNT):
            raise HTTPException(422, f"region_id 须在 0..{store_mod.REGION_COUNT - 1}")
        usage = next(
            (u for u in store.region_usage() if u["region_id"] == region_id), None
        )
        records = [
            SlotRecord(
                seq=r["seq"],
                slot_index=r["slot_index"],
                group_id=r["group_id"],
                block_id=r["block_id"],
                data_base64=base64.b64encode(r["data"]).decode("ascii"),
            )
            for r in store.region_records(region_id)
        ]
        return RegionDetailResponse(**usage, records=records)

    # ---------------------------------------------------------------- 搬迁
    @app.get(
        "/migrations",
        response_model=MigrationResponse,
        tags=["migration"],
        summary="查看当前搬迁进度",
    )
    def get_migration(req: Request):
        m = get_store(req).get_migration()
        if m is None:
            return MigrationResponse(in_progress=False)
        return MigrationResponse(in_progress=True, last_step=req.app.state.last_step, **m)

    @app.post(
        "/migrations/resume",
        tags=["migration"],
        summary="重启后续跑中断的搬迁（崩溃恢复 / 重试覆盖）",
    )
    async def resume_migration(req: Request):
        store = get_store(req)
        if store.get_migration() is None:
            return {"status": "no_migration", "detail": "当前没有进行中的搬迁"}
        await _drain_migration(req)
        return {"status": "completed", "last_step": req.app.state.last_step}

    # ---------------------------------------------------------------- 管理
    if admin_enabled:

        @app.post(
            "/admin/regions/{region_id}/reset",
            tags=["admin"],
            summary="【演练】整区重置：释放该区全部槽位并回收为 spare",
        )
        async def reset_region(region_id: int, req: Request):
            if not (0 <= region_id < store_mod.REGION_COUNT):
                raise HTTPException(422, f"region_id 须在 0..{store_mod.REGION_COUNT - 1}")
            async with req.app.state.write_lock:
                get_store(req).admin_reset_region(region_id)
            return {"status": "reset", "region_id": region_id}

        @app.post("/admin/reset-all", tags=["admin"], summary="【演练】清空全部数据")
        async def reset_all(req: Request):
            async with req.app.state.write_lock:
                get_store(req).admin_reset_all()
            req.app.state.last_step = None
            return Response(status_code=status.HTTP_204_NO_CONTENT)

    return app


# uvicorn --factory app.main:create_app 启动；测试中也直接调用 create_app()
__all__ = ["create_app"]
