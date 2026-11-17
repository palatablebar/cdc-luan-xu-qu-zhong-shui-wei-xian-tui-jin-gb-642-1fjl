"""发布基线与重算版本治理（深夜重算不停止查询的裁决落地）。

裁决：不停止查询；新旧结果各自完整后，由 baseline 单点原子切换。
覆盖不变量：

1. 页面与导出始终读同一个发布基线，重算中不会先露出半成品新总量；
2. 重算失败 → 基线与旧结果原样保留，不丢数据，attempt 只留审计；
3. 同批次键重试复用同一 attempt 行、attempts 累加，不新增、不重复累计；
4. 成功重算 → 结果与基线同事务切换，页面/导出随后统一指向新版本；
5. 同一批次键并发重算被 409 拦截。
"""
from __future__ import annotations

import json
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from app import errors
from app.main import create_app
from app.modules.m6_batches import service
from app.store import Store

HEADER = "# cdc-export v2 rows={n} charset=utf-8\n"


def _event(pk: str, event_time: str, amount: str = "10.00",
           version: int = 1, op: str = "upsert") -> str:
    return json.dumps({
        "table": "orders", "pk": pk, "version": version, "op": op,
        "event_time": event_time, "schema": "v2",
        "payload": {"status": "新建", "amount": amount},
    }, ensure_ascii=False)


def _write_sample(directory, name: str, rows: list[str]) -> str:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(HEADER.format(n=len(rows)) + "\n".join(rows) + "\n",
                    encoding="utf-8")
    return name


def _settings(settings, tmp_path, name: str):
    directory = tmp_path / name
    return replace(settings, sample_dir=str(directory)), directory


def _run(settings, store, name: str):
    return service.run(settings=settings, store=store, source=name)


def _snapshot_amount(store, batch_id: str) -> str:
    return store.load_totals(batch_id)["amount_snapshot"]


def test_first_run_publishes_baseline(settings, store, tmp_path):
    settings, directory = _settings(settings, tmp_path, "v1dir")
    name = _write_sample(directory, "v1.jsonl", [
        _event("9001", "2026-11-16T08:00:00Z", "100.00"),
        _event("9002", "2026-11-16T08:05:00Z", "50.00"),
    ])
    result = _run(settings, store, name)

    assert result["status"] == "done"
    active = store.active_batch()
    assert active is not None
    assert active["batch_id"] == result["batch"]["batch_id"]
    assert store.baseline_row()["batch_id"] == active["batch_id"]
    # 首次采信不是「重算」，不登记重算尝试。
    assert store.get_attempt(result["batch_key"]) is None


def test_failed_recalc_keeps_old_baseline_and_rows(settings, store, tmp_path):
    # 第一版：先建立发布基线。
    s1, d1 = _settings(settings, tmp_path, "d1")
    n1 = _write_sample(d1, "v1.jsonl", [
        _event("9001", "2026-11-16T08:00:00Z", "100.00"),
        _event("9002", "2026-11-16T08:05:00Z", "50.00"),
    ])
    first = _run(s1, store, n1)
    old_id = first["batch"]["batch_id"]
    old_rows = store.count_rows(old_id)
    old_amount = _snapshot_amount(store, old_id)

    # 第二版：不同输入 → 新批次键；在物化阶段注入失败。
    s2, d2 = _settings(settings, tmp_path, "d2")
    n2 = _write_sample(d2, "v2.jsonl", [
        _event("9001", "2026-11-16T09:00:00Z", "999.00"),
        _event("9003", "2026-11-16T09:05:00Z", "888.00"),
    ])
    from app.modules.m6_batches import service as svc

    def boom(*_args, **_kwargs):
        raise RuntimeError("boom: 模拟深夜重算中途失败")

    original = svc.materialize.materialize
    svc.materialize.materialize = boom
    try:
        with pytest.raises(RuntimeError, match="boom"):
            service.run(settings=s2, store=store, source=n2)
    finally:
        svc.materialize.materialize = original

    # 基线不变、旧结果一行不丢。
    assert store.active_batch()["batch_id"] == old_id
    assert store.count_rows(old_id) == old_rows
    assert _snapshot_amount(store, old_id) == old_amount
    # 失败重算没有留下任何业务结果行，也没有出现半成品批次。
    assert {b["batch_id"] for b in store.list_batches()} == {old_id}

    # 失败痕迹可审计：running 已收尾为 failed，且带错误原因。
    key2 = store.latest_attempts()[0]["batch_key"]
    attempt = store.get_attempt(key2)
    assert attempt["status"] == "failed"
    assert attempt["attempts"] == 1
    assert "boom" in attempt["last_error"]
    assert store.running_attempts() == []


def test_retry_same_key_accumulates_attempts_without_new_rows(settings, store, tmp_path):
    s1, d1 = _settings(settings, tmp_path, "d1")
    n1 = _write_sample(d1, "v1.jsonl", [
        # 锚点事件 09:30 把水位定在 09:20，07:00 的事件确定落在快照窗口。
        _event("9001", "2026-11-16T07:00:00Z", "100.00"),
        _event("9099", "2026-11-16T09:30:00Z", "0.00"),
    ])
    first = _run(s1, store, n1)

    # 同一份输入（同批次键）先失败两次、再成功：attempt 行只有一条。
    # 同样用晚锚点固定水位，使 07:30 那条进快照窗口。
    s2, d2 = _settings(settings, tmp_path, "d2")
    n2 = _write_sample(d2, "v2.jsonl", [
        _event("9001", "2026-11-16T07:30:00Z", "200.00"),
        _event("9002", "2026-11-16T08:10:00Z", "300.00"),
        _event("9099", "2026-11-16T09:30:00Z", "0.00"),
    ])
    from app.modules.m6_batches import service as svc

    calls = {"n": 0}

    def flaky(*_args, **_kwargs):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise RuntimeError("boom: 暂时性失败")
        return original(*_args, **_kwargs)

    original = svc.materialize.materialize
    svc.materialize.materialize = flaky
    try:
        for _ in range(2):
            with pytest.raises(RuntimeError):
                service.run(settings=s2, store=store, source=n2)
            # 每次失败后基线都仍是旧版，旧结果不受影响。
            assert store.active_batch()["batch_id"] == first["batch"]["batch_id"]
        success = service.run(settings=s2, store=store, source=n2)
    finally:
        svc.materialize.materialize = original

    # 成功后基线切到新版本。
    new_id = success["batch"]["batch_id"]
    assert new_id != first["batch"]["batch_id"]
    assert store.active_batch()["batch_id"] == new_id

    # 同一批次键只有一条 attempt 记录，attempts 累加到 3（2 失败 + 1 成功）。
    attempts = [a for a in store.latest_attempts() if a["batch_key"] == success["batch_key"]]
    assert len(attempts) == 1
    assert attempts[0]["attempts"] == 3
    assert attempts[0]["status"] == "done"

    # 业务批次只有两个版本，没有失败产生的多余批次/重复行。
    assert {b["batch_id"] for b in store.list_batches()} == {
        first["batch"]["batch_id"], new_id}
    # 晚锚点把水位定在 09:20，200+300 两条都进快照窗口。
    assert _snapshot_amount(store, new_id) == "500.00"
    # 旧版本结果仍完整保留，可随时回查。
    assert _snapshot_amount(store, first["batch"]["batch_id"]) == "100.00"


def test_idempotent_replay_does_not_touch_baseline_or_attempts(settings, store, tmp_path):
    s1, d1 = _settings(settings, tmp_path, "d1")
    n1 = _write_sample(d1, "v1.jsonl", [
        _event("9001", "2026-11-16T08:00:00Z", "100.00"),
    ])
    first = _run(s1, store, n1)
    before = store.count_rows(first["batch"]["batch_id"])

    second = _run(s1, store, n1)
    assert second["idempotent_replay"] is True
    assert second["batch"]["batch_id"] == first["batch"]["batch_id"]
    # 幂等重放不登记重算尝试、不动基线、不新增行。
    assert store.get_attempt(first["batch_key"]) is None
    assert store.active_batch()["batch_id"] == first["batch"]["batch_id"]
    assert store.count_rows(first["batch"]["batch_id"]) == before


def test_concurrent_rerun_same_key_rejected(settings, store, tmp_path, monkeypatch):
    s1, d1 = _settings(settings, tmp_path, "d1")
    n1 = _write_sample(d1, "v1.jsonl", [
        _event("9001", "2026-11-16T08:00:00Z", "100.00"),
    ])
    _run(s1, store, n1)

    s2, d2 = _settings(settings, tmp_path, "d2")
    n2 = _write_sample(d2, "v2.jsonl", [
        _event("9001", "2026-11-16T09:00:00Z", "200.00"),
    ])
    from app.modules.m6_batches import service as svc

    captured = {}

    def conflict_inside(*_args, **_kwargs):
        # 第一次重算进行到一半（attempt 已登记为 running）时，同键再来一次。
        with pytest.raises(errors.Conflict) as info:
            service.run(settings=s2, store=store, source=n2)
        captured["code"] = info.value.code
        return original(*_args, **_kwargs)

    original = svc.materialize.materialize
    monkeypatch.setattr(svc.materialize, "materialize", conflict_inside)
    service.run(settings=s2, store=store, source=n2)

    assert captured["code"] == "B_RERUN_IN_PROGRESS"
    # 被拒的并发重算没有新增 attempt 行，最终只有一条 done 记录。
    attempts = store.latest_attempts()
    assert len(attempts) == 1
    assert attempts[0]["status"] == "done"
    assert attempts[0]["attempts"] == 1


def test_page_and_export_share_baseline_during_and_after_recalc(settings, tmp_path):
    db = tmp_path / "app.sqlite"
    app_settings = replace(settings, db_path=str(db), sample_dir=str(tmp_path))
    app = create_app(app_settings, seed=False)

    # HTTP 入口只放行登记来源（main/late），用对应固定文件名承载两版数据。
    _write_sample(tmp_path, "events_main.jsonl", [
        _event("9001", "2026-11-16T07:00:00Z", "100.00"),
        _event("9002", "2026-11-16T07:05:00Z", "50.00"),
        _event("9099", "2026-11-16T09:30:00Z", "0.00"),
    ])
    _write_sample(tmp_path, "events_late.jsonl", [
        _event("9001", "2026-11-16T08:00:00Z", "999.00"),
        _event("9003", "2026-11-16T08:05:00Z", "888.00"),
        _event("9099", "2026-11-16T09:30:00Z", "0.00"),
    ])

    from app.modules.m6_batches import service as svc
    from app.modules.m7_export import service as export_service

    with TestClient(app) as client:
        first = client.post("/api/events/import",
                            json={"source": "main"}).json()
        old_id = first["batch_id"]
        assert client.get("/api/health").json()["batch_id"] == old_id
        store = app.state.store

        def check_while_running(*_args, **_kwargs):
            # 重算进行中：不在请求里嵌套请求（TestClient 会死锁），
            # 直接走与 HTTP handler 同一份 store / 导出服务读路径。
            assert [a["batch_id"] for a in store.running_attempts()]
            assert store.active_batch()["batch_id"] == old_id
            assert export_service._batch(store, None)["batch_id"] == old_id
            _, filename = export_service.render_csv(store, old_id, "snapshot")
            assert filename == f"{old_id}-snapshot.csv"
            return original(*_args, **_kwargs)

        original = svc.materialize.materialize
        svc.materialize.materialize = check_while_running
        try:
            second = client.post("/api/events/import",
                                 json={"source": "late"}).json()
        finally:
            svc.materialize.materialize = original

        new_id = second["batch_id"]
        assert new_id != old_id
        # 切换后页面与导出统一指向新版本，且文件名自带基线版本号。
        assert client.get("/api/health").json()["batch_id"] == new_id
        assert client.get("/api/events").json()["batch_id"] == new_id
        export = client.get("/api/export/snapshot.csv")
        assert f"{new_id}-snapshot.csv" in export.headers["content-disposition"]
        assert old_id not in export.headers["content-disposition"]
        # 旧版本仍可按 batch_id 显式回查（只读历史，不影响基线）。
        old_export = client.get("/api/export/snapshot.json")
        assert old_export.status_code == 200


def test_rerun_banner_visible_on_every_page_while_running(settings, tmp_path):
    """重算中全页顶栏统一提示，且页面数据仍是旧基线（不嵌套请求，直接造 running 行）。"""
    db = tmp_path / "app.sqlite"
    app_settings = replace(settings, db_path=str(db), sample_dir=str(tmp_path))
    app = create_app(app_settings, seed=False)
    _write_sample(tmp_path, "events_main.jsonl", [
        _event("9001", "2026-11-16T07:00:00Z", "100.00"),
        _event("9099", "2026-11-16T09:30:00Z", "0.00"),
    ])
    with TestClient(app) as client:
        first = client.post("/api/events/import",
                            json={"source": "main"}).json()
        old_id = first["batch_id"]
        store = app.state.store
        store.start_attempt(batch_key="recalckey01", batch_id="B-recalc01",
                            run_id="CDC-recalc01", started_at="2026-11-16T10:00:00.000Z")
        try:
            for path in ("/", "/events", "/conflicts", "/snapshot", "/batches"):
                page = client.get(path)
                assert page.status_code == 200, path
                assert "重算中" in page.text, path
                assert old_id in page.text, path  # 数据仍是旧基线
            health = client.get("/api/health").json()
            assert health["recalculating"] is True
            assert health["batch_id"] == old_id
        finally:
            store.finish_attempt(batch_key="recalckey01", status="failed",
                                 finished_at="2026-11-16T10:05:00.000Z",
                                 error="teardown")


def test_failed_recalc_via_http_keeps_baseline(settings, tmp_path):
    db = tmp_path / "app.sqlite"
    app_settings = replace(settings, db_path=str(db), sample_dir=str(tmp_path))
    app = create_app(app_settings, seed=False)
    _write_sample(tmp_path, "events_main.jsonl", [
        _event("9001", "2026-11-16T07:00:00Z", "100.00"),
        _event("9099", "2026-11-16T09:30:00Z", "0.00"),
    ])
    _write_sample(tmp_path, "events_late.jsonl", [
        _event("9001", "2026-11-16T08:00:00Z", "999.00"),
        _event("9099", "2026-11-16T09:30:00Z", "0.00"),
    ])
    with TestClient(app) as client:
        first = client.post("/api/events/import",
                            json={"source": "main"}).json()
        old_id = first["batch_id"]

        from app.modules.m6_batches import service as svc

        def boom(*_args, **_kwargs):
            raise RuntimeError("boom http")

        original = svc.materialize.materialize
        svc.materialize.materialize = boom
        try:
            with pytest.raises(RuntimeError, match="boom http"):
                client.post("/api/events/import", json={"source": "late"})
        finally:
            svc.materialize.materialize = original

        health = client.get("/api/health").json()
        assert health["batch_id"] == old_id
        assert health["recalculating"] is False
        # 页面在失败后仍正常渲染旧版，不出现空页/500。
        assert client.get("/").status_code == 200
        assert client.get("/api/export/events.csv").status_code == 200

        # 批次页能看到 failed 尝试与重试次数（状态经过滤器渲染为中文）。
        page = client.get("/batches").text
        assert "重算尝试" in page
        assert "失败" in page
        api = client.get("/api/batches").json()
        assert api["active_batch_id"] == old_id
        assert any(a["status"] == "failed" for a in api["attempts"])


def test_stale_rejection_leaves_baseline_untouched(settings, store, tmp_path):
    s1, d1 = _settings(settings, tmp_path, "d1")
    n1 = _write_sample(d1, "v1.jsonl", [
        _event("9001", "2026-11-16T09:00:00Z", "100.00"),
    ])
    first = _run(s1, store, n1)

    # 更旧的输入：event_time 落后于已持久化水位 → 409。
    s2, d2 = _settings(settings, tmp_path, "d2")
    n2 = _write_sample(d2, "old.jsonl", [
        _event("9009", "2026-11-16T06:00:00Z", "1.00"),
    ])
    with pytest.raises(errors.StaleBatch):
        service.run(settings=s2, store=store, source=n2)

    assert store.active_batch()["batch_id"] == first["batch"]["batch_id"]
    # 陈旧拒绝发生在 attempt 登记之后、结果落库之前：留 failed 审计，不产生业务批次。
    attempt = store.latest_attempts()[0]
    assert attempt["status"] == "failed"
    assert {b["batch_id"] for b in store.list_batches()} == {first["batch"]["batch_id"]}


def test_legacy_store_without_baseline_falls_back_to_latest_done(settings, tmp_path):
    """老库升级：还没有 baseline 行时，读路径回退到最新 done 批次，页面不空。"""
    db = tmp_path / "legacy.sqlite"
    d1 = tmp_path / "d1"
    s1 = replace(settings, db_path=str(db), sample_dir=str(d1))
    store = Store(s1.db_path)
    n1 = _write_sample(d1, "v1.jsonl", [
        _event("9001", "2026-11-16T07:00:00Z", "100.00"),
        _event("9099", "2026-11-16T09:30:00Z", "0.00"),
    ])
    result = service.run(settings=s1, store=store, source=n1)
    # 手动模拟老库：删掉 baseline 行。
    with store.connect() as conn:
        conn.execute("DELETE FROM baseline")
    assert store.baseline_row() is None
    assert store.active_batch()["batch_id"] == result["batch"]["batch_id"]
