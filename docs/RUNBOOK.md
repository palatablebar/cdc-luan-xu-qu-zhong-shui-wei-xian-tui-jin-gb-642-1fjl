# 运行手册 cdcwater

## 1 启动与自检

```bash
docker compose config --quiet
docker compose up -d --build
curl -s http://127.0.0.1:18201/api/health
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:18201/
docker compose exec -T app python -m pytest -q
```

首次启动会用 `sample_data/` 的三个文件跑一次采信（`app/bootstrap.py`），页面直接有数据；
重复启动命中批次键，走幂等重放。

## 2 主流程（页面）

1. `/` 水位时间轴 → 选来源 → 点「采信」；
2. `/events` 按分类筛选，确认每行的行号引用与原因码；
3. `/conflicts` 看冲突分组与隔离区，导出隔离 CSV；
4. `/snapshot` 点「物化」，核对三条对账；
5. `/batches` 看 `run_id`、输入 sha256 与阶段耗时，导出运行清单。

## 3 主流程（命令）

```bash
BASE=http://127.0.0.1:18201
curl -s -X POST $BASE/api/events/import -H 'content-type: application/json' -d '{"source":"all"}' | head -c 600
curl -s "$BASE/api/conflicts" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["counters"])'
curl -s "$BASE/api/watermark"  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["watermark"])'
curl -s -X POST $BASE/api/snapshot | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["rows"], d["amounts"])'
curl -s -o /tmp/events.csv      "$BASE/api/export/events.csv"
curl -s -o /tmp/quarantine.csv  "$BASE/api/export/quarantine.csv"
curl -s -o /tmp/conflicts.csv   "$BASE/api/export/conflicts.csv"
curl -s -o /tmp/snapshot.csv    "$BASE/api/export/snapshot.csv"
curl -s -o /tmp/manifest.json   "$BASE/api/export/manifest.json"
```

判定看数字：行账 42 = 27 + 2 + 3 + 10；键账 19 = 18 + 1；金额 1552.51 + 200.00 + 0.00 = 1752.51。

## 4 失败处置

| 现象 | 原因码 | 处置 |
|---|---|---|
| 采信后页面无数据 | — | 看 `/events` 的「输入文件」表，文件状态 `failed` 说明编码链全失败 |
| 隔离区大量同名行 | `E_COLUMN_COUNT` / `E_BAD_JSON` | 导出端截断或多分隔符，重新导出 |
| 冲突表出现同键同版本 | `C_VERSION_CONFLICT` | 两条都不应用，回源确认版本号 |
| 导入报 409 | `C_STALE_BATCH` | 拿到的是旧导出，重新导出后再跑 |
| 重算报 409 | `B_RERUN_IN_PROGRESS` | 同一批次键已有重算在跑，等其结束再试；并发重算不会累计 |
| 重算后页面还是旧数字 | `B_BASELINE_KEPT`（语义） | 重算失败，基线未切换；看 `/batches` 重算尝试的失败原因，修复后同键重试 |
| 快照键比预期少 | `W_PENDING_WINDOW` | 事件落在水位之后，等下一轮水位推过 |
| 水位不前进 | `W_WATERMARK_REGRESS` | 检查导出端时钟与文件版本 |

## 5 重建数据库

```bash
docker compose down -v          # 删掉命名卷 cdcwater-data
docker compose up -d --build    # 重新初始化并跑一次首次采信
```

本地直接跑（不用 Docker）：

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
CDCWATER_DB=./var/cdcwater.sqlite CDCWATER_SAMPLE_DIR=./sample_data \
  uvicorn app.main:create_app --factory --port 18201
python -m pytest -q
```

## 6 导出复核

- `events.csv`：42 行，含 `行号引用`、`原因码`、`分类`、`迟到`；
- `quarantine.csv`：10 行，含 `字段`、`原值`、`原始文本`；
- `conflicts.csv`：3 行，含两条证据行引用；
- `snapshot.csv` / `snapshot.json`：18 键，活跃 16、墓碑 2；
- `manifest.json`：`run_id`、每个输入的 sha256、阶段耗时、四账。

## 7 深夜重算与发布基线（不停止查询）

**裁决：重算期间不停止查询，新旧结果各自完整后由「发布基线」单点原子切换，
页面与导出全程共用同一个基准版本。**

读路径只认 `baseline` 表（单行）指向的 `status='done'` 批次；重算过程不写业务结果行，
只在 `run_attempts` 留一条运行痕迹。因此：

| 时刻 | 页面 / 导出 | 顶栏 |
|---|---|---|
| 重算进行中 | 仍是旧基线版本，数字与导出文件名里的批次号一致 | 黄色「重算中」，提示当前仍用旧基线 |
| 重算成功提交 | 结果落库与基线切换在**同一事务**，提交后统一指向新版本 | 提示消失，基线批次号更新 |
| 重算失败 | 基线与旧结果原样保留，一行不丢；`run_attempts` 留 `failed` 与错误原因 | 提示消失，继续展示旧基线 |

值班核对命令：

```bash
# 对外版本（页面和所有 /api/export/* 都取这个 batch_id）
curl -s $BASE/api/health | python3 -c 'import json,sys; d=json.load(sys.stdin); \
  print(d["batch_id"], d["baseline_published_at"], "recalc=", d["recalculating"])'

# 重算尝试历史：同批次键的重试只有一行，attempts 累加，不新增、不重复累计
curl -s $BASE/api/batches | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["attempts"], ensure_ascii=False))'

# 导出文件名自带基线批次号，可直接和页面顶栏的「发布基线」对账
curl -s -D - -o /dev/null $BASE/api/export/snapshot.csv | grep -i content-disposition
```

规则细节：

- 同一 `batch_key` 已有 `running` 重算时，再触发返回 409 `B_RERUN_IN_PROGRESS`，
  拒绝并发重算，避免重复累计；
- 失败后按**同一批次键**重试：复用同一 `run_attempts` 行，`attempts + 1`，
  不产生新批次、不写半成品结果；不同批次键才是独立的新版本；
- 同一输入重跑命中已完成批次 → 幂等重放（`B_IDEMPOTENT_REPLAY`），
  不登记重算尝试、不动基线、不新增行；
- 老库升级（无 `baseline` 行）读路径自动回退到最新 `done` 批次，页面不空；
  下一次成功采信会在同事务补上基线行。

