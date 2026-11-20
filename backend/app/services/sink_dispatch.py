"""下沉分发业务规则：状态流转、字段校验与筛选口径都收在这里。

增强：订单聚合表下沉执行引擎——按物化视图的刷新版本把湖表数据切分成批，
为每个批次键写一份分发幂等数据回执（保留批次键与已提交位置）。失败重试
从整批起点改为按回执续推：已提交确认的批次整批跳过，部分提交的批次从
已提交位置继续，只处理尚未完成项；目标系统台账按（目标系统, 批次键,
记录键）去重，保证同一批次键在目标系统只提交一次，ClickHouse 聚合值不重复。

冲突收口：同一批次的部分成功回执与完整重试批次相遇时，"从成功键之后续推"
与"从最早失败键重推"在有序提交模型下指向同一位置（已提交位置即最早未提交
键），回执把选定路线记入 resolution 并递增数据版本，一个批次键只发一次
提交确认（confirmation），重试批次以回执为准收口，不再产生第二份数据版本。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.materialized_view import current_refresh_version
from app.store import store

MODULE = "sink_dispatch"
LIST_FIELDS = ["规则编号", "湖表名", "目标系统", "写入模式", "字段映射", "过滤条件", "批次大小", "失败回退"]
REQUIRED_FIELDS = ["规则编号", "湖表名", "目标系统"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
NEGATIVE_ACTIONS = ["停用规则"]

RECEIPT_TABLE = "sink_dispatch_receipts"
LEDGER_TABLE = "sink_dispatch_target_ledger"

# 模拟源数据规模：真实环境按批次大小从湖表扫描，这里用固定窗口让部分失败可观察
SIMULATED_SOURCE_ROWS = 12
SIMULATED_BATCH_SIZE = 4


class SinkDispatchService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("规则编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"分发规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于下沉分发可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"分发规则已{action}"

    # ───────────────────────────────
    # 分发执行与幂等回执
    # ───────────────────────────────

    def execute_dispatch(
        self, entry_id: int, *, simulate_partial_failure: bool = False
    ) -> tuple[dict[str, Any] | None, str]:
        """执行一次分发：按当前视图刷新版本切批，逐键提交并落回执。

        每个批次先读回执：已提交确认的批次整批跳过；部分提交的批次从已提交
        位置续推；没有回执的批次新建回执从头推送。simulate_partial_failure
        仅在第二个批次（不足两批时为首批）中途注入一次中断，用于验证回执。
        """
        rule = store.find(MODULE, entry_id)
        if rule is None:
            return None, f"分发规则 {entry_id} 不存在或已归档"
        if rule.get("status") != "已生效":
            return None, f"分发规则当前状态为「{rule.get('status')}」，须先标记生效再执行分发"

        lake_table = str(rule.get("湖表名") or "")
        refresh_version = self._resolve_refresh_version(lake_table)
        source_rows = self._source_rows(lake_table, rule)
        batch_size = self._batch_size(rule)
        batches = [source_rows[i:i + batch_size] for i in range(0, len(source_rows), batch_size)]

        run: dict[str, Any] = {
            "rule_id": entry_id,
            "规则编号": rule.get("规则编号"),
            "lake_table": lake_table,
            "target_system": rule.get("目标系统"),
            "view_refresh_version": refresh_version,
            "filter_applied": rule.get("过滤条件"),
            "batches_total": len(batches),
            "committed_now": 0,
            "skipped_committed": 0,
            "resumed_batches": 0,
            "skipped_batches": 0,
            "interrupted": False,
            "failure": None,
        }
        fail_at_seq: int | None = None
        if simulate_partial_failure and batches:
            fail_at_seq = 2 if len(batches) >= 2 else 1

        for seq, batch_rows in enumerate(batches, start=1):
            batch_key = f"{rule.get('规则编号')}@{refresh_version}#B{seq:02d}"
            receipt = self._load_receipt(batch_key)
            if receipt is None:
                receipt = self._create_receipt(rule, batch_key, seq, refresh_version, batch_rows)
            if receipt.get("status") == "已提交" and receipt.get("confirmation"):
                # 一个批次键只能有一个提交确认：已确认的批次整批跳过
                run["skipped_batches"] += 1
                continue

            receipt["attempts"] = int(receipt.get("attempts", 0)) + 1
            if receipt.get("status") == "部分提交":
                # 冲突收口：部分成功回执与完整重试批次相遇，从已提交位置续推
                run["resumed_batches"] += 1
                receipt["resolution"] = "续推未提交键"
                receipt["conflict"] = "部分成功回执与完整重试批次相遇"

            position = int(receipt.get("committed_position", 0))
            interrupted = False
            while position < len(batch_rows):
                if fail_at_seq == seq and position >= max(1, len(batch_rows) // 2):
                    interrupted = True
                    break
                if self._commit_key(receipt, batch_rows[position]):
                    run["committed_now"] += 1
                else:
                    run["skipped_committed"] += 1
                position += 1

            # 回执落盘：保留批次键与已提交位置，递增回执数据版本
            receipt["committed_position"] = position
            receipt["committed_keys"] = [row["record_key"] for row in batch_rows[:position]]
            receipt["pending_keys"] = [row["record_key"] for row in batch_rows[position:]]
            receipt["version"] = int(receipt.get("version", 0)) + 1
            receipt["updated_at"] = datetime.now().isoformat()
            if position >= len(batch_rows):
                receipt["status"] = "已提交"
                if not receipt.get("confirmation"):
                    receipt["confirmation"] = {
                        "confirmed_at": receipt["updated_at"],
                        "confirm_seq": self._next_confirm_seq(),
                        "route": receipt.get("resolution") or "整批提交",
                    }
            else:
                receipt["status"] = "部分提交"

            if interrupted:
                run["interrupted"] = True
                run["failure"] = {
                    "batch_key": batch_key,
                    "committed_position": position,
                    "total_keys": len(batch_rows),
                    "reason": "目标系统连接中断（模拟部分失败）",
                }
                break

        run["receipts"] = self.list_receipts(entry_id)
        run["ledger"] = self._ledger_summary(rule)
        return run, self._dispatch_message(run)

    def retry_dispatch(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """失败重试：恢复分发幂等数据回执，仅处理尚未完成项，保留已提交位置。"""
        return self.execute_dispatch(entry_id, simulate_partial_failure=False)

    def list_receipts(self, entry_id: int) -> list[dict[str, Any]]:
        """该规则的全部分发幂等数据回执：按刷新版本与批次序号排序，不错位不重复。"""
        receipts = [r for r in store.rows(RECEIPT_TABLE) if r.get("rule_id") == entry_id]
        receipts.sort(key=lambda r: (str(r.get("view_refresh_version")), int(r.get("batch_seq", 0))))
        return receipts

    def dispatch_summary(self, entry_id: int) -> dict[str, Any] | None:
        """回执面板数据：业务标识 + 回执列表 + 目标系统台账汇总。"""
        rule = store.find(MODULE, entry_id)
        if rule is None:
            return None
        return {
            "rule": {
                "id": rule.get("id"),
                "规则编号": rule.get("规则编号"),
                "湖表名": rule.get("湖表名"),
                "目标系统": rule.get("目标系统"),
                "status": rule.get("status"),
            },
            "receipts": self.list_receipts(entry_id),
            "ledger": self._ledger_summary(rule),
        }

    # ───────────────────────────────
    # 内部辅助
    # ───────────────────────────────

    def _resolve_refresh_version(self, lake_table: str) -> str:
        """本次推送使用的视图刷新版本：取湖表对应物化视图的当前版本。"""
        for mv in store.rows("materialized_view"):
            if mv.get("基表名") == lake_table:
                return current_refresh_version(mv)
        return "未关联物化视图"

    def _source_rows(self, lake_table: str, rule: dict[str, Any]) -> list[dict[str, Any]]:
        """按分发规则读取订单聚合表：确定性生成源数据并应用过滤条件。"""
        rows = []
        for i in range(SIMULATED_SOURCE_ROWS):
            order_key = f"ORDER-{i + 1:04d}"
            rows.append({
                "record_key": f"{lake_table}#{order_key}",
                "order_key": order_key,
                "aggregate_value": 100 + 10 * i,
            })
        return self._apply_filter(rows, str(rule.get("过滤条件") or ""))

    def _apply_filter(self, rows: list[dict[str, Any]], condition: str) -> list[dict[str, Any]]:
        """应用规则过滤条件（支持 amount> / amount>= 阈值），纠正读取口径。"""
        condition = condition.strip()
        for op in (">=", ">"):
            token = f"amount{op}"
            if token in condition:
                try:
                    threshold = float(condition.split(token, 1)[1].strip())
                except ValueError:
                    return rows
                if op == ">=":
                    return [row for row in rows if row["aggregate_value"] >= threshold]
                return [row for row in rows if row["aggregate_value"] > threshold]
        return rows

    def _batch_size(self, rule: dict[str, Any]) -> int:
        try:
            configured = int(str(rule.get("批次大小") or "").strip())
        except ValueError:
            configured = 0
        if configured <= 0:
            return SIMULATED_BATCH_SIZE
        return max(1, min(configured, SIMULATED_BATCH_SIZE))

    def _load_receipt(self, batch_key: str) -> dict[str, Any] | None:
        for receipt in store.rows(RECEIPT_TABLE):
            if receipt.get("batch_key") == batch_key:
                return receipt
        return None

    def _create_receipt(
        self,
        rule: dict[str, Any],
        batch_key: str,
        seq: int,
        refresh_version: str,
        batch_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        receipts = store.rows(RECEIPT_TABLE)
        receipt = {
            "receipt_id": f"RECV-{len(receipts) + 1:04d}",
            "rule_id": rule.get("id"),
            "规则编号": rule.get("规则编号"),
            "lake_table": rule.get("湖表名"),
            "target_system": rule.get("目标系统"),
            "batch_key": batch_key,
            "batch_seq": seq,
            "view_refresh_version": refresh_version,
            "total_keys": len(batch_rows),
            "committed_position": 0,
            "committed_keys": [],
            "pending_keys": [row["record_key"] for row in batch_rows],
            "status": "待推送",
            "attempts": 0,
            "version": 0,
            "resolution": None,
            "conflict": None,
            "confirmation": None,
            "created_at": datetime.now().isoformat(),
            "updated_at": None,
        }
        receipts.append(receipt)
        return receipt

    def _commit_key(self, receipt: dict[str, Any], row: dict[str, Any]) -> bool:
        """向目标系统提交一个键：台账按（目标系统, 批次键, 记录键）去重。

        已提交过的键幂等跳过，保证同一批次键在目标系统只能提交一次。
        """
        ledger = store.rows(LEDGER_TABLE)
        for item in ledger:
            if (
                item.get("target_system") == receipt.get("target_system")
                and item.get("batch_key") == receipt.get("batch_key")
                and item.get("record_key") == row.get("record_key")
            ):
                return False
        ledger.append({
            "commit_seq": len(ledger) + 1,
            "target_system": receipt.get("target_system"),
            "batch_key": receipt.get("batch_key"),
            "record_key": row.get("record_key"),
            "order_key": row.get("order_key"),
            "aggregate_value": row.get("aggregate_value"),
            "view_refresh_version": receipt.get("view_refresh_version"),
            "committed_at": datetime.now().isoformat(),
        })
        return True

    def _next_confirm_seq(self) -> int:
        return sum(1 for r in store.rows(RECEIPT_TABLE) if r.get("confirmation")) + 1

    def _ledger_summary(self, rule: dict[str, Any]) -> dict[str, Any]:
        prefix = f"{rule.get('规则编号')}@"
        entries = [
            item for item in store.rows(LEDGER_TABLE)
            if str(item.get("batch_key", "")).startswith(prefix)
        ]
        seen: set[tuple[str, str]] = set()
        duplicates = 0
        for item in entries:
            key = (str(item.get("batch_key")), str(item.get("record_key")))
            if key in seen:
                duplicates += 1
            seen.add(key)
        return {
            "target_system": rule.get("目标系统"),
            "committed_keys": len(entries),
            "aggregate_total": sum(int(item.get("aggregate_value", 0)) for item in entries),
            "duplicate_commits": duplicates,
        }

    def _dispatch_message(self, run: dict[str, Any]) -> str:
        ledger = run["ledger"]
        if run["interrupted"]:
            failure = run["failure"]
            return (
                f"分发中断：批次 {failure['batch_key']} 已提交位置 "
                f"{failure['committed_position']}/{failure['total_keys']}，"
                "回执已保留批次键与已提交位置，重试将仅续推未提交键"
            )
        if run["resumed_batches"]:
            return (
                f"重试续推完成：{run['resumed_batches']} 个部分提交批次从已提交位置续推，"
                f"目标系统聚合值 {ledger['aggregate_total']}（仅增加一次）"
            )
        if run["skipped_batches"] == run["batches_total"]:
            return f"所有批次键均已提交确认，本次无需推送，目标系统聚合值 {ledger['aggregate_total']}"
        return f"分发完成：{run['batches_total']} 个批次全部提交确认，目标系统聚合值 {ledger['aggregate_total']}"
