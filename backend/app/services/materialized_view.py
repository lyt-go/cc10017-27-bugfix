"""物化查询业务规则：状态流转、字段校验与筛选口径都收在这里。

增强：物化视图持有刷新版本；核对环节跨模块读取下沉分发写入的
分发幂等数据回执，逐批次比对本次推送使用的视图刷新版本。
"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "materialized_view"
LIST_FIELDS = ["视图编号", "查询SQL", "基表名", "刷新模式", "刷新间隔", "存储位置", "数据量", "最后刷新"]
REQUIRED_FIELDS = ["视图编号", "查询SQL", "基表名"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用刷新": "已启用", "停用视图": "已停用"}
NEGATIVE_ACTIONS = ["停用视图"]

RECEIPT_TABLE = "sink_dispatch_receipts"
LEDGER_TABLE = "sink_dispatch_target_ledger"


def current_refresh_version(entry: dict[str, Any]) -> str:
    """视图当前刷新版本：取刷新日志里该视图的最新版本，无日志时为初始版本。"""
    mv_id = entry.get("id")
    versions = [
        str(log.get("refresh_version"))
        for log in store.rows("materialized_refresh_log")
        if log.get("mv_id") == mv_id and log.get("refresh_version")
    ]
    if versions:
        return versions[-1]
    return f"MV{mv_id}-v0"


class MaterializedViewService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("视图编号", "")).casefold()]
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
            return None, f"物化视图 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于物化查询可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"物化视图已{action}"

    def verify_dispatch_version(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """核对刷新版本：读取下沉分发写入的分发幂等数据回执，逐批次比对视图刷新版本。

        下沉分发跨模块写入回执后，物化查询在这里读取该版本；同时汇总目标系统
        台账，校验同一批次键只提交过一次、聚合值没有重复累加。
        """
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"物化视图 {entry_id} 不存在或已归档"
        base_table = str(entry.get("基表名") or "")
        version = current_refresh_version(entry)
        entry["刷新版本"] = version

        receipts = [
            receipt for receipt in store.rows(RECEIPT_TABLE)
            if receipt.get("lake_table") == base_table
        ]
        receipts.sort(key=lambda r: (str(r.get("view_refresh_version")), int(r.get("batch_seq", 0))))
        checks = []
        for receipt in receipts:
            match = receipt.get("view_refresh_version") == version
            checks.append({
                "receipt_id": receipt.get("receipt_id"),
                "batch_key": receipt.get("batch_key"),
                "规则编号": receipt.get("规则编号"),
                "target_system": receipt.get("target_system"),
                "receipt_refresh_version": receipt.get("view_refresh_version"),
                "committed_position": receipt.get("committed_position"),
                "total_keys": receipt.get("total_keys"),
                "status": receipt.get("status"),
                "receipt_version": receipt.get("version"),
                "confirmation": receipt.get("confirmation"),
                "version_match": match,
                "核对结果": "一致" if match else "滞后",
            })

        ledger_entries = [
            item for item in store.rows(LEDGER_TABLE)
            if str(item.get("record_key", "")).startswith(f"{base_table}#")
        ]
        seen: set[tuple[str, str]] = set()
        duplicates = 0
        for item in ledger_entries:
            key = (str(item.get("batch_key")), str(item.get("record_key")))
            if key in seen:
                duplicates += 1
            seen.add(key)

        result = {
            "视图编号": entry.get("视图编号"),
            "基表名": base_table,
            "当前刷新版本": version,
            "receipts": checks,
            "ledger": {
                "committed_keys": len(ledger_entries),
                "aggregate_total": sum(int(item.get("aggregate_value", 0)) for item in ledger_entries),
                "duplicate_commits": duplicates,
            },
        }
        return result, "分发幂等数据回执与视图刷新版本核对完成"
