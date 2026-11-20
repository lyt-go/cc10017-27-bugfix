"""下沉分发幂等回执测试：部分失败→重试续推→目标系统只提交一次的完整链路断言。"""
from __future__ import annotations

import unittest

from app.pipeline_engine import pipeline_engine
from app.services.materialized_view import MaterializedViewService
from app.services.sink_dispatch import SinkDispatchService
from app.store import store

RECEIPT_TABLE = "sink_dispatch_receipts"
LEDGER_TABLE = "sink_dispatch_target_ledger"

# 模拟源数据：12 个订单键，聚合值 100,110,...,210，总和 1860
EXPECTED_TOTAL = sum(100 + 10 * i for i in range(12))


class TestDispatchIdempotency(unittest.TestCase):
    """订单聚合表（SINK-0001 → ClickHouse）失败重试的幂等收口。"""

    def setUp(self) -> None:
        store.rows(RECEIPT_TABLE).clear()
        store.rows(LEDGER_TABLE).clear()
        self.svc = SinkDispatchService()
        self.mv_svc = MaterializedViewService()

    def test_partial_failure_then_retry_commits_each_key_once(self) -> None:
        """部分失败保留已提交位置，重试只续推未提交键，聚合值只增加一次。"""
        run, msg = self.svc.execute_dispatch(1, simulate_partial_failure=True)
        self.assertIsNotNone(run)
        self.assertTrue(run["interrupted"])
        self.assertIn("已提交位置", msg)
        self.assertEqual(run["batches_total"], 3)

        by_seq = {r["batch_seq"]: r for r in run["receipts"]}
        # 第一批整批提交确认，第二批在 2/4 处中断，回执保留批次键与已提交位置
        self.assertEqual(by_seq[1]["status"], "已提交")
        self.assertEqual(by_seq[1]["committed_position"], 4)
        self.assertEqual(by_seq[2]["status"], "部分提交")
        self.assertEqual(by_seq[2]["committed_position"], 2)
        self.assertEqual(len(by_seq[2]["pending_keys"]), 2)
        self.assertEqual(run["ledger"]["committed_keys"], 6)
        partial_total = run["ledger"]["aggregate_total"]

        run2, msg2 = self.svc.retry_dispatch(1)
        self.assertIsNotNone(run2)
        self.assertFalse(run2["interrupted"])
        self.assertIn("续推", msg2)
        # 目标系统聚合值只能增加一次：12 个键各提交一次，无重复
        self.assertEqual(run2["ledger"]["committed_keys"], 12)
        self.assertEqual(run2["ledger"]["aggregate_total"], EXPECTED_TOTAL)
        self.assertEqual(run2["ledger"]["duplicate_commits"], 0)
        self.assertGreater(run2["ledger"]["aggregate_total"], partial_total)
        for receipt in run2["receipts"]:
            self.assertEqual(receipt["status"], "已提交")
            self.assertIsNotNone(receipt["confirmation"])

    def test_retry_is_idempotent_and_skips_confirmed_batches(self) -> None:
        """重复重试不改变台账：已确认批次整批跳过，提交确认只有一个。"""
        self.svc.execute_dispatch(1, simulate_partial_failure=True)
        run1, _ = self.svc.retry_dispatch(1)
        confirmations = {r["batch_key"]: r["confirmation"]["confirm_seq"] for r in run1["receipts"]}
        attempts = {r["batch_key"]: r["attempts"] for r in run1["receipts"]}

        run2, msg2 = self.svc.retry_dispatch(1)
        self.assertEqual(run2["committed_now"], 0)
        self.assertEqual(run2["skipped_batches"], 3)
        self.assertIn("无需推送", msg2)
        self.assertEqual(run2["ledger"]["committed_keys"], 12)
        self.assertEqual(run2["ledger"]["aggregate_total"], EXPECTED_TOTAL)
        for receipt in run2["receipts"]:
            # 一个批次键只能有一个提交确认，重试不再改写确认与尝试次数
            self.assertEqual(receipt["confirmation"]["confirm_seq"], confirmations[receipt["batch_key"]])
            self.assertEqual(receipt["attempts"], attempts[receipt["batch_key"]])

    def test_resume_preserves_committed_position(self) -> None:
        """中断恢复后保留已提交位置：部分成功回执与完整重试批次相遇时续推收口。"""
        run, _ = self.svc.execute_dispatch(1, simulate_partial_failure=True)
        partial = next(r for r in run["receipts"] if r["status"] == "部分提交")
        committed_before = list(partial["committed_keys"])

        run2, _ = self.svc.retry_dispatch(1)
        resumed = next(r for r in run2["receipts"] if r["batch_key"] == partial["batch_key"])
        # 已提交位置保留：原已提交键仍在回执已提交列表前部，续推只补未提交键
        self.assertEqual(resumed["committed_keys"][: len(committed_before)], committed_before)
        self.assertEqual(resumed["committed_position"], resumed["total_keys"])
        self.assertEqual(resumed["resolution"], "续推未提交键")
        self.assertEqual(resumed["conflict"], "部分成功回执与完整重试批次相遇")
        self.assertEqual(resumed["confirmation"]["route"], "续推未提交键")
        # 该批次键在目标系统的每个记录键只提交一次
        ledger = [l for l in store.rows(LEDGER_TABLE) if l["batch_key"] == partial["batch_key"]]
        record_keys = [l["record_key"] for l in ledger]
        self.assertEqual(len(record_keys), len(set(record_keys)))

    def test_receipt_unique_per_batch_key(self) -> None:
        """重复执行不产生重复或错位回执：一个批次键只有一份回执。"""
        self.svc.execute_dispatch(1)
        self.svc.execute_dispatch(1)
        receipts = self.svc.list_receipts(1)
        batch_keys = [r["batch_key"] for r in receipts]
        self.assertEqual(len(batch_keys), len(set(batch_keys)))
        self.assertEqual(len(receipts), 3)
        seqs = [r["batch_seq"] for r in receipts]
        self.assertEqual(seqs, sorted(seqs))

    def test_materialized_view_verifies_refresh_version(self) -> None:
        """物化查询读取回执并核对刷新版本；视图刷新后旧回执核对为滞后。"""
        run, _ = self.svc.execute_dispatch(1)
        version_used = run["view_refresh_version"]

        result, msg = self.mv_svc.verify_dispatch_version(1)
        self.assertIsNotNone(result)
        self.assertEqual(result["当前刷新版本"], version_used)
        self.assertTrue(all(c["version_match"] for c in result["receipts"]))
        self.assertTrue(all(c["核对结果"] == "一致" for c in result["receipts"]))
        self.assertEqual(result["ledger"]["aggregate_total"], EXPECTED_TOTAL)
        self.assertEqual(result["ledger"]["duplicate_commits"], 0)

        # 血缘驱动物化视图刷新 → 版本递增 → 旧回执核对为滞后
        refresh = pipeline_engine.refresh_materialized_from_lineage(1)
        self.assertTrue(refresh["ok"])
        result2, _ = self.mv_svc.verify_dispatch_version(1)
        self.assertNotEqual(result2["当前刷新版本"], version_used)
        self.assertTrue(all(c["核对结果"] == "滞后" for c in result2["receipts"]))

        # 新版本再次分发：生成新批次键，回执不重复，物化查询读到新版本
        run2, _ = self.svc.execute_dispatch(1)
        self.assertEqual(run2["view_refresh_version"], result2["当前刷新版本"])
        all_keys = [r["batch_key"] for r in self.svc.list_receipts(1)]
        self.assertEqual(len(all_keys), len(set(all_keys)))
        result3, _ = self.mv_svc.verify_dispatch_version(1)
        latest = [c for c in result3["receipts"] if c["receipt_refresh_version"] == result3["当前刷新版本"]]
        self.assertTrue(all(c["核对结果"] == "一致" for c in latest))
        self.assertEqual(len(latest), 3)

    def test_business_identifiers_preserved_after_dispatch(self) -> None:
        """分发与重试不改写规则业务标识，回执引用的规则编号与下游一致。"""
        before = dict(store.find("sink_dispatch", 1))
        self.svc.execute_dispatch(1, simulate_partial_failure=True)
        self.svc.retry_dispatch(1)
        after = store.find("sink_dispatch", 1)
        for field in ["规则编号", "湖表名", "目标系统", "写入模式", "字段映射", "过滤条件", "批次大小", "失败回退"]:
            self.assertEqual(after.get(field), before.get(field))
        for receipt in self.svc.list_receipts(1):
            self.assertEqual(receipt["规则编号"], before["规则编号"])
            self.assertEqual(receipt["lake_table"], before["湖表名"])
            self.assertEqual(receipt["target_system"], before["目标系统"])

    def test_dispatch_requires_active_rule(self) -> None:
        """草稿规则不允许执行分发，并给出可读原因。"""
        result, msg = self.svc.execute_dispatch(3)
        self.assertIsNone(result)
        self.assertIn("标记生效", msg)
        result2, msg2 = self.svc.execute_dispatch(9999)
        self.assertIsNone(result2)
        self.assertIn("不存在", msg2)


if __name__ == "__main__":
    unittest.main()
