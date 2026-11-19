"""订阅续接核心的规则测试：分叉撤回、按进度补齐、迟到回执、幂等重试。"""
import copy
import unittest
from src.services import resumption

NODES = [
    {'id': 1, 'name': 'hs-primary', 'role': 'leader', 'term': 6, 'log_position': 0,
     'available': True, 'fenced': False, 'vote_enabled': True},
    {'id': 2, 'name': 'hs-east', 'role': 'follower', 'term': 6, 'log_position': 0,
     'available': True, 'fenced': False, 'vote_enabled': True},
    {'id': 3, 'name': 'hs-west', 'role': 'follower', 'term': 6, 'log_position': 0,
     'available': True, 'fenced': False, 'vote_enabled': True},
]
SCENARIO = {'id': 1, 'name': '续接', 'fail_primary': True, 'fence_primary': True,
            'max_data_loss': 5, 'required_term': 7}


def make(subs=('对账中心', '通知服务'), scenario=None):
    return resumption.initial_state(copy.deepcopy(NODES), dict(scenario or SCENARIO), 'hs-east', list(subs))


def emit(state, *changes):
    for order, text in changes:
        resumption.emit_change(state, order, text)


def roundtrip(state, rounds=1):
    for _ in range(rounds):
        resumption.deliver_round(state)
        resumption.collect_receipts(state)


def sub(state, name):
    return next(s for s in state['subscribers'] if s['name'] == name)


def kinds(state, kind):
    return [e for e in state['timeline'] if e['kind'] == kind]


class StreamingTest(unittest.TestCase):
    def test_emit_replicates_and_delivers_in_order(self):
        s = make()
        emit(s, ('ORD-1', '创建'), ('ORD-1', '支付'))
        roundtrip(s, 2)
        for name in ('对账中心', '通知服务'):
            self.assertEqual(sub(s, name)['applied_ids'], ['c-1', 'c-2'])
            self.assertEqual(sub(s, name)['next_lsn'], 3)
        self.assertEqual(s['replica_position'], 2)

    def test_paused_subscriber_backlogs_then_catches_up(self):
        s = make()
        resumption.set_paused(s, '通知服务', True)
        emit(s, ('ORD-1', '创建'))
        roundtrip(s, 1)
        self.assertEqual(sub(s, '通知服务')['applied_ids'], [])
        self.assertEqual(sub(s, '对账中心')['applied_ids'], ['c-1'])
        resumption.set_paused(s, '通知服务', False)
        roundtrip(s, 1)
        self.assertEqual(sub(s, '通知服务')['applied_ids'], ['c-1'])


class FailoverTest(unittest.TestCase):
    def diverged(self):
        """副本停在 2 条，两个订阅方都应用了 4 条（c-3/c-4 为分叉）。"""
        s = make()
        emit(s, ('ORD-1', '创建'), ('ORD-1', '支付'))
        roundtrip(s, 2)
        resumption.set_replication(s, False)
        emit(s, ('ORD-1', '发货'), ('ORD-2', '创建'))
        roundtrip(s, 2)
        resumption.failover(s)
        return s

    def test_failover_truncates_log_and_handshakes(self):
        s = self.diverged()
        self.assertEqual(s['phase'], 'promoted')
        self.assertEqual(s['term'], 7)
        self.assertEqual([c['id'] for c in s['log']], ['c-1', 'c-2'])
        self.assertEqual([c['id'] for c in s['forked']], ['c-3', 'c-4'])
        for name in ('对账中心', '通知服务'):
            self.assertEqual([c['id'] for c in sub(s, name)['retract_queue']], ['c-4', 'c-3'])
            self.assertEqual(sub(s, name)['next_lsn'], 3)
        self.assertTrue(kinds(s, 'handshake'))

    def test_retract_before_redeliver_same_order(self):
        s = self.diverged()
        emit(s, ('ORD-1', '派送'))  # c-5，新日志 LSN 3，与分叉 c-3 同订单
        resumption.deliver_round(s)
        for name in ('对账中心', '通知服务'):
            self.assertEqual(sub(s, name)['retracted'], ['c-3'])  # 先撤回同订单分叉
            self.assertNotIn('c-5', sub(s, name)['applied_ids'])
        resumption.collect_receipts(s)
        roundtrip(s, 1)  # 再交付该订单新变更
        for name in ('对账中心', '通知服务'):
            self.assertIn('c-5', sub(s, name)['applied_ids'])
        texts = [e['text'] for e in s['timeline']]
        first_retract = next(i for i, t in enumerate(texts) if '撤回 c-3' in t and '对账中心' in t)
        first_new = next(i for i, t in enumerate(texts) if 'c-5' in t and '对账中心' in t and '已应用' in t)
        self.assertLess(first_retract, first_new)

    def test_other_orders_delivered_despite_pending_retract(self):
        s = self.diverged()
        emit(s, ('ORD-3', '创建'))  # c-5，与分叉订单无关
        resumption.deliver_round(s)
        for name in ('对账中心', '通知服务'):
            self.assertIn('c-5', sub(s, name)['applied_ids'])  # 其他订单照常送达
            self.assertEqual(sub(s, name)['retracted'], [])
        roundtrip(s, 3)  # 确认 c-5 后逐轮处理悬挂撤回
        for name in ('对账中心', '通知服务'):
            self.assertEqual(sorted(sub(s, name)['retracted']), ['c-3', 'c-4'])

    def test_backfill_follows_each_subscriber_progress(self):
        s = make()
        resumption.set_paused(s, '通知服务', True)
        emit(s, ('ORD-1', '创建'), ('ORD-2', '创建'), ('ORD-1', '支付'))
        roundtrip(s, 3)  # 对账中心到 3，通知服务停在 0，副本为 3
        resumption.failover(s)
        self.assertEqual(sub(s, '对账中心')['next_lsn'], 4)
        self.assertEqual(sub(s, '通知服务')['next_lsn'], 1)
        resumption.set_paused(s, '通知服务', False)
        emit(s, ('ORD-2', '支付'))
        roundtrip(s, 4)
        self.assertEqual(sub(s, '通知服务')['applied_ids'], ['c-1', 'c-2', 'c-3', 'c-4'])
        self.assertEqual(sub(s, '对账中心')['applied_ids'], ['c-1', 'c-2', 'c-3', 'c-4'])

    def test_late_old_term_receipt_never_skips_pending(self):
        s = make()
        emit(s, ('ORD-1', '创建'), ('ORD-1', '支付'))
        roundtrip(s, 2)
        resumption.set_replication(s, False)
        emit(s, ('ORD-1', '发货'))
        resumption.deliver_round(s)  # 订阅方已应用 c-3，回执留在通道不收取
        resumption.failover(s)       # c-3 成分叉，通道里的旧回执仍在
        emit(s, ('ORD-1', '派送'))   # c-4，新日志 LSN 3
        resumption.deliver_round(s)  # 先送 c-3 的撤回
        resumption.collect_receipts(s)
        for name in ('对账中心', '通知服务'):
            self.assertEqual(sub(s, name)['next_lsn'], 3)  # 迟到回执没有推进游标
            self.assertIsNone(sub(s, name)['awaiting'])
        self.assertTrue(kinds(s, 'late'))
        roundtrip(s, 1)              # 待送的 c-4 没有被跳过
        for name in ('对账中心', '通知服务'):
            self.assertIn('c-4', sub(s, name)['applied_ids'])

    def test_failover_blocked_by_original_rules(self):
        strict = dict(SCENARIO, max_data_loss=0)
        s = make(scenario=strict)
        resumption.set_replication(s, False)
        emit(s, ('ORD-1', '创建'))
        resumption.failover(s)
        self.assertEqual(s['phase'], 'streaming')  # RPO 阻断，不续接
        self.assertEqual(s['last_election']['decision'], 'blocked')
        unfenced = dict(SCENARIO, fence_primary=False)
        s2 = make(scenario=unfenced)
        resumption.failover(s2)
        self.assertEqual(s2['phase'], 'streaming')  # 缺少 fencing 证明阻断


class ReceiptTest(unittest.TestCase):
    def test_lost_receipt_recovers_by_retry_without_reapply(self):
        s = make()
        emit(s, ('ORD-1', '创建'))
        resumption.deliver_round(s)
        rid = s['receipts'][0]['id']
        resumption.drop_receipt(s, rid)
        resumption.deliver_round(s)  # 超时重试
        for name in ('对账中心', '通知服务'):
            self.assertEqual(sub(s, name)['applied_ids'], ['c-1'])  # 不重复应用
        self.assertTrue(kinds(s, 'dedupe'))
        resumption.collect_receipts(s)
        for name in ('对账中心', '通知服务'):
            self.assertEqual(sub(s, name)['next_lsn'], 2)

    def test_retract_retry_dedupes(self):
        s = make()
        emit(s, ('ORD-1', '创建'))
        roundtrip(s, 1)
        resumption.set_replication(s, False)
        emit(s, ('ORD-1', '发货'))
        roundtrip(s, 1)
        resumption.failover(s)
        resumption.deliver_round(s)  # 送出 c-2 撤回
        rid = s['receipts'][0]['id']
        resumption.drop_receipt(s, rid)
        resumption.deliver_round(s)  # 重试撤回
        for name in ('对账中心', '通知服务'):
            self.assertEqual(sub(s, name)['retracted'], ['c-2'])  # 只撤回一次
        resumption.collect_receipts(s)
        self.assertFalse(s['receipts'])

    def test_unknown_receipt_and_subscriber_rejected(self):
        s = make()
        with self.assertRaises(ValueError):
            resumption.drop_receipt(s, 'r-99')
        with self.assertRaises(ValueError):
            resumption.set_paused(s, '不存在的订阅方', True)


if __name__ == '__main__':
    unittest.main()
