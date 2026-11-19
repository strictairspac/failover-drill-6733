"""订阅续接演练模型：切主后订单变更流的撤回、补齐与续送。

纯函数操作 JSON 状态，不连接真实复制集群。沿用 election.elect 的原切主规则。
安全不变量：
- 同一订单：已应用的分叉变更先撤回，再交付该订单的新变更；
- 其他订单照常送达，不被无关订单的撤回阻塞；
- 两边共有的变更按订阅方各自的游标补齐，不重复、不跳过；
- 迟到的旧主回执只记录在案，不推进游标、不跳过待送记录；
- 回执丢失后靠重试恢复，订阅方按变更号幂等去重，不重复应用。
"""
from src.services.election import elect

MODEL_VERSION = 'subscription-resumption-v1'
MAX_CHANGES = 200


def initial_state(nodes, scenario, replica_name, subscribers):
    """冻结节点与剧本，建立演练流。节点日志位置从 0 起算，与节点配置页解耦。"""
    sim_nodes = []
    for n in nodes:
        node = dict(n)
        node['log_position'] = 0
        sim_nodes.append(node)
    primary = next(n['name'] for n in sim_nodes if n['role'] == 'leader')
    state = {'version': MODEL_VERSION, 'phase': 'streaming',
             'term': max(n['term'] for n in sim_nodes),
             'primary_name': primary, 'replica_name': replica_name,
             'nodes': sim_nodes, 'scenario': scenario,
             'seq': 0, 'receipt_seq': 0, 'event_seq': 0,
             'log': [], 'replica_position': 0, 'replication_on': True,
             'old_log': None, 'forked': [], 'last_election': None,
             'subscribers': [{'name': name, 'paused': False, 'applied': [], 'applied_ids': [],
                              'retracted': [], 'next_lsn': 1, 'retract_queue': [], 'awaiting': None}
                             for name in subscribers],
             'receipts': [], 'timeline': []}
    _event(state, 'init', f"演练流建立：主 {primary} → 接任副本 {replica_name}，订阅方 {len(subscribers)} 个，日志位置从 0 起算")
    return state


def emit_change(state, order, text):
    """主节点发出一条订单变更；复制正常时副本同步追平。"""
    if len(state['log']) >= MAX_CHANGES:
        raise ValueError(f'单场次变更数上限 {MAX_CHANGES}')
    state['seq'] += 1
    change = {'lsn': len(state['log']) + 1, 'id': f"c-{state['seq']}",
              'order': order, 'text': text, 'term': state['term']}
    state['log'].append(change)
    writer = state['primary_name'] if state['phase'] == 'streaming' else state['replica_name']
    _node(state, writer)['log_position'] = len(state['log'])
    _event(state, 'emit', f"{writer} 发出变更 {change['id']}（{order} · {text}），LSN {change['lsn']}，任期 {change['term']}")
    if state['phase'] == 'streaming':
        if state['replication_on']:
            _sync_replica(state)
            _event(state, 'replicate', f"变更 {change['id']} 已复制到副本 {state['replica_name']}（{state['replica_position']} 条）")
        else:
            _event(state, 'replicate', f"复制中断中：{change['id']} 未到达副本 {state['replica_name']}（仍 {state['replica_position']} 条）")
    return state


def set_replication(state, on):
    """中断或恢复主到副本的复制链路，用于制造副本落后。"""
    if state['phase'] != 'streaming':
        raise ValueError('已完成切主，复制链路演练结束')
    on = bool(on)
    if on == state['replication_on']:
        return state
    state['replication_on'] = on
    if on:
        _sync_replica(state)
        _event(state, 'replicate', f"复制恢复：副本 {state['replica_name']} 追平到 {state['replica_position']} 条")
    else:
        _event(state, 'replicate', f"复制链路中断：副本 {state['replica_name']} 停留在 {state['replica_position']} 条，主继续接单")
    return state


def set_paused(state, name, paused):
    """暂停或恢复某个订阅方，制造订阅方之间的进度差。"""
    sub = _subscriber(state, name)
    paused = bool(paused)
    if paused != sub['paused']:
        sub['paused'] = paused
        _event(state, 'pause', f"订阅方 {name} {'暂停接收，待送记录开始积压' if paused else '恢复接收'}")
    return state


def deliver_round(state):
    """投递一轮：每个订阅方至多一条。有待确认投递且回执不在通道时按超时重试。"""
    sent = 0
    for sub in state['subscribers']:
        if sub['paused']:
            if sub['awaiting'] or sub['retract_queue'] or sub['next_lsn'] <= len(state['log']):
                _event(state, 'pause', f"{sub['name']} 已暂停，待送记录继续积压")
            continue
        awaiting = sub['awaiting']
        if awaiting:
            inflight = any(r for r in state['receipts'] if r['subscriber'] == sub['name']
                           and r['change_id'] == awaiting['change']['id'] and r['kind'] == awaiting['kind'])
            if inflight:
                _event(state, 'receipt', f"{sub['name']} 的回执仍在回执通道，先收取再续送")
            else:
                _event(state, 'retry', f"{sub['name']} 的回执超时未到，重试 {awaiting['change']['id']}")
                _apply(state, sub, awaiting, retry=True)
                sent += 1
            continue
        msg = _next_message(state, sub)
        if not msg:
            continue
        if msg['kind'] == 'retract':
            sub['retract_queue'] = [f for f in sub['retract_queue'] if f['id'] != msg['change']['id']]
        sub['awaiting'] = msg
        _apply(state, sub, msg)
        sent += 1
    if not sent:
        _event(state, 'deliver', '投递一轮：没有新的待送记录')
    return state


def collect_receipts(state):
    """收取回执：只有与待确认投递完全匹配的回执才推进游标。

    迟到的旧主回执或重复回执只记录在案，不跳过任何待送记录。
    """
    if not state['receipts']:
        _event(state, 'receipt', '回执通道为空，没有可收取的回执')
        return state
    for receipt in list(state['receipts']):
        state['receipts'].remove(receipt)
        sub = _subscriber(state, receipt['subscriber'])
        awaiting = sub['awaiting']
        if awaiting and awaiting['change']['id'] == receipt['change_id'] and awaiting['kind'] == receipt['kind']:
            sub['awaiting'] = None
            if receipt['kind'] == 'change':
                sub['next_lsn'] += 1
                _event(state, 'collect', f"回执 {receipt['id']} 确认：{sub['name']} 游标推进到 LSN {sub['next_lsn']}")
            else:
                _event(state, 'collect', f"撤回回执 {receipt['id']} 确认：{sub['name']} 的 {receipt['change_id']} 撤回完成")
        else:
            _event(state, 'late', f"回执 {receipt['id']}（{receipt['change_id']}，任期 {receipt['term']}）是迟到的旧主回执或重复回执：记录在案，不跳过任何待送记录")
    return state


def drop_receipt(state, receipt_id):
    """模拟回执在传输中丢失：订阅方已应用但主未确认，靠重试去重恢复。"""
    receipt = next((r for r in state['receipts'] if r['id'] == receipt_id), None)
    if receipt is None:
        raise ValueError(f'回执 {receipt_id} 不存在或已收取')
    state['receipts'].remove(receipt)
    _event(state, 'lost', f"回执 {receipt['id']}（{receipt['change_id']}）在传输中丢失：{receipt['subscriber']} 已应用但主未确认，将靠重试去重恢复")
    return state


def failover(state):
    """沿用原切主规则选主，通过后接任副本升主并续接各订阅方。

    新主日志以副本为准截断；续接握手按各订阅方实际上报的应用位点
    计算分叉撤回队列与续送游标。
    """
    if state['phase'] != 'streaming':
        raise ValueError('本场演练已完成切主')
    result = elect(state['nodes'], state['scenario'])
    state['last_election'] = result
    if result['decision'] != 'approved':
        failed = next(c for c in result['checks'] if not c['pass'])
        _event(state, 'blocked', f"切主被安全规则阻断：{failed['rule']}（{failed['evidence']}）")
        return state
    if result['winner'] != state['replica_name']:
        _event(state, 'blocked', f"按原切主规则当选节点为 {result['winner']}，并非演练副本 {state['replica_name']}，续接演练不执行")
        return state
    p = state['replica_position']
    state['old_log'] = state['log']
    state['log'] = state['log'][:p]
    state['forked'] = state['old_log'][p:]
    state['term'] = result['new_term']
    state['phase'] = 'promoted'
    _node(state, state['replica_name'])['log_position'] = len(state['log'])
    _event(state, 'failover', f"切主完成：{state['replica_name']} 接任，新任期 {state['term']}，日志以副本为准截断到 {p} 条，{len(state['forked'])} 条旧主分叉变更被隔离")
    for sub in state['subscribers']:
        if sub['awaiting']:
            _event(state, 'failover', f"{sub['name']} 有一条旧任期未确认投递（{sub['awaiting']['change']['id']}），随旧主作废")
            sub['awaiting'] = None
        applied = len(sub['applied'])
        forked_applied = sub['applied'][p:]
        sub['retract_queue'] = list(reversed(forked_applied))
        sub['next_lsn'] = min(applied, p) + 1
        _event(state, 'handshake', f"续接握手 · {sub['name']}：已应用 {applied} 条，其中分叉 {len(forked_applied)} 条待撤回，从 LSN {sub['next_lsn']} 续送")
    return state


def _next_message(state, sub):
    """订阅方的下一条待送：游标处订单有未撤回分叉时先撤回，否则送游标处变更；
    日志已追平时才处理无新变更订单的悬挂撤回。其他订单不被无关撤回阻塞。"""
    log = state['log']
    if sub['next_lsn'] <= len(log):
        change = log[sub['next_lsn'] - 1]
        for fork in sub['retract_queue']:
            if fork['order'] == change['order']:
                return {'kind': 'retract', 'change': fork, 'term': state['term']}
        return {'kind': 'change', 'change': change, 'term': change['term']}
    if sub['retract_queue']:
        return {'kind': 'retract', 'change': sub['retract_queue'][0], 'term': state['term']}
    return None


def _apply(state, sub, msg, retry=False):
    """订阅方应用投递并按变更号幂等去重，随后出具回执。"""
    change = msg['change']
    verb = '重试投递' if retry else '投递'
    if msg['kind'] == 'change':
        if change['id'] in sub['applied_ids']:
            _event(state, 'dedupe', f"{verb} {change['id']} → {sub['name']}：订阅方按变更号去重，未重复应用")
        else:
            sub['applied'].append(change)
            sub['applied_ids'].append(change['id'])
            _event(state, 'deliver', f"{verb} {change['id']}（{change['order']} · {change['text']}）→ {sub['name']}：已应用")
    else:
        if change['id'] in sub['retracted']:
            _event(state, 'dedupe', f"{verb}撤回 {change['id']} → {sub['name']}：已撤回过，去重跳过")
        elif change['id'] not in sub['applied_ids']:
            _event(state, 'dedupe', f"{verb}撤回 {change['id']} → {sub['name']}：该变更未应用过，幂等跳过")
        else:
            sub['retracted'].append(change['id'])
            _event(state, 'retract', f"{verb}撤回 {change['id']}（{change['order']}）→ {sub['name']}：分叉变更已撤回")
    receipt = _issue_receipt(state, sub, msg)
    _event(state, 'receipt', f"{sub['name']} 出具回执 {receipt['id']}（{change['id']}），等待收取")


def _issue_receipt(state, sub, msg):
    state['receipt_seq'] += 1
    receipt = {'id': f"r-{state['receipt_seq']}", 'subscriber': sub['name'],
               'change_id': msg['change']['id'], 'kind': msg['kind'],
               'order': msg['change']['order'], 'term': msg['term']}
    state['receipts'].append(receipt)
    return receipt


def _sync_replica(state):
    state['replica_position'] = len(state['log'])
    _node(state, state['replica_name'])['log_position'] = state['replica_position']


def _node(state, name):
    return next(n for n in state['nodes'] if n['name'] == name)


def _subscriber(state, name):
    sub = next((s for s in state['subscribers'] if s['name'] == name), None)
    if sub is None:
        raise ValueError(f'订阅方 {name} 不在本场演练中')
    return sub


def _event(state, kind, text):
    state['event_seq'] += 1
    state['timeline'].append({'id': state['event_seq'], 'kind': kind, 'text': text})
