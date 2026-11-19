from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified
from fastapi import HTTPException
from src.models.entities import Scenario, Stream
from src.services.node_service import list_nodes
from src.services import resumption
from src.utils.serialize import record

MAX_STREAMS = 20

def create_stream(db, payload):
    scenario = db.get(Scenario, payload.scenario_id)
    if not scenario: raise HTTPException(404, '故障剧本不存在')
    if len(db.scalars(select(Stream.id)).all()) >= MAX_STREAMS:
        raise HTTPException(429, f'续接演练配额{MAX_STREAMS}已满，请先复核既有记录')
    nodes = list_nodes(db)
    leaders = [n for n in nodes if n['role'] == 'leader']
    if len(leaders) != 1: raise HTTPException(422, '节点配置必须且只能有一个主节点')
    replica = next((n for n in nodes if n['name'] == payload.replica_name), None)
    if replica is None or replica['role'] != 'follower':
        raise HTTPException(422, '接任副本须为已配置的跟随节点')
    names = []
    for raw in payload.subscribers:
        name = raw.strip()
        if name and name not in names: names.append(name)
    if not names: raise HTTPException(422, '至少声明一个订阅方')
    state = resumption.initial_state(nodes, record(scenario), payload.replica_name, names)
    obj = Stream(name=payload.name.strip(), scenario_id=scenario.id, phase=state['phase'], state=state)
    db.add(obj); db.flush(); return obj

def list_streams(db):
    out = []
    for s in db.scalars(select(Stream).order_by(Stream.id.desc()).limit(100)):
        row = record(s)
        st = row.pop('state') or {}
        row.update(term=st.get('term'), primary_name=st.get('primary_name'), replica_name=st.get('replica_name'),
                   subscribers=len(st.get('subscribers', [])), changes=len(st.get('log', [])),
                   pending_receipts=len(st.get('receipts', [])))
        out.append(row)
    return out

def get_stream(db, stream_id):
    obj = db.get(Stream, stream_id)
    if obj is None: raise HTTPException(404, '续接演练不存在')
    return obj

def _mutate(db, stream_id, op):
    obj = get_stream(db, stream_id)
    try:
        op(obj.state)
    except ValueError as exc:
        raise HTTPException(409, str(exc))
    obj.phase = obj.state['phase']
    flag_modified(obj, 'state')
    db.flush(); return obj

def emit(db, stream_id, order, text):
    return _mutate(db, stream_id, lambda s: resumption.emit_change(s, order.strip(), text.strip()))
def set_replication(db, stream_id, on):
    return _mutate(db, stream_id, lambda s: resumption.set_replication(s, on))
def set_paused(db, stream_id, name, paused):
    return _mutate(db, stream_id, lambda s: resumption.set_paused(s, name, paused))
def deliver(db, stream_id):
    return _mutate(db, stream_id, resumption.deliver_round)
def collect(db, stream_id):
    return _mutate(db, stream_id, resumption.collect_receipts)
def drop_receipt(db, stream_id, receipt_id):
    return _mutate(db, stream_id, lambda s: resumption.drop_receipt(s, receipt_id))
def failover(db, stream_id):
    return _mutate(db, stream_id, resumption.failover)
