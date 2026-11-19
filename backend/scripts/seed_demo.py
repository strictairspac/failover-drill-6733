import os
from sqlalchemy import select
from src.config.database import Base, engine, Session
from src.models.entities import User, Node, Scenario, Stream
from src.utils.security import hash_password

def seed():
    Base.metadata.create_all(engine)
    if os.getenv('SEED_DEMO','true').lower() != 'true': return
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email = role + '@hengshi.local'
            if not db.scalar(select(User).where(User.email == email)):
                db.add(User(email=email, role=role, password_hash=hash_password('Drill2026!')))
        for name, role, pos in [('hs-primary','leader',8100),('hs-east','follower',8100),('hs-west','follower',8098)]:
            if not db.scalar(select(Node).where(Node.name == name)):
                db.add(Node(name=name, role=role, term=6, log_position=pos, available=True, fenced=False, vote_enabled=True))
        for name, fence in [('晚班隔离演练',True),('缺少隔离证明',False)]:
            if not db.scalar(select(Scenario).where(Scenario.name == name)):
                db.add(Scenario(name=name, fail_primary=True, fence_primary=fence, max_data_loss=0, required_term=7))
        if not db.scalar(select(Scenario).where(Scenario.name == '订阅续接演练')):
            db.add(Scenario(name='订阅续接演练', fail_primary=True, fence_primary=True, max_data_loss=5, required_term=7))
        db.flush()
        if not db.scalar(select(Stream).where(Stream.name == '订单流续接演练')):
            from src.schemas.stream import StreamInput
            from src.services import stream_service
            scenario = db.scalar(select(Scenario).where(Scenario.name == '订阅续接演练'))
            stream = stream_service.create_stream(db, StreamInput(
                name='订单流续接演练', scenario_id=scenario.id, replica_name='hs-east',
                subscribers=['对账中心', '通知服务', '风控平台']))
            for order, text in [('ORD-1001', '订单创建'), ('ORD-1001', '支付成功'), ('ORD-1002', '订单创建')]:
                stream_service.emit(db, stream.id, order, text)
            for _ in range(3):
                stream_service.deliver(db, stream.id)
            stream_service.collect(db, stream.id)
        db.commit()
if __name__ == '__main__': seed()
