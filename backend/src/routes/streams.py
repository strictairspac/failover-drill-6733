from fastapi import APIRouter, Depends
from src.config.database import get_db
from src.middlewares.auth import current_user, writable
from src.schemas.stream import StreamInput, ChangeInput, ReplicationInput, PauseInput
from src.services import stream_service
from src.controllers.actions import audit
from src.utils.serialize import record
router = APIRouter()
@router.get('/streams')
def listing(db=Depends(get_db), user=Depends(current_user)): return stream_service.list_streams(db)
@router.post('/streams', status_code=201)
def create(data: StreamInput, db=Depends(get_db), user=Depends(writable)):
    obj = stream_service.create_stream(db, data)
    audit(db, user, f'stream:create:{obj.id}'); db.commit(); return record(obj)
@router.get('/streams/{stream_id}')
def detail(stream_id: int, db=Depends(get_db), user=Depends(current_user)):
    return record(stream_service.get_stream(db, stream_id))
@router.post('/streams/{stream_id}/changes', status_code=201)
def emit(stream_id: int, data: ChangeInput, db=Depends(get_db), user=Depends(writable)):
    obj = stream_service.emit(db, stream_id, data.order, data.text)
    audit(db, user, f'stream:emit:{stream_id}'); db.commit(); return record(obj)
@router.post('/streams/{stream_id}/replication')
def replication(stream_id: int, data: ReplicationInput, db=Depends(get_db), user=Depends(writable)):
    obj = stream_service.set_replication(db, stream_id, data.on)
    audit(db, user, f'stream:replication:{stream_id}'); db.commit(); return record(obj)
@router.post('/streams/{stream_id}/subscribers/{name}/pause')
def pause(stream_id: int, name: str, data: PauseInput, db=Depends(get_db), user=Depends(writable)):
    obj = stream_service.set_paused(db, stream_id, name, data.paused)
    audit(db, user, f'stream:pause:{stream_id}:{name}'); db.commit(); return record(obj)
@router.post('/streams/{stream_id}/deliver')
def deliver(stream_id: int, db=Depends(get_db), user=Depends(writable)):
    obj = stream_service.deliver(db, stream_id)
    audit(db, user, f'stream:deliver:{stream_id}'); db.commit(); return record(obj)
@router.post('/streams/{stream_id}/receipts/collect')
def collect(stream_id: int, db=Depends(get_db), user=Depends(writable)):
    obj = stream_service.collect(db, stream_id)
    audit(db, user, f'stream:collect:{stream_id}'); db.commit(); return record(obj)
@router.post('/streams/{stream_id}/receipts/{receipt_id}/drop')
def drop(stream_id: int, receipt_id: str, db=Depends(get_db), user=Depends(writable)):
    obj = stream_service.drop_receipt(db, stream_id, receipt_id)
    audit(db, user, f'stream:drop-receipt:{stream_id}:{receipt_id}'); db.commit(); return record(obj)
@router.post('/streams/{stream_id}/failover')
def failover(stream_id: int, db=Depends(get_db), user=Depends(writable)):
    obj = stream_service.failover(db, stream_id)
    audit(db, user, f'stream:failover:{stream_id}'); db.commit(); return record(obj)
