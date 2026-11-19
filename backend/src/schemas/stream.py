from pydantic import BaseModel, Field
class StreamInput(BaseModel):
    name: str = Field(min_length=2, max_length=100)
    scenario_id: int = Field(gt=0)
    replica_name: str = Field(min_length=2, max_length=80)
    subscribers: list[str] = Field(min_length=1, max_length=6)
class ChangeInput(BaseModel):
    order: str = Field(min_length=2, max_length=60)
    text: str = Field(min_length=1, max_length=120)
class ReplicationInput(BaseModel):
    on: bool
class PauseInput(BaseModel):
    paused: bool
