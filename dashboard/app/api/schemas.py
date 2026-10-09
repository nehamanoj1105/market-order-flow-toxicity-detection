from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List

class AlertOut(BaseModel):
	id: int
	event_id: str
	symbol: str
	exchange: str
	z_score: float
	ewma: float
	ewma_var: float
	signed_ofi: float
	side: str
	quantity: float
	price: float
	trade_time_ms: int
	alerted_at: datetime
	
class LoginRequest(BaseModel):
	username: str
	password: str


class LoginResponse(BaseModel):
	access_token: str
	token_type: str
	expires_in: int
	
class SymbolConfigSchema(BaseModel):
	symbol: str = Field(...,example="BTCUSDT")
	z_threshold: float = Field(...,gt=0, example=3.0)
	ewma_alpha: float = Field(...,gt=0, le=1.0, example=0.1)
	min_trades: int = Field(..., ge=1, example=10)
	
class ConfigUpdateResponse(BaseModel):
	status: str
	symbol: str
	updated_config: SymbolConfigSchema
