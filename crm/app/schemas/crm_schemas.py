"""CRM schemas."""
import uuid

from pydantic import BaseModel, ConfigDict, Field


class BitrixAuthRequest(BaseModel):
    email: str
    password: str
    bx_auth: dict = Field(default_factory=dict)


class BitrixConnectRequest(BaseModel):
    line_id: uuid.UUID
    bx_auth: dict = Field(default_factory=dict)


class BitrixSelectTenantRequest(BaseModel):
    jwt_token: str
    tenant_id: str
    bx_auth: dict = Field(default_factory=dict)


class StartOutgoingRequest(BaseModel):
    member_id: str
    contact_id: str
    line_id: uuid.UUID
    channel_account_id: uuid.UUID | None = None
    text: str = ""


class AmoSaveTokensRequest(BaseModel):
    tenant_id: uuid.UUID
    domain: str
    access_token: str
    refresh_token: str = ""


class AmoAccountOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    domain: str
    amojo_id: str | None
    is_active: bool


class AmoLineConnectRequest(BaseModel):
    crm_account_id: uuid.UUID
    line_id: uuid.UUID
    channel_type: str