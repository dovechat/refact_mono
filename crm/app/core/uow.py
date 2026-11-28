"""Unit of work для сервиса CRM: репозитории на одной сессии."""
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.crm_repo import (
    CrmAccountRepository,
    CrmDialogLinkRepository,
)
from models.lines import ChannelAccount, Line
from models.messages import Conversation
from models.tenants import Membership, User
from repo.base import BaseRepository
from repo.crm_repo import CrmLineConnectionRepository, CrmSettingsRepository
from repo.message_repo import MessageRepository


class CrmUnitOfWork:
    def __init__(self, db: AsyncSession) -> None:
        self.crm_accounts = CrmAccountRepository(db)
        self.crm_dialog_links = CrmDialogLinkRepository(db)
        self.crm_settings = CrmSettingsRepository(db)
        self.crm_line_connections = CrmLineConnectionRepository(db)
        self.channel_accounts = BaseRepository(ChannelAccount, db)
        self.lines = BaseRepository(Line, db)
        self.conversations = BaseRepository(Conversation, db)
        self.msg_repo = MessageRepository(db)
        self.memberships = BaseRepository(Membership, db)
        self.users = BaseRepository(User, db)