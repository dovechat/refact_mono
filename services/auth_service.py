import hashlib
import hmac
import secrets
import uuid

import bcrypt
from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.redis import rdb
from infra.email.sender import OTPSender
from infra.sms.sender import SMSSender
from models.tenants import UserStatus
from repo.user import UserRepository
from schemas.auth_schemas import LoginRequest, RegisterRequest, TokenPair, TokenType, UserContext as User
# from schemas.auth_schemas import TokenPayload, TokenType
from services.jwt_services import JWTService

import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

LOGIN_MAX_FAILS = 5
LOGIN_FAIL_TTL = 900
OTP_TTL = 600
OTP_MAX_ATTEMPTS = 10
_DUMMY_HASH = bcrypt.hashpw(b"dummy", bcrypt.gensalt()).decode("utf-8")



def _invite_key(token: str) -> str:
    return f"invite:{hashlib.sha256(token.encode()).hexdigest()}"


async def create_invite_token(user_id: uuid.UUID | str) -> str:
    """Одноразовый токен приглашения. В Redis хранится только его хеш."""
    token = secrets.token_urlsafe(32)
    await rdb.setex(
        _invite_key(token),
        settings.invite_token_expire_hours * 3600,
        str(user_id),
    )
    return token


async def is_throttled(key: str, seconds: int) -> bool:
    """
    Check if the action is throttled using atomic SET NX.
    Returns True if throttled, False if allowed.
    """
    res = await rdb.set(f"throttle:{key}", "1", ex=seconds, nx=True)

    # Handle various return types from different redis-py versions
    if res in [True, "OK", 1]:
        return False

    return True


def send_otp(data, code) -> bool:
    if data.ifsms:
        return SMSSender().send(data.phone, code)
    else:
        return OTPSender().send(data.email, code)


class AuthService:
    """Service for authentication operations"""
    def __init__(self, jwt_service: JWTService) -> None:
        self.jwt_service = jwt_service

    async def set_password(
        self,
        user_id: uuid.UUID,
        password: str,
        db: AsyncSession,
    ) -> TokenPair:
        user_repo = UserRepository(db)
        user = await user_repo.get_by_id(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        await user_repo.update(user, password_hash=self.jwt_service.password_hash(password))
        return self.jwt_service.create_token_pair(
            user_id=user.id, role=user.role.value if user.role else None
        )

    async def login(
        self,
        request: LoginRequest,
        db: AsyncSession,
        tenant_id: int | str | uuid.UUID | None = None,
    ) -> TokenPair:
        """User login"""
        fail_key = f"login_fail:{request.email.lower()}"
        fails = await rdb.get(fail_key)
        if fails and int(fails) >= LOGIN_MAX_FAILS:
            raise HTTPException(status_code=429, detail="Too many requests")

        user_repo = UserRepository(db)
        user = await user_repo.get_by_email(request.email)
        hashed = user.password_hash if user and user.password_hash else _DUMMY_HASH
        valid = self.jwt_service.verify_password(request.password, hashed)
        if not user or not user.password_hash or not valid:
            fails = await rdb.incr(fail_key)
            if fails == 1:
                await rdb.expire(fail_key, LOGIN_FAIL_TTL)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
            )
        await rdb.delete(fail_key)

        role = None
        if user.role:
            role = user.role.value
        return self.jwt_service.create_token_pair(
            user_id=user.id, role=role
        )

    async def register(
        self,
        data: RegisterRequest,
        db: AsyncSession,
    ) -> TokenPair:
        """Full registration: user + organization + owner"""
        user_repo = UserRepository(db)

        existing = await user_repo.get_by_email(data.email)
        if existing:
            log.error(f"\n\n ERROR: User {data.email} already exist.  AuthService.register_user data {data} \n\n")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User with this email already exists",
            )
        log.info(f"\n\n ************* AuthService.register_user data {data} \n\n")
        # data.password = self.jwt_service.password_hash(data.password)

        user = await user_repo.create_user(data)

        # Generate 4-digit code
        code = f"{secrets.randbelow(10000):04d}"

        await rdb.setex(f"auth:{str(user.id)}", OTP_TTL, code)
        await rdb.delete(f"auth_attempts:{user.id}")

        if await is_throttled(f"otp:{data.email}", 60):
            raise HTTPException(status_code=429, detail="Too many requests")

        send_otp(data, code)

        return self.jwt_service.create_token_pair(
            user_id=user.id,
            role="owner",
        )

    async def refresh(self, refresh_token: str, db: AsyncSession) -> TokenPair:
        payload = self.jwt_service.verify_token(refresh_token, token_type=TokenType.REFRESH)

        if await is_throttled(f"refresh:{payload.sub}", 5):
            raise HTTPException(status_code=429, detail="Too many requests")

        # Права перечитываются из БД: удалённый сотрудник или заблокированный пользователь не продлит доступ
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
        )
        user_repo = UserRepository(db)
        user = await user_repo.get_by_id(payload.sub)
        if not user or user.status == UserStatus.suspended:
            raise credentials_exception

        member = None
        if payload.tenant_id:
            membership = await user_repo.get_membership(
                user.id, uuid.UUID(str(payload.tenant_id))
            )
            if not membership:
                raise credentials_exception
            member = membership.role

        return self.jwt_service.create_token_pair(
            user_id=user.id,
            role=user.role,
            tenant_id=payload.tenant_id,
            member=member,
        )

    async def accept_invite(self, token: str, password: str, db: AsyncSession) -> TokenPair:
        """Принять приглашение: токен одноразовый, пароль задаёт сам сотрудник"""
        invalid = HTTPException(status_code=400, detail="Invalid or expired invite")

        user_id = await rdb.getdel(_invite_key(token))
        if not user_id:
            raise invalid

        user_repo = UserRepository(db)
        user = await user_repo.get_by_id(user_id)
        if not user:
            raise invalid

        await user_repo.update(
            user,
            password_hash=self.jwt_service.password_hash(password),
            status=UserStatus.active,
        )
        await rdb.delete(f"user:{user_id}")

        return self.jwt_service.create_token_pair(user_id=user.id, role=user.role)

    async def verify_code(self, user_id: str, code: str, db: AsyncSession) -> str:
        lock_key = f"activated_lock:{user_id}"

        # 1. Throttle check
        is_locked = await rdb.get(lock_key)
        log.info(f"AUTH_VERIFY: user={user_id} is_locked={is_locked is not None}")

        if is_locked:
            log.warning(f"AUTH_VERIFY: too_fast for user={user_id}")
            return "too_fast"

        # Set 3s lock immediately
        await rdb.set(lock_key, "1", ex=3)
        log.info(f"AUTH_VERIFY: lock set for user={user_id}")

        # 2. Status check
        user_repo = UserRepository(db)
        user = await user_repo.get_by_id(user_id)
        if not user:
            log.error(f"AUTH_VERIFY: user_not_found {user_id}")
            return "user_not_found"

        if user.status in [UserStatus.trial, UserStatus.active]:
            log.info(f"AUTH_VERIFY: already_activated user={user_id}")
            return "already_activated"

        # 3. Code validation
        stored_code = await rdb.get(f"auth:{user_id}")
        if not stored_code:
            log.info(f"AUTH_VERIFY: invalid_code for user={user_id}")
            return "invalid_code"

        attempts_key = f"auth_attempts:{user_id}"
        attempts = await rdb.incr(attempts_key)
        if attempts == 1:
            await rdb.expire(attempts_key, OTP_TTL)
        if attempts > OTP_MAX_ATTEMPTS:
            log.warning(f"AUTH_VERIFY: too many attempts user={user_id}")
            return "too_fast"

        if not hmac.compare_digest(
            str(stored_code).encode(), str(code).encode()
        ):
            log.info(f"AUTH_VERIFY: invalid_code for user={user_id}")
            return "invalid_code"

        # 4. Success flow
        user.status = UserStatus.trial
        await user_repo.update(user)
        await rdb.delete(f"auth:{user_id}")
        await rdb.delete(attempts_key)
        await rdb.delete(f"user:{user_id}")
        log.info(f"AUTH_VERIFY: ok for user={user_id}")

        return "ok"

    async def switch_context(
            self,
            db: AsyncSession,
            user_id: uuid.UUID,
            tenant_id: uuid.UUID | None = None
    ):
        """
        Switch user context and issue a new token pair.
        """
        user_repo = UserRepository(db)
        result = await user_repo.get_membership(user_id, tenant_id)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No access to tenant"
            )

        if isinstance(result, list):
            membership = min(result, key=lambda m: 0 if m.role == "owner" else 1)
        else:
            membership = result

        user = await user_repo.get_by_id(user_id)

        res = self.jwt_service.create_token_pair(
            user_id=user_id,
            role=user.role,
            tenant_id=membership.tenant_id,
            member=membership.role,
        )

        return res

