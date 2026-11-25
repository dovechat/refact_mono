import redis.asyncio as redis

from core.config import settings

# Only redis initialization here to avoid circular imports
rdb = redis.from_url(settings.redis_url, decode_responses=True)