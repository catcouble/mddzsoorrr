"""Concurrency manager for token-based rate limiting"""
import asyncio
from typing import Dict, Optional
from ..core.logger import debug_logger
from ..core.config import config


class ConcurrencyManager:
    """Manages concurrent request limits for each token"""

    def __init__(self):
        """Initialize concurrency manager"""
        self._image_concurrency: Dict[int, int] = {}  # token_id -> remaining image concurrency (local mode)
        self._video_concurrency: Dict[int, int] = {}  # token_id -> remaining video concurrency (local mode)
        self._image_limits: Dict[int, int] = {}  # token_id -> image concurrency limit (redis mode)
        self._video_limits: Dict[int, int] = {}  # token_id -> video concurrency limit (redis mode)
        self._lock = asyncio.Lock()  # Protect concurrent access
        self._redis_manager = None

    async def _get_redis_manager(self):
        if self._redis_manager is None:
            try:
                from ..core.redis_manager import get_redis_manager
                self._redis_manager = get_redis_manager()
                if not self._redis_manager._initialized:
                    await self._redis_manager.initialize()
            except Exception:
                self._redis_manager = None
        return self._redis_manager

    async def _use_redis(self) -> bool:
        redis_mgr = await self._get_redis_manager()
        return bool(redis_mgr and redis_mgr.is_connected)

    def _get_ttl(self, lock_type: str) -> int:
        if lock_type == "video":
            return config.video_timeout
        return config.image_timeout

    async def initialize(self, tokens: list):
        """
        Initialize concurrency counters from token list
        
        Args:
            tokens: List of Token objects with image_concurrency and video_concurrency fields
        """
        use_redis = await self._use_redis()
        async with self._lock:
            self._image_concurrency.clear()
            self._video_concurrency.clear()
            self._image_limits.clear()
            self._video_limits.clear()

            for token in tokens:
                if token.image_concurrency and token.image_concurrency > 0:
                    if use_redis:
                        self._image_limits[token.id] = token.image_concurrency
                    else:
                        self._image_concurrency[token.id] = token.image_concurrency
                if token.video_concurrency and token.video_concurrency > 0:
                    if use_redis:
                        self._video_limits[token.id] = token.video_concurrency
                    else:
                        self._video_concurrency[token.id] = token.video_concurrency

            if use_redis:
                redis_mgr = await self._get_redis_manager()
                for token_id in set(self._image_limits.keys()):
                    key = f"concurrency:{token_id}:image"
                    await redis_mgr.set(key, "0")
                    await redis_mgr.expire(key, self._get_ttl("image"))
                for token_id in set(self._video_limits.keys()):
                    key = f"concurrency:{token_id}:video"
                    await redis_mgr.set(key, "0")
                    await redis_mgr.expire(key, self._get_ttl("video"))

            mode = "redis" if use_redis else "local"
            debug_logger.log_info(f"Concurrency manager initialized with {len(tokens)} tokens (mode: {mode})")

    async def can_use_image(self, token_id: int) -> bool:
        """
        Check if token can be used for image generation
        
        Args:
            token_id: Token ID
            
        Returns:
            True if token has available image concurrency, False if concurrency is 0
        """
        if await self._use_redis():
            limit = self._image_limits.get(token_id)
            if limit is None:
                return True
            redis_mgr = await self._get_redis_manager()
            current = await redis_mgr.get_concurrency(token_id, "image")
            if current >= limit:
                debug_logger.log_info(f"Token {token_id} image concurrency exhausted (in_use: {current}, limit: {limit})")
                return False
            return True

        async with self._lock:
            if token_id not in self._image_concurrency:
                return True
            remaining = self._image_concurrency[token_id]
            if remaining <= 0:
                debug_logger.log_info(f"Token {token_id} image concurrency exhausted (remaining: {remaining})")
                return False
            return True

    async def can_use_video(self, token_id: int) -> bool:
        """
        Check if token can be used for video generation
        
        Args:
            token_id: Token ID
            
        Returns:
            True if token has available video concurrency, False if concurrency is 0
        """
        if await self._use_redis():
            limit = self._video_limits.get(token_id)
            if limit is None:
                return True
            redis_mgr = await self._get_redis_manager()
            current = await redis_mgr.get_concurrency(token_id, "video")
            if current >= limit:
                debug_logger.log_info(f"Token {token_id} video concurrency exhausted (in_use: {current}, limit: {limit})")
                return False
            return True

        async with self._lock:
            if token_id not in self._video_concurrency:
                return True
            remaining = self._video_concurrency[token_id]
            if remaining <= 0:
                debug_logger.log_info(f"Token {token_id} video concurrency exhausted (remaining: {remaining})")
                return False
            return True

    async def acquire_image(self, token_id: int) -> bool:
        """
        Acquire image concurrency slot
        
        Args:
            token_id: Token ID
            
        Returns:
            True if acquired, False if not available
        """
        if await self._use_redis():
            limit = self._image_limits.get(token_id)
            if limit is None:
                return True
            redis_mgr = await self._get_redis_manager()
            current = await redis_mgr.increment_concurrency(token_id, "image")
            if current > limit:
                await redis_mgr.decrement_concurrency(token_id, "image")
                return False
            await redis_mgr.expire(f"concurrency:{token_id}:image", self._get_ttl("image"))
            debug_logger.log_info(f"Token {token_id} acquired image slot (in_use: {current}, limit: {limit})")
            return True

        async with self._lock:
            if token_id not in self._image_concurrency:
                return True
            if self._image_concurrency[token_id] <= 0:
                return False
            self._image_concurrency[token_id] -= 1
            debug_logger.log_info(f"Token {token_id} acquired image slot (remaining: {self._image_concurrency[token_id]})")
            return True

    async def acquire_video(self, token_id: int) -> bool:
        """
        Acquire video concurrency slot
        
        Args:
            token_id: Token ID
            
        Returns:
            True if acquired, False if not available
        """
        if await self._use_redis():
            limit = self._video_limits.get(token_id)
            if limit is None:
                return True
            redis_mgr = await self._get_redis_manager()
            current = await redis_mgr.increment_concurrency(token_id, "video")
            if current > limit:
                await redis_mgr.decrement_concurrency(token_id, "video")
                return False
            await redis_mgr.expire(f"concurrency:{token_id}:video", self._get_ttl("video"))
            debug_logger.log_info(f"Token {token_id} acquired video slot (in_use: {current}, limit: {limit})")
            return True

        async with self._lock:
            if token_id not in self._video_concurrency:
                return True
            if self._video_concurrency[token_id] <= 0:
                return False
            self._video_concurrency[token_id] -= 1
            debug_logger.log_info(f"Token {token_id} acquired video slot (remaining: {self._video_concurrency[token_id]})")
            return True

    async def release_image(self, token_id: int):
        """
        Release image concurrency slot
        
        Args:
            token_id: Token ID
        """
        if await self._use_redis():
            limit = self._image_limits.get(token_id)
            if limit is None:
                return
            redis_mgr = await self._get_redis_manager()
            current = await redis_mgr.decrement_concurrency(token_id, "image")
            debug_logger.log_info(f"Token {token_id} released image slot (in_use: {current}, limit: {limit})")
            return

        async with self._lock:
            if token_id in self._image_concurrency:
                self._image_concurrency[token_id] += 1
                debug_logger.log_info(f"Token {token_id} released image slot (remaining: {self._image_concurrency[token_id]})")

    async def release_video(self, token_id: int):
        """
        Release video concurrency slot
        
        Args:
            token_id: Token ID
        """
        if await self._use_redis():
            limit = self._video_limits.get(token_id)
            if limit is None:
                return
            redis_mgr = await self._get_redis_manager()
            current = await redis_mgr.decrement_concurrency(token_id, "video")
            debug_logger.log_info(f"Token {token_id} released video slot (in_use: {current}, limit: {limit})")
            return

        async with self._lock:
            if token_id in self._video_concurrency:
                self._video_concurrency[token_id] += 1
                debug_logger.log_info(f"Token {token_id} released video slot (remaining: {self._video_concurrency[token_id]})")

    async def get_image_remaining(self, token_id: int) -> Optional[int]:
        """
        Get remaining image concurrency for token
        
        Args:
            token_id: Token ID
            
        Returns:
            Remaining count or None if no limit
        """
        if await self._use_redis():
            limit = self._image_limits.get(token_id)
            if limit is None:
                return None
            redis_mgr = await self._get_redis_manager()
            current = await redis_mgr.get_concurrency(token_id, "image")
            return max(limit - current, 0)

        async with self._lock:
            return self._image_concurrency.get(token_id)

    async def get_video_remaining(self, token_id: int) -> Optional[int]:
        """
        Get remaining video concurrency for token
        
        Args:
            token_id: Token ID
            
        Returns:
            Remaining count or None if no limit
        """
        if await self._use_redis():
            limit = self._video_limits.get(token_id)
            if limit is None:
                return None
            redis_mgr = await self._get_redis_manager()
            current = await redis_mgr.get_concurrency(token_id, "video")
            return max(limit - current, 0)

        async with self._lock:
            return self._video_concurrency.get(token_id)

    async def reset_token(self, token_id: int, image_concurrency: int = -1, video_concurrency: int = -1):
        """
        Reset concurrency counters for a token
        
        Args:
            token_id: Token ID
            image_concurrency: New image concurrency limit (-1 for no limit)
            video_concurrency: New video concurrency limit (-1 for no limit)
        """
        use_redis = await self._use_redis()
        async with self._lock:
            if use_redis:
                if image_concurrency > 0:
                    self._image_limits[token_id] = image_concurrency
                else:
                    self._image_limits.pop(token_id, None)
                if video_concurrency > 0:
                    self._video_limits[token_id] = video_concurrency
                else:
                    self._video_limits.pop(token_id, None)
            else:
                if image_concurrency > 0:
                    self._image_concurrency[token_id] = image_concurrency
                else:
                    self._image_concurrency.pop(token_id, None)
                if video_concurrency > 0:
                    self._video_concurrency[token_id] = video_concurrency
                else:
                    self._video_concurrency.pop(token_id, None)

            debug_logger.log_info(f"Token {token_id} concurrency reset (image: {image_concurrency}, video: {video_concurrency})")

