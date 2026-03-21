class ForumBotError(Exception):
    """Base exception for forum bot errors."""


class AuthError(ForumBotError):
    """Raised when authentication fails."""


class MessageSendError(ForumBotError):
    """Raised when test message sending fails."""


class DatabaseError(ForumBotError):
    """Raised when a database operation fails."""
