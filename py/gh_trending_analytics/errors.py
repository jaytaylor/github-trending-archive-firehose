from __future__ import annotations


class AnalyticsError(Exception):
    pass


class InvalidRequestError(AnalyticsError):
    pass


class NotFoundError(AnalyticsError):
    pass
