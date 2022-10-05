import pytz
import datetime as dt


def timestamp_to_datetime(timestamp: float) -> dt.datetime:
    """
    Utility function that transforms a UNIX timestamp into a UTC-localized datetime
    object.
    """

    return pytz.utc.localize(dt.datetime.utcfromtimestamp(timestamp))
