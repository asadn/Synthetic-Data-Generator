from datetime import datetime
from datetime import timedelta

def generate_dates(_start, _end):
    """Generates dates between given start and end """
    new_start = _start.replace(hour=0, minute=0, second=0, microsecond=0)
    new_end = _end.replace(hour=0, minute=0, second=0, microsecond=0)
    delta = new_end - new_start
    dates = []
    for d in range(delta.days + 1):
        dates.append(new_start + timedelta(days=d))
    return dates
