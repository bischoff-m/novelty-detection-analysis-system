import numpy as np
import time
import os
import re

def cache_path():
    """
    Returns the path to the directory used for cache data by the datacollector library.
    Creates it, if not present.
    """
    d = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
    if not os.path.exists(d):
        os.mkdir(d)
    return d

def cached_patientids():
    """
    Returns all patient IDs for which cached data of any form could be found.
    :rtype: set
    """
    cache = cache_path()
    ids = set()
    for cfile in os.listdir(cache):
        ptid = re.search(r'\d+', cfile)
        if ptid:
            ids.add(int(ptid.group()))
    return ids

def formattime(tdelta):
    """
    Formats a time delta in format '%Hh %Mm %Ss' (e.g. 2h 51m 8s).
    :param tdelta: The time to format in seconds.
    :type tdelta: float
    :return: Formatted string representation
    :rtype: str
    """
    hours = int(round(tdelta) / 3600)
    mins = int(round(tdelta) / 60) % 60
    secs = tdelta % 60
    if round(secs) == 60:
        secs = 0
    res = ''
    if hours:
        res += f'{hours}h '
    if mins:
        res += f'{mins}m '
    if not hours and not mins:
        res += f'{np.format_float_positional(secs, trim="-", precision=2, fractional=False).rstrip("0").rstrip(".")}s'
    elif round(secs):
        res += f'{round(secs)}s'
    return res.strip()

class Timer:
    """
    This class is used to measure elapsed time.
    The timer is started on initialization. The elapsed time can be accessed by calling Timer.elapsed().
    """
    def __init__(self):
        self.inittime = time.time()
    
    def elapsed(self):
        """
        Returns a string representation of the elapsed time since initilazation.
        """
        return formattime(time.time() - self.inittime)
    
    def reset(self):
        """
        Restarts the timer. Works like initializing a new one.
        """
        self.inittime = time.time()