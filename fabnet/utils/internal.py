#!/usr/bin/python
"""
Copyright (C) 2011 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet.utils.internal
@author Konstantin Andrusenko
@date November 08, 2012

This module contains internal shared routins
"""

def total_seconds(td):
    """return total seconds
    timedelta.total_seconds() does not supported in <python2.7

    @param td - object of datetime.timedelta
    """
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6.
