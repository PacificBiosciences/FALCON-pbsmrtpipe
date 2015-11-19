"""Purely functional, somewhat generic code.
"""

def total_length(pairs):
    return sum(length*count for (length, count) in pairs)

def calc_cutoff(target, pairs):
    """Return read_length such that sum(lens for len >= rl) >= target.
    Raise on empty pairs, which are (length, count) tuples.
    """
    accum = 0
    for length, count in reversed(sorted(pairs)):
        accum += length*count
        if accum >= target:
            break
    else:
        raise Exception('Total=%d < target=%d' %(accum, target))
    return length

