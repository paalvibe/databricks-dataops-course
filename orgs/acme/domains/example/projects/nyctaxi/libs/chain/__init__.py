__all__ = 'chain'


def chain(data, fns):
    """Apply list of functions to input data in sequence"""
    ret = data
    for fn in fns:
        ret = fn(ret)
    return ret
