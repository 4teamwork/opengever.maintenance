

def join_lines(fn):
    """Decorator that joins a sequence of lines with newlines.
    """
    def wrapped(self, *args, **kwargs):
        return '\n'.join(fn(self, *args, **kwargs))
    return wrapped