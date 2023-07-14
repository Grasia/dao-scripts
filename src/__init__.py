try:
    from . import _version

    __version__ = _version.version
except ImportError:
    __version__ = 'Unknown'
