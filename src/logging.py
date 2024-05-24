import sys
import logging
from pathlib import Path
import datetime as dt

from . import config

LOG_FILE_FORMAT = "[%(levelname)s] - %(asctime)s - %(name)s - : %(message)s in %(pathname)s:%(lineno)d"
LOG_STREAM_FORMAT = "%(levelname)s: %(message)s"

class AuxDatawarehouseHandler(logging.Handler):
    # Inspired by MemoryHandler

    def __init__(self, aux_dw: Path, real_dw: Path, name: str, level=logging.NOTSET):
        super().__init__(level=level)
        self._aux_dw = aux_dw
        self._real_dw = real_dw
        self._fname = name
        
        self._auxHandler = logging.FileHandler(self._aux_dw / 'logs' / f'{self._fname}.log')
        self._buffer: list[logging.LogRecord] = []

        self.set_name(f'aux_dw_{name}')

    def setFormatter(self, fmt):
        self._auxHandler.setFormatter(fmt)
        return super().setFormatter(fmt)

    def setLevel(self, level):
        super().setLevel(level)
        self._auxHandler.setLevel(level)

    def emit(self, record):
        self._auxHandler.emit(record)
        self._buffer.append(record)

    def dump(self, errors: bool):
        if errors:
            target: logging.Handler = logging.handlers.RotatingFileHandler(
                self._real_dw / 'logs' / f'{self._fname}_error_{dt.datetime.now().isoformat()}.log',
            )
        else:
            target: logging.Handler = logging.handlers.RotatingFileHandler(
                self._real_dw / 'logs' / f'{self._fname}.log',
                maxBytes=int(config.LOGGING_MAX_SIZE),
                backupCount=int(config.LOGGING_BACKUP_COUNT),
            )

        target.setLevel(self.level)
        target.setFormatter(self.formatter)

        self.acquire()
        try:
            for record in self._buffer:
                target.handle(record)
            self._buffer.clear()
        finally:
            self.release()

_all_dw_handlers: list[AuxDatawarehouseHandler] = []
def _setup_handler_in_logger(logger: str | logging.Logger, aux_dw, real_dw, name):
    _all_dw_handlers.append(h := AuxDatawarehouseHandler(aux_dw, real_dw, name))
    h.setFormatter(logging.Formatter(LOG_FILE_FORMAT))

    if isinstance(logger, logging.Logger):
        logger.addHandler(h)
    else:
        logging.getLogger(logger).addHandler(h)

    return h

def setup_logging(aux_dw: Path, real_dw: Path, debug: bool):
    (aux_dw / 'logs').mkdir(exist_ok=True)
    (real_dw / 'logs').mkdir(exist_ok=True)
    
    logger = logging.getLogger('dao_analyzer')
    logger.propagate = True

    gqlLogger = logging.getLogger('gql.transport.requests')

    _setup_handler_in_logger(logger, aux_dw, real_dw, 'cache_scripts')
    _setup_handler_in_logger(gqlLogger, aux_dw, real_dw, 'gql_requests')

    streamhandler = logging.StreamHandler(sys.stderr)
    streamhandler.setLevel(logging.WARNING if debug else logging.ERROR)
    streamhandler.setFormatter(logging.Formatter(LOG_STREAM_FORMAT))

    logger.addHandler(streamhandler)
    gqlLogger.addHandler(streamhandler)

    if debug:
        logger.setLevel(logging.DEBUG)
        gqlLogger.setLevel(logging.DEBUG)

def finish_logging(errors: bool):
    for h in _all_dw_handlers:
        h.dump(errors)
        h.close()
