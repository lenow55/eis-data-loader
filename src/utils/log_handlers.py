from pathlib import Path
from logging.handlers import RotatingFileHandler as _RotatingFileHandler


class RotatingFileHandler(_RotatingFileHandler):
    def __init__(self, filename: str, *args, **kwargs):
        Path(filename).absolute().parent.mkdir(exist_ok=True, parents=True)
        super().__init__(filename, *args, **kwargs)
