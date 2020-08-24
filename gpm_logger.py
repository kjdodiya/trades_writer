from loguru import logger
from datetime import datetime


class GPMLogger:
    def __init__(self, name, rotation="1 day"):
        self.name = name
        self.info_log_file = "{nm}_{tmstmp}.log".format(
            nm=self.name, tmstpm=datetime.now().strftime("%Y%m%d")
        )
        self.format = "{time} {level} {message}"
        logger.add(self.info_log_file, format=self.format, rotation=rotation)

    def get_logger(self):
        return logger
