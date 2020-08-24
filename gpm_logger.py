from loguru import logger
from datetime import datetime


class GPMLogger:
    def __init__(self, name, rotation="1 day"):
        self.name = name
        self.info_log_file = "{nm}_info_{year}.log".format(
            nm=self.name, year=datetime.now().strftime("%Y%m%d")
        )
        self.debug_log_file = "{nm}_debug_{year}.log".format(
            nm=self.name, year=datetime.now().strftime("%Y%m%d")
        )
        self.format = "{time} {level} {message}"
        logger.add(
            self.info_log_file, format=self.format, rotation=rotation
        )

    def get_logger(self):
        return logger
