class ChecksumError(Exception):
    pass


class ConfigError(Exception):
    pass


class CloudStorageError(Exception):
    def __init__(self, message: str, details: str = ""):
        self.message = message
        self.details = details
        super().__init__(f"{message}: {details}" if details else message)
