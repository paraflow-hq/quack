class ChecksumError(Exception):
    pass


class ConfigError(Exception):
    pass


class CloudStorageError(Exception):
    def __init__(self, message: str, details: str = "", code: str | None = None):
        self.message = message
        self.details = details
        self.code = code
        super().__init__(f"{message}: {details}" if details else message)


class CloudStorageTransientError(CloudStorageError):
    pass
