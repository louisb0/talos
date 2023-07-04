from talos.exceptions.base import FatalException


class EnvironmentVariableNotSetException(FatalException):
    def __init__(self, variable):
        super().__init__(
            f"Missing required environment variable {variable}."
        )
