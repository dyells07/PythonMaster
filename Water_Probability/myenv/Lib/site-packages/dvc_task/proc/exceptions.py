"""Process exceptions."""

from dvc_task.exceptions import DvcTaskError


class ProcessNotTerminatedError(DvcTaskError):
    """Process is still running."""

    def __init__(self, name):
        super().__init__(f"Managed process '{name}' has not been terminated.")


class ProcessNotFoundError(DvcTaskError):
    """Process does not exist."""

    def __init__(self, name):
        super().__init__(f"Managed process '{name}' does not exist.")


class TimeoutExpired(DvcTaskError):  # noqa: N818
    """Process timeout expired."""

    def __init__(self, cmd, timeout):
        super().__init__(f"'{cmd}' did not complete before timeout '{timeout}'")
        self.cmd = cmd
        self.timeout = timeout


class UnsupportedSignalError(DvcTaskError):
    """Unsupported process signal."""

    def __init__(self, sig):
        super().__init__(f"Unsupported signal: {sig}")
