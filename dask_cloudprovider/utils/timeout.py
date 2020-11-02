from datetime import datetime, timedelta
import warnings


class TimeoutException(RuntimeError):
    """Raised when a loop times out."""


class Timeout:
    """A timeout object for use in ``while True`` loops instead of ``True``.

    Create an instance of this class before beginning an infinite loop and
    call ``run()`` instead of ``True``.


    Parameters
    ----------
    timeout: int
        Seconds before loop should timeout.

    error_message: str
        Error message to raise in an exception if timeout occurs.

    warn: bool
        Only raise a warning instead of a TimeoutException.

        Default ``False``.
    Examples
    --------
    >>> timeout = Timeout(10, "Oh no! We timed out.")
    >>> while timeout.run():
    ...     time.sleep(1)  # Will timeout after 10 iterations
    TimeoutException: Oh no! We timed out.

    You can also pass an exception to raise if you are suppressing for a set
    amount of time.

    >>> timeout = Timeout(10, "Oh no! We timed out.")
    >>> while timeout.run():
    ...     try:
    ...         some_function_that_raises()
    ...         break
    ...     except Exception as e:
    ...         timeout.set_exception(e)
    ...         time.sleep(1)  # Will timeout after 10 iterations
    Exception: The exception from ``some_function_that_raises``


    """

    def __init__(self, timeout, error_message, warn=False):
        self.start = None
        self.running = False
        self.timeout = timeout
        self.error_message = error_message
        self.warn = warn
        self.exception = TimeoutException(self.error_message)

    def run(self):
        """Run the timeout.

        This method when called repeatedly will return ``True`` until the
        timeout has elapsed. It will then raise or return ``False``.
        """
        if not self.running:
            self.start = datetime.now()
            self.running = True

        if self.start + timedelta(seconds=self.timeout) < datetime.now():
            if self.warn:
                warnings.warn(self.error_message)
                return False
            else:
                raise self.exception
        return True

    def set_exception(self, e):
        """Modify the default timeout exception.

        This would be useful if you are trying something repeatedly but if it
        never succeeds before the timeout you want to raise the exception from
        the thing you are trying rather than a TimeoutException.
        """
        self.exception = e
