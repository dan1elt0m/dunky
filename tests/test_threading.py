import pytest
import threading
from dunky.threading import run_with_timeout


def test_function_completes_within_timeout():
    def sample_function():
        return "Completed"

    result = run_with_timeout(sample_function, timeout=2)
    assert result == "Completed"


def test_function_exceeds_timeout():
    def long_running_function():
        threading.Event().wait(10)

    result = run_with_timeout(long_running_function, timeout=2)
    assert result is None


def test_function_raises_exception():
    def error_function():
        raise ValueError("An error occurred")

    with pytest.raises(ValueError, match="An error occurred"):
        run_with_timeout(error_function, timeout=2)


def test_function_with_arguments():
    def add(a, b):
        return a + b

    result = run_with_timeout(add, args=(2, 3), timeout=2)
    assert result == 5


def test_function_with_kwargs():
    def greet(name="World"):
        return f"Hello, {name}!"

    result = run_with_timeout(greet, kwargs={"name": "Alice"}, timeout=2)
    assert result == "Hello, Alice!"
