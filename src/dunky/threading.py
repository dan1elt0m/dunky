import threading


def run_with_timeout(func, args=(), kwargs={}, timeout=5):
    result = [None]
    exception = [None]

    def wrapper():
        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            exception[0] = e

    thread = threading.Thread(target=wrapper)
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        print(f"Function did not return within {timeout} seconds.")
        return None
    if exception[0]:
        raise exception[0]
    return result[0]
