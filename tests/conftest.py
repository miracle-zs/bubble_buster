import importlib.machinery
import importlib.util
import sys
import types


def _requests_available() -> bool:
    try:
        return importlib.util.find_spec("requests") is not None
    except ValueError:
        return False


if not _requests_available():
    requests_stub = types.ModuleType("requests")
    requests_stub.__spec__ = importlib.machinery.ModuleSpec("requests", loader=None)

    class _DummySession:
        def __init__(self):
            self.headers = {}
            self.proxies = {}

        def mount(self, *_args, **_kwargs):
            return None

        def request(self, *_args, **_kwargs):
            raise _DummyRequestException("requests is not installed")

    class _DummyRequestException(Exception):
        pass

    requests_stub.Session = _DummySession
    requests_stub.RequestException = _DummyRequestException

    adapters_stub = types.ModuleType("requests.adapters")
    adapters_stub.__spec__ = importlib.machinery.ModuleSpec("requests.adapters", loader=None)

    class _DummyHTTPAdapter:
        def __init__(self, *args, **kwargs):
            pass

    adapters_stub.HTTPAdapter = _DummyHTTPAdapter
    requests_stub.adapters = adapters_stub
    sys.modules["requests"] = requests_stub
    sys.modules["requests.adapters"] = adapters_stub
