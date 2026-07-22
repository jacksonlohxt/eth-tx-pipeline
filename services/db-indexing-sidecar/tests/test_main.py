import src.main as main_module
from pymongo import MongoClient
from src.main import index_specs


def test_index_specs_cover_expected_fields():
    fields = {field for field, _direction in index_specs()}
    assert fields == {
        "block_number",
        "contract_address",
        "block_timestamp",
        "from_address",
        "to_address",
    }


class _FakeCollection:
    def create_index(self, spec):
        pass


class _FakeDatabase:
    def __getitem__(self, name):
        return _FakeCollection()


class _RecordingClient:
    """Wraps a real (lazy, no-network) MongoClient so get_default_database()
    exercises pymongo's actual URI parsing/ConfigurationError behavior, while
    faking out create_index so the test doesn't need a live Mongo server.
    """

    def __init__(self, url, **kwargs):
        self._real = MongoClient(url, connect=False)

    def get_default_database(self):
        self._real.get_default_database()
        return _FakeDatabase()

    def close(self):
        self._real.close()


def test_main_uses_default_database_when_mongodb_url_unset(monkeypatch):
    monkeypatch.delenv("MONGODB_URL", raising=False)
    monkeypatch.setattr(main_module, "MongoClient", _RecordingClient)

    main_module.main()
