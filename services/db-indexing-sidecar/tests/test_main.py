from src.main import index_specs


def test_index_specs_cover_expected_fields():
    fields = {field for field, _direction in index_specs()}
    assert fields == {"block_number", "contract_address", "block_timestamp"}
