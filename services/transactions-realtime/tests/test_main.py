from src import main


def test_main_module_defines_entrypoint():
    assert callable(main.main)
