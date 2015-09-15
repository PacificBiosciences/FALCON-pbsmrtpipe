# https://github.com/PacificBiosciences/pbcommand/blob/master/tests/test_e2e_example_apps.py#L25

class TestQuickDevHelloWorld(pbcommand.testkit.PbTestApp):
    """Runs dev_qhello_world """
    DRIVER_EMIT = "python -m pbcommand.cli.examples.dev_quick_hello_world  emit-tool-contract pbcommand.tasks.dev_qhello_world "
    DRIVER_RESOLVE = "python -m pbcommand.cli.examples.dev_quick_hello_world  run-rtc "

    REQUIRES_PBCORE = False
    INPUT_FILES = [get_data_file("example.txt")]
