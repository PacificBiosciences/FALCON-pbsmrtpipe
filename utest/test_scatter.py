from nose.tools import assert_equal
from pbfalcon.tasks.scatter_run_scripts_in_json import num_items_in_chunks

def test_num_items_in_chunks():
    expected = [3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2]
    got = num_items_in_chunks(28, 12)
    assert_equal(expected, got)

    expected = [1, 1]
    got = num_items_in_chunks(2, 2)
    assert_equal(expected, got)

    expected = [4, 3]
    got = num_items_in_chunks(7, 2)
    assert_equal(expected, got)
