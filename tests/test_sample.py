from utils import dwh_utils
import pytest


def test_text_is_my_name_is_sss():
    text = dwh_utils.print_name()
    assert text == "Sample Project Function"