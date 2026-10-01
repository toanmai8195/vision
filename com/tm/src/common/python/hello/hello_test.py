import sys

import pytest

from com.tm.src.common.python.hello.hello import greet


def test_greet() -> None:
    assert greet("vision") == "hello, vision"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, *sys.argv[1:]]))
