import pytest

from dao_analyzer.cache_scripts.daohaus.runner import DaohausRunner

@pytest.mark.parametrize("c", DaohausRunner(None).collectors)
def test_verify_collectors(c):
    assert c.verify()
