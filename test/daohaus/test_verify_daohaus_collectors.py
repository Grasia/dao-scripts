import pytest

from dao_analyzer.cache_scripts.common.graphql import GraphQLCollector
from dao_analyzer.cache_scripts.daohaus.runner import DaohausRunner

@pytest.mark.parametrize("c", DaohausRunner().collectors)
def test_verify_collectors(c):
    assert c.verify()
    