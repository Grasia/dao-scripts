import pytest

from src.common.graphql import GraphQLCollector
from src.daohaus.runner import DaohausRunner

@pytest.mark.parametrize("c", DaohausRunner().collectors)
def test_verify_collectors(c):
    assert c.verify()
    