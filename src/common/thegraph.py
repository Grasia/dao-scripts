from typing import Optional, Callable, Any
from abc import ABC, abstractmethod

from gql.dsl import DSLField
from graphql.language import visit, Visitor

import pandas as pd

from .common import ENDPOINTS, Runner, NetworkCollector, UpdatableCollector, GQLRequester, get_graph_url
from ..metadata import Block
from .. import config

Postprocessor = Callable[[pd.DataFrame], pd.DataFrame]

EMPTY_KEY_MSG = """
Empty The Graph API key. You can obtain one from https://thegraph.com/docs/en/querying/managing-api-keys/
"""

def add_where(d: dict[str, Any], **kwargs):
    """
    Adds the values specified in kwargs to the where inside d
        Example: `**add_where(kwargs, deleted=False)`
    """
    if "where" in d:
        d["where"] |= kwargs
    else:
        d["where"] = kwargs
    
    return d

def partial_query(q: Callable[..., DSLField], w: dict[str, Any]) -> Callable[..., DSLField]:
    def wrapper(**kwargs):
        return q(**add_where(kwargs, **w))
    return wrapper

class ColumnsVisitor(Visitor):
    def __init__(self):
        super().__init__()
        self.columns = []
        self._bases = []

    def enter_field(self, node, *args):
        self._bases.append(node)

    def leave_field(self, node, *args):
        self._bases.pop()

    def leave_selection_set(self, node, *_args):
        base = ".".join([x.name.value for x in self._bases])
        for s in node.selections:
            # Skip non-leaf nodes
            if s.selection_set:
                continue
            name = s.name.value
            fullname = ".".join([base, name]) if base else name
            self.columns.append(fullname)
    
def get_columns_from_query(q: DSLField) -> list[str]:
    c = ColumnsVisitor()
    # We use selection_set directly to avoid putting the name of the query
    # i.e: returning id instead of moloches.id
    visit(q.ast_field.selection_set, c)
    return c.columns

class TheGraphCollector(NetworkCollector, UpdatableCollector, ABC):
    def __init__(
        self, 
        name: str,
        network: str,
        subgraph_id: str,
        runner: Runner,
        index: Optional[str]=None,
        result_key: Optional[str]=None,
        pbar_enabled: bool=True
    ):
        super().__init__(name, runner, network)

        self._index_col: str = index or  'id'
        self._result_key: str = result_key or name
        self._postprocessors: list[Postprocessor] = []
        self._indexer_block: Optional[Block] = None
        self._requester = GQLRequester(
            endpoint=get_graph_url(subgraph_id),
            pbar_enabled=pbar_enabled,
        )

    def postprocessor(self, f: Postprocessor):
        self._postprocessors.append(f)
        return f

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def schema(self):
        return self._requester.get_schema()

    @abstractmethod
    def query(self, **kwargs) -> DSLField:
        raise NotImplementedError

    @property
    def df(self) -> pd.DataFrame:
        if not self.data_path.is_file():
            return pd.DataFrame()

        df = pd.read_feather(self.data_path)
        if self.network:
            df = df[df['network'] == self.network]
        
        return df

    def transform_to_df(self, data: list[dict[str, Any]], skip_post: bool=False) -> pd.DataFrame:
        if data:
            df = pd.DataFrame.from_dict(pd.json_normalize(data))
        else:
            df = pd.DataFrame(columns=get_columns_from_query(self.query()))

        if (s1 := set(df.columns)) != (s2 := set(get_columns_from_query(self.query()))):
            raise ValueError(f"Received columns are not the expected columns: {s1} != {s2}")

        # For compatibility reasons we change from . to snake case
        def dotsToSnakeCase(str: str) -> str:
            splitted = str.split('.')
            return splitted[0] + ''.join(x[0].upper()+x[1:] for x in splitted[1:])
                        
        df = df.rename(columns=dotsToSnakeCase)
        df['network'] = self.network

        if not skip_post:
            for post in self._postprocessors:
                self.logger.debug(f"Running postprocessor {post.__name__}")
                df = post(df)
                if df is None:
                    raise ValueError(f"The postprocessor {post.__name__} returned None")

        return df

    def check_deployment_health(self, deployment_id: str) -> bool:
        # TODO(2025-05-04): Check again subgraph status
        # Waiting for https://github.com/graphprotocol/graph-node/issues/5983
        return True
        _requester = GQLRequester(ENDPOINTS['_theGraph']['index-node'])
        ds = _requester.get_schema()
        q = ds.Query.indexingStatuses(subgraphs=[deployment_id]).select(
            ds.SubgraphIndexingStatus.node,    
            ds.SubgraphIndexingStatus.entityCount,
            ds.SubgraphIndexingStatus.health,
            ds.SubgraphIndexingStatus.subgraph,
            ds.SubgraphIndexingStatus.synced,
            ds.SubgraphIndexingStatus.fatalError.select(
                ds.SubgraphError.message,
            ),
            ds.SubgraphIndexingStatus.nonFatalErrors.select(
                ds.SubgraphError.message,
            ),
            ds.SubgraphIndexingStatus.chains.select(
                ds.ChainIndexingStatus.network,
            ),
        )

        r: dict[str, Any] = _requester.request_single(q)[0]
        
        no_errors = True
        assert r['subgraph'] == deployment_id, "Got response for other subgraph"
        if r['fatalError']:
            self.logger.error(f'Subgraph {deployment_id} has fatal error: {r["fatalError"]}')
            no_errors = False

        if r['health'] != 'healthy':
            self.logger.error(f'Subgraph {deployment_id} is not healthy.')
            no_errors = False

        _network = r['chains'][0]['network']
        if _network != self.network:
            self.logger.error(f'Subgraph {deployment_id} is deployed on incorrect network. Expected {self.network} but got {_network}')
            no_errors = False

        if r['nonFatalErrors']:
            self.logger.warning(f'Subgraph {deployment_id} has non fatal errors, check subgraph studio')

        if not r['synced']:
            self.logger.warning(f'Subgraph {deployment_id} is not synced. Check subgraph studio.')

        return no_errors

    def check_subgraph_health(self, check_deployment: bool = True) -> bool:
        ds = self.schema
        q = ds.Query._meta().select(
            ds._Meta_.deployment,
            ds._Meta_.hasIndexingErrors,
            ds._Meta_.block.select(
                ds._Block_.hash,
                ds._Block_.number,
                ds._Block_.timestamp,
            ),
        )

        r = self._requester.request_single(q)

        if r['hasIndexingErrors']:
            self.logger.error('Subgraph has indexing errors')
            return False
        
        self._indexer_block = Block(r['block'])

        if not check_deployment:
            return True
        
        return self.check_deployment_health(r['deployment'])

    def verify(self) -> bool:
        if not config.THE_GRAPH_API_KEY:
            self.logger.error('Empty The Graph api key')
            return False
        
        # Checking if the queryBuilder doesnt raise any errors
        self.query()

        return self.check_subgraph_health()

    def query_cb(self, prev_block: Optional[Block] = None):
        if prev_block:
            return partial_query(self.query, {'_change_block': {'number_gte': prev_block.number}})
        else:
            return self.query

    def run(self, force=False, block: Optional[Block] = None, prev_block: Optional[Block] = None):
        self.logger.info(f"Running The Graph collector with block: {block}, prev_block: {prev_block}")
        assert self.check_subgraph_health(check_deployment=False) # Just update the _indexer_block
        if block and self._indexer_block:
            assert self._indexer_block >= block, f"Block number {block} is not indexed yet ({self._indexer_block})"
        
        if block is None:
            block = Block()
        if prev_block is None or force:
            prev_block = Block()

        data = self._requester.n_requests(query=self.query_cb(prev_block), block_hash=block.id)

        # transform to df
        df: pd.DataFrame = self.transform_to_df(data)
        self._update_data(df, force)
