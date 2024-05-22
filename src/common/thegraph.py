from typing import Optional, Callable, Any, TypeAlias
from abc import ABC, abstractmethod

from gql.dsl import DSLField

import pandas as pd

from .common import Runner, NetworkCollector, UpdatableCollector, GQLRequester, get_graph_url
from ..metadata import Block
from .. import config

Postprocessor: TypeAlias = Callable[[pd.DataFrame], pd.DataFrame]

EMPTY_KEY_MSG = """
Empty The Graph API key. You can obtain one from https://thegraph.com/docs/en/querying/managing-api-keys/
"""

def add_where(d, **kwargs):
    """
    Adds the values specified in kwargs to the where inside d
        Example: `**add_where(kwargs, deleted=False)`
    """
    if "where" in d:
        d["where"] |= kwargs
    else:
        d["where"] = kwargs
    
    return d

def partial_query(q, w) -> DSLField:
    def wrapper(**kwargs):
        return q(**add_where(kwargs, **w))
    return wrapper


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
        df = pd.DataFrame.from_dict(pd.json_normalize(data))

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

    def verify(self) -> bool:
        if not config.THE_GRAPH_API_KEY:
            self.logger.error('Empty The Graph api key')
            return False
        
        # Checking if the queryBuilder doesnt raise any errors
        self.query()

        return True

    def query_cb(self, prev_block: Block = None):
        if prev_block:
            return partial_query(self.query, {'_change_block': {'number_gte': prev_block.number}})
        else:
            return self.query

    def run(self, force=False, block: Block = None, prev_block: Block = None):
        self.logger.info(f"Running The Graph collector with block: {block}, prev_block: {prev_block}")
        if block is None:
            block = Block()
        if prev_block is None or force:
            prev_block = Block()

        data = self._requester.n_requests(query=self.query_cb(prev_block), block_hash=block.id)

        # transform to df
        df: pd.DataFrame = self.transform_to_df(data)
        self._update_data(df, force)
