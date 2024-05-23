"""
    Descp: Aragon Runner and Collectors

    Created on: 02-nov-2021

    Copyright 2021 David Davó
        <david@ddavo.me>
"""
from typing import List

import pkgutil
from gql.dsl import DSLField
import pandas as pd
import numpy as np
import json

from ..aragon import __name__ as aragon_module_name
from ..common.cryptocompare import CCPricesCollector
from ..common import ENDPOINTS, Collector, NetworkRunner
from ..common.thegraph import TheGraphCollector
from ..common.blockscout import BlockscoutBallancesCollector

class AppsCollector(TheGraphCollector):
    def __init__(self, runner, network: str):
        super().__init__('apps', network, ENDPOINTS[network]['aragon'], runner)

    def query(self, **kwargs) -> DSLField:
        ds = self.schema
        return ds.Query.apps(**kwargs).select(
            ds.App.id,
            ds.App.isForwarder,
            ds.App.isUpgradeable,
            ds.App.repoName,
            ds.App.repoAddress,
            ds.App.organization.select(ds.Organization.id)
        )

class BalancesCollector(BlockscoutBallancesCollector):
    def __init__(self, runner, base, network: str):
        super().__init__(runner, addr_key='recoveryVault', base=base, network=network)

class CastsCollector(TheGraphCollector):
    def __init__(self, runner, network: str):
        super().__init__('casts', network, ENDPOINTS[network]['aragon_voting'], runner, pbar_enabled=False)

        @self.postprocessor
        def changeColumnNames(df: pd.DataFrame) -> pd.DataFrame:
            df = df.rename(columns={
                'voterId':'voter', 
                'voteAppAddress':'appAddress',
                'voteOrgAddress':'orgAddress'})
            return df

    def query(self, **kwargs) -> DSLField:
        ds = self.schema
        return ds.Query.casts(**kwargs).select(
            ds.Cast.id,
            ds.Cast.voter.select(ds.Voter.id),
            ds.Cast.supports,
            ds.Cast.stake,
            ds.Cast.createdAt,
            ds.Cast.vote.select(
                ds.Vote.id,
                ds.Vote.orgAddress,
                ds.Vote.appAddress
            )
        )

class OrganizationsCollector(TheGraphCollector):
    DAO_NAMES=pkgutil.get_data(aragon_module_name, 'dao_names.json')

    def __init__(self, runner, network: str):
        super().__init__('organizations', network, ENDPOINTS[network]['aragon'], runner)

        @self.postprocessor
        def set_dead_recoveryVault(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty: return df

            df['recoveryVault'] = df['recoveryVault'].replace(r'^0x0+$', np.NaN, regex=True)
            return df

        @self.postprocessor
        def apply_names(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty: return df

            names_dict = json.loads(self.DAO_NAMES)

            if self.network not in names_dict.keys() or \
            not names_dict[self.network] or \
            df.empty:
                return df

            names_df = pd.json_normalize(names_dict[self.network])
            names_df['id'] = names_df['address'].str.lower()
            names_df['name'] = names_df['name'].fillna(names_df['domain'])
            names_df = names_df[['id', 'name']]
            df = df.merge(names_df, on='id', how='left')

            return df

        @self.postprocessor
        def copy_id(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty: return df

            df['orgAddress'] = df['id']
            return df

    def query(self, **kwargs) -> DSLField:
        ds = self.schema
        return ds.Query.organizations(**kwargs).select(
            ds.Organization.id,
            ds.Organization.createdAt,
            ds.Organization.recoveryVault
        )

class MiniMeTokensCollector(TheGraphCollector):
    def __init__(self, runner, network: str):
        super().__init__('miniMeTokens', network, ENDPOINTS[network]['aragon_tokens'], runner, pbar_enabled=False)

    def query(self, **kwargs) -> DSLField:
        ds = self.schema
        return ds.Query.miniMeTokens(**kwargs).select(
            ds.MiniMeToken.id,
            ds.MiniMeToken.address,
            ds.MiniMeToken.totalSupply,
            ds.MiniMeToken.transferable,
            ds.MiniMeToken.name,
            ds.MiniMeToken.symbol,
            ds.MiniMeToken.orgAddress,
            ds.MiniMeToken.appAddress,
            ds.MiniMeToken.lastUpdateAt
        )

class TokenHoldersCollector(TheGraphCollector):
    def __init__(self, runner: NetworkRunner, network: str):
        super().__init__('tokenHolders', network, ENDPOINTS[network]['aragon_tokens'], runner)

        @self.postprocessor
        def add_minitokens(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty: return df

            tokens = runner.filterCollector(name='miniMeTokens', network=network).df
            tokens = tokens.rename(columns={'address':'tokenAddress', 'orgAddress':'organizationAddress'})
            return df.merge(tokens[['tokenAddress', 'organizationAddress']], on='tokenAddress', how='left')
            
    def query(self, **kwargs) -> DSLField:
        ds = self.schema
        return ds.Query.tokenHolders(**kwargs).select(
            ds.TokenHolder.id,
            ds.TokenHolder.address,
            ds.TokenHolder.tokenAddress,
            ds.TokenHolder.lastUpdateAt,
            ds.TokenHolder.balance
        )

class TokenPricesCollector(CCPricesCollector):
    pass

class ReposCollector(TheGraphCollector):
    def __init__(self, runner, network: str):
        super().__init__('repos', network, ENDPOINTS[network]['aragon'], runner)

    def query(self, **kwargs) -> DSLField:
        ds = self.schema
        return ds.Query.repos(**kwargs).select(
            ds.Repo.id,
            ds.Repo.address,
            ds.Repo.name,
            ds.Repo.node,
            ds.Repo.appCount
        )

class TransactionsCollector(TheGraphCollector):
    def __init__(self, runner, network: str):
        super().__init__('transactions', network, ENDPOINTS[network]['aragon_finance'], runner)

    def query(self, **kwargs) -> DSLField:
        ds = self.schema
        return ds.Query.transactions(**kwargs).select(
            ds.Transaction.id,
            ds.Transaction.orgAddress,
            ds.Transaction.appAddress,
            ds.Transaction.token,
            ds.Transaction.entity,
            ds.Transaction.isIncoming,
            ds.Transaction.amount,
            ds.Transaction.date,
            ds.Transaction.reference
        )

class VotesCollector(TheGraphCollector):
    def __init__(self, runner, network: str):
        super().__init__('votes', network, ENDPOINTS[network]['aragon_voting'], runner)

    def query(self, **kwargs) -> DSLField:
        ds = self.schema
        return ds.Query.votes(**kwargs).select(
            ds.Vote.id,
            ds.Vote.orgAddress,
            ds.Vote.appAddress,
            ds.Vote.creator,
            ds.Vote.originalCreator,
            ds.Vote.metadata,
            ds.Vote.executed,
            ds.Vote.executedAt,
            ds.Vote.startDate,
            ds.Vote.supportRequiredPct,
            ds.Vote.minAcceptQuorum,
            ds.Vote.yea,
            ds.Vote.nay,
            ds.Vote.voteNum,
            ds.Vote.votingPower,
            # Textual information
            ds.Vote.metadata,
        )

class AragonRunner(NetworkRunner):
    name: str = 'aragon'

    def __init__(self, dw=None):
        super().__init__(dw)
        self._collectors: List[Collector] = []

        for n in self.networks: 
            self._collectors.extend([
                AppsCollector(self, n),
                CastsCollector(self, n),
                MiniMeTokensCollector(self, n),
                ReposCollector(self, n),
                TransactionsCollector(self, n),
                TokenHoldersCollector(self, n),
                VotesCollector(self, n),
                oc := OrganizationsCollector(self, n),
                BalancesCollector(self, oc, n),
            ])
        
        self._collectors.append(CCPricesCollector(self))

    @property
    def collectors(self) -> List[Collector]:
        return self._collectors