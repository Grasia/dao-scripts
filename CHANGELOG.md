# Changelog
All notable changes to this project will be documented in this file.

## 1.5.6 - 2025-06-01
- Added Python 3.13 support
- Pinned down `gql` library

## 1.5.5 - 2025-05-30
- Updated aragon blocks endpoint

## 1.5.4 - 2025-05-04
- Quickfix: Temporarily removed deployment health check
  - See [graphprotocol/graph-node#5983](https://github.com/graphprotocol/graph-node/issues/5983)

## 1.5.3 - 2024-07-25
- Reverted pin down kaggle version

## 1.5.2 - 2024-07-24
- Pin down kaggle version until [Kaggle/kaggle-api#611](https://github.com/Kaggle/kaggle-api/issues/611) is fixed

## 1.5.1 - 2024-06-05
- Multiple small bugfixes

## 1.5.0 - 2024-06-04
- Avoid errors when no new data is available

## 1.4.1 - 2024-06-03
- Added debug logging to `dao-utils-upload-dw`

## 1.4.0 - 2024-06-03
- Fixed bug when process was killed
- Updated endpoint network names

## 1.3.1 - 2024-05-27
- Fixed bug with `--delete-force`

## 1.3.0 - 2024-05-24
- Improved and standarized logging #10
- The Graph Hosted Service sunsetting
  - Added DAOA_THE_GRAPH_API_KEY variable (now needed)

## 1.2.2 - 2023-12-13
- Changed daohaus names cache folder

## 1.2.1 - 2023-12-12
- Fixed error that made impossible getting DAOhaus proposals

## 1.2.0 - 2023-12-12
- Getting textual information from DAOhaus proposals
- Getting textual information from Aragon proposals

## 1.1.14 - 2023-10-24
- Updated uploadDataWarehouse.py to report Zenodo API errors
- Skipped 1.1.13 to trigger CI

## 1.1.12 - 2023-07-20
- Ignoring info logs from `gql.transport.requests` as they took too much space (#3)
- Saving version.txt and update_date.txt with a trailing newline to make printing to screen easier
- Fixed aragon/casts #4

## 1.1.11 - 2023-07-19
- Added utils scripts: `dao-utils-upload-dw`

## 1.1.10 - 2023-07-19
- Stopped using daohaus-stats endpoint
- Removed some fields from DAOHaus data
  - moloches
    - title
    - timestamp (now createdAt)
    - proposalCount
    - memberCount
    - voteCount
    - rageQuitCount
    - totalGas
- Added some fields
  - moloch
    - createdAt (formerly timestamp)
    - totalShares
    - totalLoot
    - guildBankAddress
  - vote
    - memberPower
  - member
    - tokenTribute

## 1.1.9 - 2023-05-11
- Obtaining textual fields from DAOstack proposals
  - title
  - description
  - url
  - confidence
  - confidenceThreshold

## 1.1.8 - 2023-01-23
- Updated cache-scripts to get more daostack parameters
  - thresholdConst
  - minimumDaoBounty
  - daoBountyConst

## 1.1.7 - 2022-12-13
- Obtaining more fields from DAOstack proposals
  - queuedVotePeriodLimit
  - boostedVotePeriodLimit

## 1.1.6 - 2022-10-22
- Obtaining more time fields from DAOstack proposals

## 1.1.5 - 2022-10-17
- Remove DAOstack phantom DAOs [#120](https://github.com/Grasia/dao-analyzer/issues/120)
- Added option to obtain non-registered DAOs from DAOstack (`--daostack-all`)

## 1.1.4 - 2022-07-15
- Added postProcessor to add a `dao` field to reputation mints and burns
- Not getting reputation mints/burns of amount 0 (not useful)

## 1.1.3 - 2022-07-11
- Added competitionId to daostack proposals

## 1.1.2 - 2022-06-29
- Added ReputationMint and ReputationBurn collectors to DAOstack

## 1.1.1 - 2022-06-10
- Added originalCreator field to Aragon Voting subgraph

## 1.1.0 - 2022-05
- Used `_change_block` filter to make every subgraph updatable
- Fixed cryptocompare error
- Fixed requests to `_blocks` endpoint 
- Added --skip-token-balances option to cli

## 1.0.3 - 2022-03-24
- Obtaining assets of DAOs
- Added BlockScout balances collector
- Added CryptoCompare token prices collector
- Some changes on Class sctructure