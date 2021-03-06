-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.Redis.IO
    ( -- * Redis client
      Client
    , MonadClient (..)
    , runRedis
    , stepwise
    , pipelined
    , transactional
    , pubSub

    -- * Connection pool
    , Pool
    , mkPool
    , shutdown

    -- * Client and pool settings
    , Settings
    , defSettings
    , setHost
    , setPort
    , setIdleTimeout
    , setMaxConnections
    , setPoolStripes
    , setConnectTimeout
    , setSendRecvTimeout

    -- * Exceptions
    , ConnectionError    (..)
    , InternalError      (..)
    , Timeout            (..)
    , TransactionFailure (..)

    -- * Re-exports
    , module Data.Redis.Command
    ) where

import Database.Redis.IO.Client
import Database.Redis.IO.Settings
import Database.Redis.IO.Types
import Data.Redis.Command
