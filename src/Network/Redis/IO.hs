-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Network.Redis.IO
    ( -- * Redis client
      Client
    , runRedis
    , stepwise
    , pipelined
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
    , setMaxWaitQueue
    , setPoolStripes
    , setConnectTimeout
    , setSendRecvTimeout

    -- * Exceptions
    , ConnectionError (..)
    , InternalError   (..)
    , Timeout         (..)
    ) where

import Network.Redis.IO.Client
import Network.Redis.IO.Settings
import Network.Redis.IO.Types
