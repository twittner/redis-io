-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Database.Redis.IO.Settings where

import Data.Time
import Data.Word
import Database.Redis.IO.Types (Milliseconds (..))

data Settings = Settings
    { sHost            :: !String
    , sPort            :: !Word16
    , sIdleTimeout     :: !NominalDiffTime
    , sMaxConnections  :: !Int
    , sPoolStripes     :: !Int
    , sConnectTimeout  :: !Milliseconds
    , sSendRecvTimeout :: !Milliseconds
    }

-- | Default settings.
--
-- * host = localhost
-- * port = 6379
-- * idle timeout = 60s
-- * stripes = 2
-- * connections per stripe = 25
-- * connect timeout = 5s
-- * send-receive timeout = 10s
defSettings :: Settings
defSettings = Settings "localhost" 6379
    60    -- idle timeout
    50    -- max connections per stripe
    2     -- max stripes
    5000  -- connect timeout
    10000 -- send and recv timeout (sum)

setHost :: String -> Settings -> Settings
setHost v s = s { sHost = v }

setPort :: Word16 -> Settings -> Settings
setPort v s = s { sPort = v }

setIdleTimeout :: NominalDiffTime -> Settings -> Settings
setIdleTimeout v s = s { sIdleTimeout = v }

-- | Maximum connections per pool stripe.
setMaxConnections :: Int -> Settings -> Settings
setMaxConnections v s = s { sMaxConnections = v }

setPoolStripes :: Int -> Settings -> Settings
setPoolStripes v s
    | v < 1     = error "Network.Redis.IO.Settings: at least one stripe required"
    | otherwise = s { sPoolStripes = v }

-- | When a pool connection is opened, connect timeout is the maximum time
-- we are willing to wait for the connection attempt to the redis server to
-- succeed.
setConnectTimeout :: NominalDiffTime -> Settings -> Settings
setConnectTimeout v s = s { sConnectTimeout = Ms $ round (1000 * v) }

setSendRecvTimeout :: NominalDiffTime -> Settings -> Settings
setSendRecvTimeout v s = s { sSendRecvTimeout = Ms $ round (1000 * v) }
