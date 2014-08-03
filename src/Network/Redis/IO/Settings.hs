-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Network.Redis.IO.Settings where

import Data.Time
import Data.Word
import Network.Redis.IO.Types (Milliseconds (..))

data Settings = Settings
    { sHost            :: String
    , sPort            :: Word16
    , sIdleTimeout     :: NominalDiffTime
    , sMaxConnections  :: Int
    , sPoolStripes     :: Int
    , sMaxWaitQueue    :: Maybe Word64
    , sConnectTimeout  :: Milliseconds
    , sSendRecvTimeout :: Milliseconds
    }

defSettings :: Settings
defSettings = Settings "localhost" 6379
    60      -- idle timeout
    25      -- max connections per stripe
    2       -- max stripes
    Nothing -- max wait queue
    5000    -- connect timeout
    10000   -- send and recv timeout (sum)

setHost :: String -> Settings -> Settings
setHost v s = s { sHost = v }

setPort :: Word16 -> Settings -> Settings
setPort v s = s { sPort = v }

setIdleTimeout :: NominalDiffTime -> Settings -> Settings
setIdleTimeout v s = s { sIdleTimeout = v }

-- | Maximum connections per pool stripe.
setMaxConnections :: Int -> Settings -> Settings
setMaxConnections v s = s { sMaxConnections = v }

setMaxWaitQueue :: Word64 -> Settings -> Settings
setMaxWaitQueue v s = s { sMaxWaitQueue = Just v }

setPoolStripes :: Int -> Settings -> Settings
setPoolStripes v s
    | v < 1     = error "Network.Redis.IO.Settings: at least one stripe required"
    | otherwise = s { sPoolStripes = v }

setConnectTimeout :: NominalDiffTime -> Settings -> Settings
setConnectTimeout v s = s { sConnectTimeout = Ms $ round (1000 * v) }

setSendRecvTimeout :: NominalDiffTime -> Settings -> Settings
setSendRecvTimeout v s = s { sSendRecvTimeout = Ms $ round (1000 * v) }
