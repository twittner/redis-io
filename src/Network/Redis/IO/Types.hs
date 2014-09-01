-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Network.Redis.IO.Types where

import Control.Exception (Exception, SomeException, catch)
import Data.Typeable

newtype Milliseconds = Ms { ms :: Int } deriving (Eq, Show, Num)

-----------------------------------------------------------------------------
-- ConnectionError

data ConnectionError
    = ConnectionsBusy  -- ^ All connections are in use and wait queue is full.
    | ConnectionClosed -- ^ The connection has been closed unexpectedly.
    | ConnectTimeout   -- ^ Connecting to redis server took too long.
    deriving Typeable

instance Exception ConnectionError

instance Show ConnectionError where
    show ConnectionsBusy   = "Network.Redis.IO.ConnectionsBusy"
    show ConnectionClosed  = "Network.Redis.IO.ConnectionClosed"
    show ConnectTimeout    = "Network.Redis.IO.ConnectTimeout"

-----------------------------------------------------------------------------
-- InternalError

-- | General error, e.g. parsing redis responses failed.
data InternalError = InternalError String
    deriving Typeable

instance Exception InternalError

instance Show InternalError where
    show (InternalError e) = "Network.Redis.IO.InternalError: " ++ show e

-----------------------------------------------------------------------------
-- Timeout

-- | A single send-receive cycle took too long.
data Timeout = Timeout String
    deriving Typeable

instance Exception Timeout

instance Show Timeout where
    show (Timeout e) = "Network.Redis.IO.Timeout: " ++ e

ignore :: IO () -> IO ()
ignore a = catch a (const $ return () :: SomeException -> IO ())
{-# INLINE ignore #-}
