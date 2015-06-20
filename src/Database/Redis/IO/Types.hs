-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Database.Redis.IO.Types where

import Control.Exception (Exception, SomeException, catch)
import Data.IP
import Data.Typeable
import Network.Socket (SockAddr (..), PortNumber)
import System.Logger.Message

newtype Milliseconds = Ms { ms :: Int } deriving (Eq, Show, Num)

-----------------------------------------------------------------------------
-- InetAddr

newtype InetAddr = InetAddr { sockAddr :: SockAddr } deriving (Eq, Ord)

instance Show InetAddr where
    show (InetAddr (SockAddrInet p a)) =
        let i = fromIntegral p :: Int in
        shows (fromHostAddress a) . showString ":" . shows i $ ""
    show (InetAddr (SockAddrInet6 p _ a _)) =
        let i = fromIntegral p :: Int in
        shows (fromHostAddress6 a) . showString ":" . shows i $ ""
    show (InetAddr (SockAddrUnix unix)) = unix
    show (InetAddr (SockAddrCan int32)) = show int32

instance ToBytes InetAddr where
    bytes (InetAddr (SockAddrInet p a)) =
        let i = fromIntegral p :: Int in
        show (fromHostAddress a) +++ val ":" +++ i
    bytes (InetAddr (SockAddrInet6 p _ a _)) =
        let i = fromIntegral p :: Int in
        show (fromHostAddress6 a) +++ val ":" +++ i
    bytes (InetAddr (SockAddrUnix unix)) = bytes unix
    bytes (InetAddr (SockAddrCan int32)) = bytes int32

ip2inet :: PortNumber -> IP -> InetAddr
ip2inet p (IPv4 a) = InetAddr $ SockAddrInet p (toHostAddress a)
ip2inet p (IPv6 a) = InetAddr $ SockAddrInet6 p 0 (toHostAddress6 a) 0

-----------------------------------------------------------------------------
-- ConnectionError

data ConnectionError
    = ConnectionsBusy  -- ^ All connections are in use.
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
newtype InternalError = InternalError String
    deriving Typeable

instance Exception InternalError

instance Show InternalError where
    show (InternalError e) = "Network.Redis.IO.InternalError: " ++ show e

-----------------------------------------------------------------------------
-- Timeout

-- | A single send-receive cycle took too long.
newtype Timeout = Timeout String
    deriving Typeable

instance Exception Timeout

instance Show Timeout where
    show (Timeout e) = "Network.Redis.IO.Timeout: " ++ e

ignore :: IO () -> IO ()
ignore a = catch a (const $ return () :: SomeException -> IO ())
{-# INLINE ignore #-}
