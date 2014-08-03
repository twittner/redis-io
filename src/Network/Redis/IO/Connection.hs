-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Network.Redis.IO.Connection
    ( Connection
    , resolve
    , connect
    , close
    , request
    ) where

import Control.Applicative
import Control.Exception
import Control.Monad
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toChunks)
import Data.Foldable (foldrM)
import Data.IORef
import Data.Maybe (isJust)
import Data.Redis.Resp
import Data.Word
import Network
import Network.Redis.IO.Settings
import Network.Redis.IO.Types
import Network.Redis.IO.Timeouts (TimeoutManager, withTimeout)
import Network.Socket hiding (connect, close, recv, send)
import Network.Socket.ByteString
import Pipes
import Pipes.Attoparsec
import Pipes.Parse
import System.Logger hiding (Settings, settings, close)
import System.Timeout

import qualified Data.ByteString as B
import qualified Network.Socket  as S

data Connection = Connection
    { settings :: !Settings
    , logger   :: !Logger
    , timeouts :: !TimeoutManager
    , sock     :: !Socket
    , producer :: IORef (Producer ByteString IO ())
    }

instance Show Connection where
    show c = "Connection" ++ show (sock c)

resolve :: String -> Word16 -> IO AddrInfo
resolve host port =
    head <$> getAddrInfo (Just hints) (Just host) (Just (show port))
  where
    hints = defaultHints { addrFlags = [AI_ADDRCONFIG], addrSocketType = Stream }

connect :: Settings -> Logger -> TimeoutManager -> AddrInfo -> IO Connection
connect t g m a =
    bracketOnError mkSock S.close $ \s -> do
        ok <- timeout (ms (sConnectTimeout t) * 1000) (S.connect s (addrAddress a))
        unless (isJust ok) $
            throwIO ConnectTimeout
        Connection t g m s <$> newIORef (fromSock s)
  where
    mkSock = socket (addrFamily a) (addrSocketType a) (addrProtocol a)

    fromSock :: Socket -> Producer ByteString IO ()
    fromSock s = do
        x <- lift $ recv s 4096
        when (B.null x) $
            lift $ throwIO ConnectionClosed
        yield x
        fromSock s

close :: Connection -> IO ()
close = S.close . sock

request :: [Resp] -> Connection -> IO [Either String Resp]
request a c =
    withTimeout (timeouts c) (sSendRecvTimeout (settings c)) abort $ do
        sendMany (sock c) $
            concatMap (toChunks . encode) a
        prod    <- readIORef (producer c)
        (r, p') <- foldrM getResult ([], prod) a
        writeIORef (producer c) p'
        return (reverse r)
  where
    abort = do
        let str = show c
        err (logger c) $ "connection.timeout" .= str
        close c
        throwIO (Timeout str)

    getResult _ (x, p) = do
        (y, p') <- runStateT (parse resp) p
        case y of
            Nothing        -> throwIO ConnectionClosed
            Just (Left  e) -> throwIO $ InternalError (peMessage e)
            Just (Right r) -> return  $ (Right r : x, p')
