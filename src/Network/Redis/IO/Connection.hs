-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module Network.Redis.IO.Connection
    ( Connection
    , settings
    , resolve
    , connect
    , close
    , request
    , sync
    , send
    , receive
    ) where

import Control.Applicative
import Control.Exception
import Control.Monad
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toChunks)
import Data.Foldable hiding (concatMap)
import Data.IORef
import Data.Maybe (isJust)
import Data.Redis
import Data.Sequence (Seq, (|>))
import Data.Word
import Network
import Network.Redis.IO.Settings
import Network.Redis.IO.Types
import Network.Redis.IO.Timeouts (TimeoutManager, withTimeout)
import Network.Socket hiding (connect, close, send, recv)
import Network.Socket.ByteString (recv, sendMany)
import Pipes
import Pipes.Attoparsec
import Pipes.Parse
import System.Logger hiding (Settings, settings, close)
import System.Timeout

import qualified Data.ByteString as B
import qualified Data.Sequence   as Seq
import qualified Network.Socket  as S

data Connection = Connection
    { settings :: !Settings
    , logger   :: !Logger
    , timeouts :: !TimeoutManager
    , sock     :: !Socket
    , producer :: IORef (Producer ByteString IO ())
    , buffer   :: IORef (Seq (Resp, IORef Resp))
    }

instance Show Connection where
    show c = "Connection" ++ show (sock c)

resolve :: String -> Word16 -> IO AddrInfo
resolve host port =
    head <$> getAddrInfo (Just hints) (Just host) (Just (show port))
  where
    hints = defaultHints { addrFlags = [AI_ADDRCONFIG], addrSocketType = Stream }

connect :: Settings -> Logger -> TimeoutManager -> AddrInfo -> IO Connection
connect t g m a = bracketOnError mkSock S.close $ \s -> do
    ok <- timeout (ms (sConnectTimeout t) * 1000) (S.connect s (addrAddress a))
    unless (isJust ok) $
        throwIO ConnectTimeout
    Connection t g m s <$> newIORef (fromSock s) <*> newIORef Seq.empty
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

request :: Resp -> IORef Resp -> Connection -> IO ()
request x y c = modifyIORef' (buffer c) (|> (x, y))

sync :: Connection -> IO ()
sync c = do
    a <- readIORef (buffer c)
    unless (Seq.null a) $ do
        writeIORef (buffer c) Seq.empty
        case sSendRecvTimeout (settings c) of
            0 -> go a
            t -> withTimeout (timeouts c) t abort (go a)
  where
    go a = do
        send c (toList $ fmap fst a)
        prod <- readIORef (producer c)
        foldlM fetchResult prod (fmap snd a) >>= writeIORef (producer c)

    abort = do
        err (logger c) $ "connection.timeout" .= show c
        close c
        throwIO $ Timeout (show c)

    fetchResult :: Producer ByteString IO () -> IORef Resp -> IO (Producer ByteString IO ())
    fetchResult p r = do
        (p', x) <- receiveWith p
        writeIORef r x
        return p'

send :: Connection -> [Resp] -> IO ()
send c = sendMany (sock c) . concatMap (toChunks . encode)

receive :: Connection -> IO Resp
receive c = do
    prod   <- readIORef (producer c)
    (p, x) <- receiveWith prod
    writeIORef (producer c) p
    return x

receiveWith :: Producer ByteString IO () -> IO (Producer ByteString IO (), Resp)
receiveWith p = do
    (x, p') <- runStateT (parse resp) p
    case x of
        Nothing        -> throwIO ConnectionClosed
        Just (Left e)  -> throwIO $ InternalError (peMessage e)
        Just (Right y) -> (p',) <$> errorCheck y

errorCheck :: Resp -> IO Resp
errorCheck (Err e) = throwIO $ RedisError e
errorCheck r       = return r
