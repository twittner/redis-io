-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module Database.Redis.IO.Connection
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
import Data.Attoparsec.ByteString hiding (Result)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toChunks)
import Data.Foldable hiding (concatMap)
import Data.IORef
import Data.Maybe (isJust)
import Data.Redis
import Data.Sequence (Seq, (|>))
import Data.Word
import Database.Redis.IO.Settings
import Database.Redis.IO.Types
import Database.Redis.IO.Timeouts (TimeoutManager, withTimeout)
import Network
import Network.Socket hiding (connect, close, send, recv)
import Network.Socket.ByteString (recv, sendMany)
import System.Logger hiding (Settings, settings, close)
import System.Timeout
import Prelude

import qualified Data.Sequence  as Seq
import qualified Network.Socket as S

data Connection = Connection
    { settings :: !Settings
    , logger   :: !Logger
    , timeouts :: !TimeoutManager
    , sock     :: !Socket
    , leftover :: !(IORef ByteString)
    , buffer   :: !(IORef (Seq (Resp, IORef Resp)))
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
    Connection t g m s <$> newIORef "" <*> newIORef Seq.empty
  where
    mkSock = socket (addrFamily a) (addrSocketType a) (addrProtocol a)

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
        bb <- readIORef (leftover c)
        foldlM fetchResult bb (fmap snd a) >>= writeIORef (leftover c)

    abort = do
        err (logger c) $ "connection.timeout" .= show c
        close c
        throwIO $ Timeout (show c)

    fetchResult :: ByteString -> IORef Resp -> IO ByteString
    fetchResult b r = do
        (b', x) <- receiveWith c b
        writeIORef r x
        return b'

send :: Connection -> [Resp] -> IO ()
send c = sendMany (sock c) . concatMap (toChunks . encode)

receive :: Connection -> IO Resp
receive c = do
    bstr   <- readIORef (leftover c)
    (b, x) <- receiveWith c bstr
    writeIORef (leftover c) b
    return x

receiveWith :: Connection -> ByteString -> IO (ByteString, Resp)
receiveWith c b = do
    res <- parseWith (recv (sock c) 4096) resp b
    case res of
        Fail    _  _ m -> throwIO $ InternalError m
        Partial _      -> throwIO $ InternalError "partial result"
        Done    b'   x -> (b',) <$> errorCheck x

errorCheck :: Resp -> IO Resp
errorCheck (Err e) = throwIO $ RedisError e
errorCheck r       = return r
