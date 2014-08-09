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
    , sync
    ) where

import Control.Applicative
import Control.Exception
import Control.Monad
import Data.Attoparsec.ByteString hiding (Result)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (isPrefixOf)
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
import Network.Socket hiding (connect, close, recv)
import Network.Socket.ByteString (recv, sendMany)
import System.Logger hiding (Settings, Error, settings, close)
import System.Timeout

import qualified Data.Sequence  as Seq
import qualified Network.Socket as S

data Connection = Connection
    { settings :: !Settings
    , logger   :: !Logger
    , timeouts :: !TimeoutManager
    , sock     :: !Socket
    , leftover :: IORef ByteString
    , buffer   :: IORef (Seq (Resp, IORef (Result Resp)))
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

request :: Resp -> IORef (Result Resp) -> Connection -> IO ()
request x y c = modifyIORef' (buffer c) (|> (x, y))

sync :: Connection -> IO ()
sync c = do
    a <- readIORef (buffer c)
    unless (Seq.null a) $ do
        writeIORef (buffer c) Seq.empty
        withTimeout (timeouts c) (sSendRecvTimeout (settings c)) abort $ do
            sendMany (sock c) $
                concatMap (toChunks . encode) (toList $ fmap fst a)
            bb <- readIORef (leftover c)
            foldlM fetchResult bb (fmap snd a) >>= writeIORef (leftover c)
  where
    abort = do
        err (logger c) $ "connection.timeout" .= show c
        close c
        throwIO $ Timeout (show c)

    fetchResult :: ByteString -> IORef (Result Resp) -> IO ByteString
    fetchResult b r = do
        res <- parseWith (recv (sock c) 4096) resp b
        case res of
            Fail    _ _ m -> throwIO $ InternalError m
            Partial _     -> throwIO $ InternalError "partial result"
            Done    b'  x -> writeIORef r (fromResp x) >> return b'

fromResp :: Resp -> (Result Resp)
fromResp (Err e)
    | "WRONGTYPE" `isPrefixOf` e = Left WrongType
    | otherwise                  = Left $ Error e
fromResp r = Right r
