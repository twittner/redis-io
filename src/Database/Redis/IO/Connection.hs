-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE BangPatterns      #-}
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
    , transaction
    , send
    , receive
    ) where

import Control.Applicative
import Control.Exception
import Control.Monad
import Data.Attoparsec.ByteString hiding (Result)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toChunks)
import Data.Foldable (for_, foldlM, toList)
import Data.IORef
import Data.Maybe (isJust)
import Data.Redis
import Data.Sequence (Seq, (|>))
import Data.Int
import Data.Word
import Foreign.C.Types (CInt (..))
import Database.Redis.IO.Settings
import Database.Redis.IO.Types
import Database.Redis.IO.Timeouts (TimeoutManager, withTimeout)
import Network
import Network.Socket hiding (connect, close, send, recv)
import Network.Socket.ByteString (recv, sendMany)
import System.Logger hiding (Settings, settings, close)
import System.Timeout
import Prelude

import qualified Data.ByteString.Lazy.Char8 as Char8
import qualified Data.Sequence as Seq
import qualified Network.Socket as S

data Connection = Connection
    { settings :: !Settings
    , logger   :: !Logger
    , timeouts :: !TimeoutManager
    , address  :: !InetAddr
    , sock     :: !Socket
    , leftover :: !(IORef ByteString)
    , buffer   :: !(IORef (Seq (Resp, IORef Resp)))
    }

instance Show Connection where
    show = Char8.unpack . eval . bytes

instance ToBytes Connection where
    bytes c = bytes (address c) +++ val "#" +++ fd (sock c)

resolve :: String -> Word16 -> IO [InetAddr]
resolve host port =
    map (InetAddr . addrAddress) <$> getAddrInfo (Just hints) (Just host) (Just (show port))
  where
    hints = defaultHints { addrFlags = [AI_ADDRCONFIG], addrSocketType = Stream }

connect :: Settings -> Logger -> TimeoutManager -> InetAddr -> IO Connection
connect t g m a = bracketOnError mkSock S.close $ \s -> do
    ok <- timeout (ms (sConnectTimeout t) * 1000) (S.connect s (sockAddr a))
    unless (isJust ok) $
        throwIO ConnectTimeout
    Connection t g m a s <$> newIORef "" <*> newIORef Seq.empty
  where
    mkSock = socket (familyOf $ sockAddr a) Stream defaultProtocol

    familyOf (SockAddrInet  _ _    ) = AF_INET
    familyOf (SockAddrInet6 _ _ _ _) = AF_INET6
    familyOf (SockAddrUnix  _      ) = AF_UNIX
    familyOf (SockAddrCan   _      ) = AF_CAN

close :: Connection -> IO ()
close = S.close . sock

request :: Resp -> IORef Resp -> Connection -> IO ()
request x y c = modifyIORef' (buffer c) (|> (x, y))

transaction :: Connection -> IO ()
transaction c = do
    buf <- readIORef (buffer c)
    unless (Seq.null buf) $ do
        writeIORef (buffer c) Seq.empty
        case sSendRecvTimeout (settings c) of
            0 -> go buf
            t -> withTimeout (timeouts c) t (abort c) (go buf)
  where
    go buf = do
        let (reqs, vars) = unzip (toList buf)
        send c (cmdMulti : reqs ++ [cmdExecute])
        receive c >>= expect "MULTI" "OK"
        for_ vars $ const $
            receive c >>= expect "*" "QUEUED"
        (lft, res) <- receiveWith c =<< readIORef (leftover c)
        writeIORef (leftover c) lft
        case res of
            Array n resps | n == length vars ->
                mapM_ (uncurry writeIORef) (zip vars resps)
            _ -> throwIO (InvalidResponse "EXEC")

sync :: Connection -> IO ()
sync c = do
    buf <- readIORef (buffer c)
    unless (Seq.null buf) $ do
        writeIORef (buffer c) Seq.empty
        case sSendRecvTimeout (settings c) of
            0 -> go buf
            t -> withTimeout (timeouts c) t (abort c) (go buf)
  where
    go buf = do
        send c (toList $ fmap fst buf)
        bb <- readIORef (leftover c)
        foldlM fetchResult bb (fmap snd buf) >>= writeIORef (leftover c)

    fetchResult :: ByteString -> IORef Resp -> IO ByteString
    fetchResult b r = do
        (b', x) <- receiveWith c b
        writeIORef r x
        return b'

abort :: Connection -> IO a
abort c = do
    err (logger c) $ "connection.timeout" .= show c
    close c
    throwIO $ Timeout (show c)

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

-- Helpers:

fd :: Socket -> Int32
fd !s = let CInt !n = fdSocket s in n

cmdMulti :: Resp
cmdMulti = Array 1 [Bulk "MULTI"]

cmdExecute :: Resp
cmdExecute = Array 1 [Bulk "EXEC"]

expect :: String -> Char8.ByteString -> Resp -> IO ()
expect x y = void . either throwIO return . matchStr x y
