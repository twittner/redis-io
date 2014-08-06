-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GADTs             #-}
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
import Data.Foldable (foldlM)
import Data.IORef
import Data.Maybe (isJust)
import Data.Redis.Resp
import Data.Redis.Command
import Data.Word
import Network
import Network.Redis.IO.Fetch
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

import qualified Data.ByteString      as B
import qualified Data.ByteString.Lazy as Lazy
import qualified Network.Socket       as S

type Src = Producer ByteString IO ()

data Connection = Connection
    { settings :: !Settings
    , logger   :: !Logger
    , timeouts :: !TimeoutManager
    , sock     :: !Socket
    , producer :: IORef Src
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

request :: [Request] -> Connection -> IO ()
request a c =
    withTimeout (timeouts c) (sSendRecvTimeout (settings c)) abort $ do
        sendMany (sock c) $
            concatMap (toChunks . encoded) a
        prod <- readIORef (producer c)
        foldlM getResult prod a >>= writeIORef (producer c)
  where
    abort = do
        let str = show c
        err (logger c) $ "connection.timeout" .= str
        close c
        throwIO (Timeout str)

    getResult :: Src -> Request -> IO Src
    getResult p r = do
        (x, p') <- runStateT (parse resp) p
        case x of
            Nothing        -> throwIO ConnectionClosed
            Just (Left  e) -> throwIO $ InternalError (peMessage e)
            Just (Right y) -> respond r y >> return p'

respond :: Request -> Resp -> IO ()
respond (Request (Ping _) r) (Str x) = writeIORef r (Value x) -- FIXME!
respond (Request (Set _) r) x = case x of
    Err _    -> writeIORef r (Value False)
    NullBulk -> writeIORef r (Value False)
    _        -> writeIORef r (Value True)
respond (Request (Get _) r) x = case x of
    Err  _ -> writeIORef r (Value Nothing)
    Str  y -> writeIORef r (Value (Just y))
    Bulk y -> writeIORef r (Value (Just y))
    _      -> writeIORef r (Value Nothing)


encoded :: Request -> Lazy.ByteString
encoded (Request (Ping x) _) = encode x
encoded (Request (Get  x) _) = encode x
encoded (Request (Set  x) _) = encode x
