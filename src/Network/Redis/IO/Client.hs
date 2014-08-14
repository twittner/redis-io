-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Network.Redis.IO.Client where

import Control.Applicative
import Control.Exception (throw, throwIO)
import Control.Monad.Catch
import Control.Monad.Operational
import Control.Monad.Reader
import Data.IORef
import Data.Redis
import Data.Redis.Command
import Data.Word
import Data.Pool hiding (Pool)
import Network.Redis.IO.Connection (Connection)
import Network.Redis.IO.Lazy
import Network.Redis.IO.Settings
import Network.Redis.IO.Timeouts (TimeoutManager)
import Network.Redis.IO.Types (ConnectionError (..))
import Prelude hiding (readList)
import System.Logger.Class hiding (Settings, settings)

import qualified Data.Pool                   as P
import qualified Network.Redis.IO.Connection as C
import qualified System.Logger               as Logger
import qualified Network.Redis.IO.Timeouts   as TM

data Pool = Pool
    { settings :: Settings
    , connPool :: P.Pool Connection
    , logger   :: Logger.Logger
    , failures :: IORef Word64
    , timeouts :: TimeoutManager
    }

newtype Client a = Client
    { client :: ReaderT Pool IO a
    } deriving ( Functor
               , Applicative
               , Monad
               , MonadIO
               , MonadThrow
               , MonadMask
               , MonadCatch
               , MonadReader Pool
               )

instance MonadLogger Client where
    log l m = asks logger >>= \g -> Logger.log g l m

mkPool :: MonadIO m => Logger -> Settings -> m Pool
mkPool g s = liftIO $ do
    t <- TM.create 250
    a <- C.resolve (sHost s) (sPort s)
    Pool s <$> createPool (connOpen t a)
                          connClose
                          (sPoolStripes s)
                          (sIdleTimeout s)
                          (sMaxConnections s)
           <*> pure g
           <*> newIORef 0
           <*> pure t
  where
    connOpen t a = do
        c <- C.connect s g t a
        Logger.debug g $ "client.connect" .= sHost s ~~ msg (show c)
        return c

    connClose c = do
        Logger.debug g $ "client.close" .= sHost s ~~ msg (show c)
        C.close c

shutdown :: MonadIO m => Pool -> m ()
shutdown p = liftIO $ P.destroyAllResources (connPool p)

runClient :: MonadIO m => Pool -> Client a -> m a
runClient p a = liftIO $ runReaderT (client a) p

runRedis :: MonadIO m => Pool -> Redis Lazy IO a -> m a
runRedis p = runClient p . request

request :: Redis Lazy IO a -> Client a
request a = do
    p <- ask
    let c = connPool p
        s = settings p
    liftIO $ case sMaxWaitQueue s of
        Nothing -> withResource c $ \h -> run h a `finally` C.sync h
        Just  q -> tryWithResource c (go p) >>= maybe (retry q c p) return
  where
    go p h = do
        atomicModifyIORef' (failures p) $ \n -> (if n > 0 then n - 1 else 0, ())
        run h a `finally` C.sync h

    retry q c p = do
        k <- atomicModifyIORef' (failures p) $ \n -> (n + 1, n)
        unless (k < q) $
            throwIO ConnectionsBusy
        withResource c (go p)

run :: Connection -> Redis Lazy IO a -> IO a
run h c = do
    r <- viewT c
    case r of
        Return      a          -> return a
        Ping          x :>>= k -> getResult h x (matchStr "PING" "PONG")          >>= run h . k
        Echo          x :>>= k -> getResult h x (readBulk' "ECHO")                >>= run h . k
        Quit          x :>>= k -> getResult h x (matchStr "QUIT" "OK")            >>= run h . k
        Select        x :>>= k -> getResult h x (matchStr "SELECT" "OK")          >>= run h . k
        BgRewriteAOF  x :>>= k -> getResult h x (matchStr "BGREWRITEAOF" "OK")    >>= run h . k
        Auth          x :>>= k -> getResult h x (matchStr "AUTH" "OK")            >>= run h . k
        Get           x :>>= k -> getResult h x (readBulk "GET")                  >>= run h . k
        GetSet        x :>>= k -> getResult h x (readBulk "GETSET")               >>= run h . k
        GetRange      x :>>= k -> getResult h x (readBulk' "GETRANGE")            >>= run h . k
        MGet          x :>>= k -> getResult h x (readList "MGET")                 >>= run h . k
        Set           x :>>= k -> getResult h x fromSet                           >>= run h . k
        SetRange      x :>>= k -> getResult h x (readInt "SETRANGE")              >>= run h . k
        MSet          x :>>= k -> getResult h x (matchStr "MSET" "OK")            >>= run h . k
        MSetNx        x :>>= k -> getResult h x (readBool "MSETNX")               >>= run h . k
        Append        x :>>= k -> getResult h x (readInt "APPEND")                >>= run h . k
        StrLen        x :>>= k -> getResult h x (readInt "STRLEN")                >>= run h . k
        BitCount      x :>>= k -> getResult h x (readInt "BITCOUNT")              >>= run h . k
        BitAnd        x :>>= k -> getResult h x (readInt "BITOP")                 >>= run h . k
        BitOr         x :>>= k -> getResult h x (readInt "BITOP")                 >>= run h . k
        BitXOr        x :>>= k -> getResult h x (readInt "BITOP")                 >>= run h . k
        BitNot        x :>>= k -> getResult h x (readInt "BITOP")                 >>= run h . k
        BitPos        x :>>= k -> getResult h x (readInt "BITPOS")                >>= run h . k
        Keys          x :>>= k -> getResult h x (readList' "KEYS")                >>= run h . k
        RandomKey     x :>>= k -> getResult h x (readBulk "RANDOMKEY")            >>= run h . k
        Rename        x :>>= k -> getResult h x (matchStr "RENAME" "OK")          >>= run h . k
        RenameNx      x :>>= k -> getResult h x (readBool "RENAMENX")             >>= run h . k
        Sort          x :>>= k -> getResult h x (readList' "SORT")                >>= run h . k
        Ttl           x :>>= k -> getResult h x (readTTL "TTL")                   >>= run h . k
        Type          x :>>= k -> getResult h x (readType "TYPE")                 >>= run h . k
        Scan          x :>>= k -> getResult h x (readScan "SCAN")                 >>= run h . k
        Del           x :>>= k -> getResult h x (readInt "DEL")                   >>= run h . k
        HDel          x :>>= k -> getResult h x (readInt "HDEL")                  >>= run h . k
        HGetAll       x :>>= k -> getResult h x (readFields "HGETALL")            >>= run h . k
        LSet          x :>>= k -> getResult h x (matchStr "LSET" "OK")            >>= run h . k
        BLPop         x :>>= k -> getResult h x (readKeyValue "BLPOP")            >>= run h . k
        SRandMember a x :>>= k -> getResult h x (readBulkOrArray "SRANDMEMBER" a) >>= run h . k
        ZInterStore   x :>>= k -> getResult h x (readInt "ZINTERSTORE")           >>= run h . k
        ZRange      a x :>>= k -> getResult h x (readScoreList "ZRANGE" a)        >>= run h . k
        ZRangeByLex   x :>>= k -> getResult h x (readList' "ZRANGEBYLEX")         >>= run h . k

getResult :: Connection -> Resp -> (Resp -> Result a) -> IO (Lazy (Result a))
getResult h x g = do
    r <- newIORef (Left $ RedisError "missing response")
    f <- lazy $ C.sync h >> either Left g <$> readIORef r
    C.request x r h
    return f
{-# INLINE getResult #-}

peel :: Redis Lazy IO (Lazy (Result a)) -> Redis Lazy IO a
peel r = r >>= force >>= either throw return
{-# INLINE peel #-}
