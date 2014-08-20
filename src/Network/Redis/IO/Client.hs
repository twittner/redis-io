-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes                 #-}

module Network.Redis.IO.Client where

import Control.Applicative
import Control.Exception (throw, throwIO)
import Control.Monad.Catch
import Control.Monad.Operational
import Control.Monad.Reader
import Data.ByteString (ByteString)
import Data.Int
import Data.IORef
import Data.Redis
import Data.Word
import Data.Pool hiding (Pool)
import Network.Redis.IO.Connection (Connection)
import Network.Redis.IO.Settings
import Network.Redis.IO.Timeouts (TimeoutManager)
import Network.Redis.IO.Types (ConnectionError (..))
import Prelude hiding (readList)
import System.Logger.Class hiding (Settings, settings, eval)
import System.IO.Unsafe (unsafeInterleaveIO)

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

runRedis :: MonadIO m => Pool -> Client a -> m a
runRedis p a = liftIO $ runReaderT (client a) p

request :: Redis IO a -> Client a
request a = withConnection (flip (eval getDirect) a)

pipeline :: Redis IO a -> Client a
pipeline a = withConnection (flip (eval getResult) a)

pubSub :: (ByteString -> ByteString -> PubSub IO ()) -> PubSub IO () -> Client ()
pubSub f a = withConnection (loop a)
  where
    loop :: PubSub IO () -> Connection -> IO ()
    loop p h = do
        commands h p
        r <- responses h
        case r of
            Nothing -> return ()
            Just  k -> loop k h

    commands :: Connection -> PubSub IO () -> IO ()
    commands h c = do
        r <- viewT c
        case r of
            Return              x -> return x
            Subscribe    x :>>= k -> C.send h [x] >>= commands h . k
            Unsubscribe  x :>>= k -> C.send h [x] >>= commands h . k
            PSubscribe   x :>>= k -> C.send h [x] >>= commands h . k
            PUnsubscribe x :>>= k -> C.send h [x] >>= commands h . k

    responses :: Connection -> IO (Maybe (PubSub IO ()))
    responses h = do
        m <- readPushMessage <$> C.receive h
        case m of
            Right (Message ch ms)          -> return (Just $ f ch ms)
            Right (UnsubscribeMessage _ 0) -> return Nothing
            Right _                        -> responses h
            Left  e                        -> throwIO e

eval :: (forall a. Connection -> Resp -> (Resp -> Result a) -> IO a) -> Connection -> Redis IO b -> IO b
eval f conn red = run conn red
  where
    run :: Connection -> Redis IO a -> IO a
    run h c = do
        r <- viewT c
        case r of
            Return a -> return a

            -- Connection
            Ping   x :>>= k -> f h x (matchStr "PING" "PONG") >>= run h . k
            Echo   x :>>= k -> f h x (readBulk "ECHO")        >>= run h . k
            Auth   x :>>= k -> f h x (matchStr "AUTH" "OK")   >>= run h . k
            Quit   x :>>= k -> f h x (matchStr "QUIT" "OK")   >>= run h . k
            Select x :>>= k -> f h x (matchStr "SELECT" "OK") >>= run h . k

            -- Server
            BgRewriteAOF x :>>= k -> f h x (anyStr "BGREWRITEAOF")    >>= run h . k
            BgSave       x :>>= k -> f h x (anyStr "BGSAVE")          >>= run h . k
            Save         x :>>= k -> f h x (matchStr "SAVE" "OK")     >>= run h . k
            FlushAll     x :>>= k -> f h x (matchStr "FLUSHALL" "OK") >>= run h . k
            FlushDb      x :>>= k -> f h x (matchStr "FLUSHDB" "OK")  >>= run h . k
            DbSize       x :>>= k -> f h x (readInt "DBSIZE")         >>= run h . k
            LastSave     x :>>= k -> f h x (readInt "LASTSAVE")       >>= run h . k

            -- Transactions
            Multi   x :>>= k -> f h x (matchStr "MULTI" "OK")   >>= run h . k
            Watch   x :>>= k -> f h x (matchStr "WATCH" "OK")   >>= run h . k
            Unwatch x :>>= k -> f h x (matchStr "UNWATCH" "OK") >>= run h . k
            Discard x :>>= k -> f h x (matchStr "DISCARD" "OK") >>= run h . k
            Exec    x :>>= k -> f h x (readList "EXEC")         >>= run h . k
            ExecRaw x :>>= k -> f h x return                    >>= run h . k

            -- Keys
            Del       x :>>= k -> f h x (readInt "DEL")             >>= run h . k
            Dump      x :>>= k -> f h x (readBulk'Null "DUMP")      >>= run h . k
            Exists    x :>>= k -> f h x (readBool "EXISTS")         >>= run h . k
            Expire    x :>>= k -> f h x (readBool "EXPIRE")         >>= run h . k
            ExpireAt  x :>>= k -> f h x (readBool "EXPIREAT")       >>= run h . k
            Persist   x :>>= k -> f h x (readBool "PERSIST")        >>= run h . k
            Keys      x :>>= k -> f h x (readList "KEYS")           >>= run h . k
            RandomKey x :>>= k -> f h x (readBulk'Null "RANDOMKEY") >>= run h . k
            Rename    x :>>= k -> f h x (matchStr "RENAME" "OK")    >>= run h . k
            RenameNx  x :>>= k -> f h x (readBool "RENAMENX")       >>= run h . k
            Ttl       x :>>= k -> f h x (readTTL "TTL")             >>= run h . k
            Type      x :>>= k -> f h x (readType "TYPE")           >>= run h . k
            Scan      x :>>= k -> f h x (readScan "SCAN")           >>= run h . k

            -- Strings
            Append   x :>>= k -> f h x (readInt "APPEND")          >>= run h . k
            Get      x :>>= k -> f h x (readBulk'Null "GET")       >>= run h . k
            GetRange x :>>= k -> f h x (readBulk "GETRANGE")       >>= run h . k
            GetSet   x :>>= k -> f h x (readBulk'Null "GETSET")    >>= run h . k
            MGet     x :>>= k -> f h x (readListOfMaybes "MGET")   >>= run h . k
            MSet     x :>>= k -> f h x (matchStr "MSET" "OK")      >>= run h . k
            MSetNx   x :>>= k -> f h x (readBool "MSETNX")         >>= run h . k
            Set      x :>>= k -> f h x fromSet                     >>= run h . k
            SetRange x :>>= k -> f h x (readInt "SETRANGE")        >>= run h . k
            StrLen   x :>>= k -> f h x (readInt "STRLEN")          >>= run h . k

            -- Bits
            BitAnd   x :>>= k -> f h x (readInt "BITOP")    >>= run h . k
            BitCount x :>>= k -> f h x (readInt "BITCOUNT") >>= run h . k
            BitNot   x :>>= k -> f h x (readInt "BITOP")    >>= run h . k
            BitOr    x :>>= k -> f h x (readInt "BITOP")    >>= run h . k
            BitPos   x :>>= k -> f h x (readInt "BITPOS")   >>= run h . k
            BitXOr   x :>>= k -> f h x (readInt "BITOP")    >>= run h . k
            GetBit   x :>>= k -> f h x (readInt "GETBIT")   >>= run h . k
            SetBit   x :>>= k -> f h x (readInt "SETBIT")   >>= run h . k

            -- Numeric
            Decr        x :>>= k -> f h x (readInt "DECR")         >>= run h . k
            DecrBy      x :>>= k -> f h x (readInt "DECRBY")       >>= run h . k
            Incr        x :>>= k -> f h x (readInt "INCR")         >>= run h . k
            IncrBy      x :>>= k -> f h x (readInt "INCRBY")       >>= run h . k
            IncrByFloat x :>>= k -> f h x (readBulk "INCRBYFLOAT") >>= run h . k

            -- Hashes
            HDel         x :>>= k -> f h x (readInt "HDEL")           >>= run h . k
            HExists      x :>>= k -> f h x (readBool "HEXISTS")       >>= run h . k
            HGet         x :>>= k -> f h x (readBulk'Null "HGET")     >>= run h . k
            HGetAll      x :>>= k -> f h x (readFields "HGETALL")     >>= run h . k
            HIncrBy      x :>>= k -> f h x (readInt "HINCRBY")        >>= run h . k
            HIncrByFloat x :>>= k -> f h x (readBulk "HINCRBYFLOAT")  >>= run h . k
            HKeys        x :>>= k -> f h x (readList "HKEYS")         >>= run h . k
            HLen         x :>>= k -> f h x (readInt "HLEN")           >>= run h . k
            HMGet        x :>>= k -> f h x (readListOfMaybes "HMGET") >>= run h . k
            HMSet        x :>>= k -> f h x (matchStr "HMSET" "OK")    >>= run h . k
            HSet         x :>>= k -> f h x (readBool "HSET")          >>= run h . k
            HSetNx       x :>>= k -> f h x (readBool "HSETNX")        >>= run h . k
            HVals        x :>>= k -> f h x (readList "HVALS")         >>= run h . k
            HScan        x :>>= k -> f h x (readScan "HSCAN")         >>= run h . k

            -- Lists
            BLPop      t x :>>= k -> getNow (withTimeout t h) x (readKeyValue "BLPOP")       >>= run h . k
            BRPop      t x :>>= k -> getNow (withTimeout t h) x (readKeyValue "BRPOP")       >>= run h . k
            BRPopLPush t x :>>= k -> getNow (withTimeout t h) x (readBulk'Null "BRPOPLPUSH") >>= run h . k
            LIndex       x :>>= k -> f h x (readBulk'Null "LINDEX")     >>= run h . k
            LInsert      x :>>= k -> f h x (readInt "LINSERT")          >>= run h . k
            LLen         x :>>= k -> f h x (readInt "LLEN")             >>= run h . k
            LPop         x :>>= k -> f h x (readBulk'Null "LPOP")       >>= run h . k
            LPush        x :>>= k -> f h x (readInt "LPUSH")            >>= run h . k
            LPushX       x :>>= k -> f h x (readInt "LPUSHX")           >>= run h . k
            LRange       x :>>= k -> f h x (readList "LRANGE")          >>= run h . k
            LRem         x :>>= k -> f h x (readInt "LREM")             >>= run h . k
            LSet         x :>>= k -> f h x (matchStr "LSET" "OK")       >>= run h . k
            LTrim        x :>>= k -> f h x (matchStr "LTRIM" "OK")      >>= run h . k
            RPop         x :>>= k -> f h x (readBulk'Null "RPOP")       >>= run h . k
            RPopLPush    x :>>= k -> f h x (readBulk'Null "RPOPLPUSH")  >>= run h . k
            RPush        x :>>= k -> f h x (readInt "RPUSH")            >>= run h . k
            RPushX       x :>>= k -> f h x (readInt "RPUSHX")           >>= run h . k

            -- Sets
            SAdd          x :>>= k -> f h x (readInt "SADD")                 >>= run h . k
            SCard         x :>>= k -> f h x (readInt "SCARD")                >>= run h . k
            SDiff         x :>>= k -> f h x (readList "SDIFF")               >>= run h . k
            SDiffStore    x :>>= k -> f h x (readInt "SDIFFSTORE")           >>= run h . k
            SInter        x :>>= k -> f h x (readList "SINTER")              >>= run h . k
            SInterStore   x :>>= k -> f h x (readInt "SINTERSTORE")          >>= run h . k
            SIsMember     x :>>= k -> f h x (readBool "SISMEMBER")           >>= run h . k
            SMembers      x :>>= k -> f h x (readList "SMEMBERS")            >>= run h . k
            SMove         x :>>= k -> f h x (readBool "SMOVE")               >>= run h . k
            SPop          x :>>= k -> f h x (readBulk'Null "SPOP")           >>= run h . k
            SRandMember a x :>>= k -> f h x (readBulk'Array "SRANDMEMBER" a) >>= run h . k
            SRem          x :>>= k -> f h x (readInt "SREM")                 >>= run h . k
            SScan         x :>>= k -> f h x (readScan "SSCAN")               >>= run h . k
            SUnion        x :>>= k -> f h x (readList "SUNION")              >>= run h . k
            SUnionStore   x :>>= k -> f h x (readInt "SUNIONSTORE")          >>= run h . k

            -- Sorted Sets
            ZAdd               x :>>= k -> f h x (readInt "ZADD")                     >>= run h . k
            ZCard              x :>>= k -> f h x (readInt "ZCARD")                    >>= run h . k
            ZCount             x :>>= k -> f h x (readInt "ZCOUNT")                   >>= run h . k
            ZIncrBy            x :>>= k -> f h x (readBulk "ZINCRBY")                 >>= run h . k
            ZInterStore        x :>>= k -> f h x (readInt "ZINTERSTORE")              >>= run h . k
            ZLexCount          x :>>= k -> f h x (readInt "ZLEXCOUNT")                >>= run h . k
            ZRange           a x :>>= k -> f h x (readScoreList "ZRANGE" a)           >>= run h . k
            ZRangeByLex        x :>>= k -> f h x (readList "ZRANGEBYLEX")             >>= run h . k
            ZRangeByScore    a x :>>= k -> f h x (readScoreList "ZRANGEBYSCORE" a)    >>= run h . k
            ZRank              x :>>= k -> f h x (readInt'Null  "ZRANK")              >>= run h . k
            ZRem               x :>>= k -> f h x (readInt "ZREM")                     >>= run h . k
            ZRemRangeByLex     x :>>= k -> f h x (readInt "ZREMRANGEBYLEX")           >>= run h . k
            ZRemRangeByRank    x :>>= k -> f h x (readInt "ZREMRANGEBYRANK")          >>= run h . k
            ZRemRangeByScore   x :>>= k -> f h x (readInt "ZREMRANGEBYSCORE")         >>= run h . k
            ZRevRange        a x :>>= k -> f h x (readScoreList "ZREVRANGE" a)        >>= run h . k
            ZRevRangeByScore a x :>>= k -> f h x (readScoreList "ZREVRANGEBYSCORE" a) >>= run h . k
            ZRevRank           x :>>= k -> f h x (readInt'Null  "ZREVRANK")           >>= run h . k
            ZScan              x :>>= k -> f h x (readScan "ZSCAN")                   >>= run h . k
            ZScore             x :>>= k -> f h x (readBulk'Null "ZSCORE")             >>= run h . k
            ZUnionStore        x :>>= k -> f h x (readInt "ZUNIONSTORE")              >>= run h . k

            -- Sort
            Sort x :>>= k -> f h x (readList "SORT") >>= run h . k

            -- HyperLogLog
            PfAdd   x :>>= k -> f h x (readBool "PFADD")        >>= run h . k
            PfCount x :>>= k -> f h x (readInt "PFCOUNT")       >>= run h . k
            PfMerge x :>>= k -> f h x (matchStr "PFMERGE" "OK") >>= run h . k

            -- Pub/Sub
            Publish x :>>= k -> getNow h x (readInt "PUBLISH") >>= run h . k

withConnection :: (Connection -> IO a) -> Client a
withConnection f = do
    p <- ask
    let c = connPool p
        s = settings p
    liftIO $ case sMaxWaitQueue s of
        Nothing -> withResource c $ \h -> f h `finally` C.sync h
        Just  q -> tryWithResource c (go p) >>= maybe (retry q c p) return
  where
    go p h = do
        atomicModifyIORef' (failures p) $ \n -> (if n > 0 then n - 1 else 0, ())
        f h `finally` C.sync h

    retry q c p = do
        k <- atomicModifyIORef' (failures p) $ \n -> (n + 1, n)
        unless (k < q) $
            throwIO ConnectionsBusy
        withResource c (go p)

getResult :: Connection -> Resp -> (Resp -> Result a) -> IO a
getResult h x g = do
    r <- newIORef (throw $ RedisError "missing response")
    C.request x r h
    unsafeInterleaveIO $ do
        C.sync h
        either throwIO return =<< g <$> readIORef r
{-# INLINE getResult #-}

-- Like 'getResult' but triggers immediate execution.
getNow :: Connection -> Resp -> (Resp -> Result a) -> IO a
getNow h x g = do
    r <- newIORef (throw $ RedisError "missing response")
    C.request x r h
    C.sync h
    either throwIO return =<< g <$> readIORef r
{-# INLINE getNow #-}

getDirect :: Connection -> Resp -> (Resp -> Result a) -> IO a
getDirect c r f = do
    C.send c [r]
    either throwIO return =<< f <$> C.receive c
{-# INLINE getDirect #-}

-- Update a 'Connection's send/recv timeout. Values > 0 get an additional
-- 10s grace period added to give redis enough time to finish first.
withTimeout :: Int64 -> Connection -> Connection
withTimeout 0 c = c { C.settings = setSendRecvTimeout 0                     (C.settings c) }
withTimeout t c = c { C.settings = setSendRecvTimeout (10 + fromIntegral t) (C.settings c) }
{-# INLINE withTimeout #-}
