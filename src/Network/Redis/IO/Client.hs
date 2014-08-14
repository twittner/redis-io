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
        Return a -> return a

        -- Connection
        Ping   x :>>= k -> getResult h x (matchStr "PING" "PONG") >>= run h . k
        Echo   x :>>= k -> getResult h x (readBulk "ECHO")        >>= run h . k
        Auth   x :>>= k -> getResult h x (matchStr "AUTH" "OK")   >>= run h . k
        Quit   x :>>= k -> getResult h x (matchStr "QUIT" "OK")   >>= run h . k
        Select x :>>= k -> getResult h x (matchStr "SELECT" "OK") >>= run h . k

        -- Server
        BgRewriteAOF x :>>= k -> getResult h x (matchStr "BGREWRITEAOF" "OK") >>= run h . k
        BgSave       x :>>= k -> getResult h x (matchStr "BGSAVE" "OK")       >>= run h . k
        Save         x :>>= k -> getResult h x (matchStr "SAVE" "OK")         >>= run h . k
        FlushAll     x :>>= k -> getResult h x (matchStr "FLUSHALL" "OK")     >>= run h . k
        FlushDb      x :>>= k -> getResult h x (matchStr "FLUSHDB" "OK")      >>= run h . k
        DbSize       x :>>= k -> getResult h x (readInt "DBSIZE")             >>= run h . k
        LastSave     x :>>= k -> getResult h x (readInt "LASTSAVE")           >>= run h . k

        -- Transactions
        Multi   x :>>= k -> getResult h x (matchStr "MULTI" "OK")   >>= run h . k
        Watch   x :>>= k -> getResult h x (matchStr "WATCH" "OK")   >>= run h . k
        Unwatch x :>>= k -> getResult h x (matchStr "UNWATCH" "OK") >>= run h . k
        Discard x :>>= k -> getResult h x (matchStr "DISCARD" "OK") >>= run h . k
        Exec    x :>>= k -> getResult h x (readList "EXEC")         >>= run h . k
        ExecRaw x :>>= k -> getResult h x return                    >>= run h . k

        -- Keys
        Del       x :>>= k -> getResult h x (readInt "DEL")             >>= run h . k
        Dump      x :>>= k -> getResult h x (readBulk'Null "DUMP")      >>= run h . k
        Exists    x :>>= k -> getResult h x (readBool "EXISTS")         >>= run h . k
        Expire    x :>>= k -> getResult h x (readBool "EXPIRE")         >>= run h . k
        ExpireAt  x :>>= k -> getResult h x (readBool "EXPIREAT")       >>= run h . k
        Persist   x :>>= k -> getResult h x (readBool "PERSIST")        >>= run h . k
        Keys      x :>>= k -> getResult h x (readList "KEYS")           >>= run h . k
        RandomKey x :>>= k -> getResult h x (readBulk'Null "RANDOMKEY") >>= run h . k
        Rename    x :>>= k -> getResult h x (matchStr "RENAME" "OK")    >>= run h . k
        RenameNx  x :>>= k -> getResult h x (readBool "RENAMENX")       >>= run h . k
        Sort      x :>>= k -> getResult h x (readList "SORT")           >>= run h . k
        Ttl       x :>>= k -> getResult h x (readTTL "TTL")             >>= run h . k
        Type      x :>>= k -> getResult h x (readType "TYPE")           >>= run h . k
        Scan      x :>>= k -> getResult h x (readScan "SCAN")           >>= run h . k

        -- Strings
        Append   x :>>= k -> getResult h x (readInt "APPEND")          >>= run h . k
        Get      x :>>= k -> getResult h x (readBulk'Null "GET")       >>= run h . k
        GetRange x :>>= k -> getResult h x (readBulk "GETRANGE")       >>= run h . k
        GetSet   x :>>= k -> getResult h x (readBulk'Null "GETSET")    >>= run h . k
        MGet     x :>>= k -> getResult h x (readListOfMaybes "MGET")   >>= run h . k
        MSet     x :>>= k -> getResult h x (matchStr "MSET" "OK")      >>= run h . k
        MSetNx   x :>>= k -> getResult h x (readBool "MSETNX")         >>= run h . k
        Set      x :>>= k -> getResult h x fromSet                     >>= run h . k
        SetRange x :>>= k -> getResult h x (readInt "SETRANGE")        >>= run h . k
        StrLen   x :>>= k -> getResult h x (readInt "STRLEN")          >>= run h . k

        -- Bits
        BitAnd   x :>>= k -> getResult h x (readInt "BITOP")    >>= run h . k
        BitCount x :>>= k -> getResult h x (readInt "BITCOUNT") >>= run h . k
        BitNot   x :>>= k -> getResult h x (readInt "BITOP")    >>= run h . k
        BitOr    x :>>= k -> getResult h x (readInt "BITOP")    >>= run h . k
        BitPos   x :>>= k -> getResult h x (readInt "BITPOS")   >>= run h . k
        BitXOr   x :>>= k -> getResult h x (readInt "BITOP")    >>= run h . k
        GetBit   x :>>= k -> getResult h x (readInt "GETBIT")   >>= run h . k
        SetBit   x :>>= k -> getResult h x (readInt "SETBIT")   >>= run h . k

        -- Numeric
        Decr        x :>>= k -> getResult h x (readInt "DECR")        >>= run h . k
        DecrBy      x :>>= k -> getResult h x (readInt "DECRBY")      >>= run h . k
        Incr        x :>>= k -> getResult h x (readInt "INCR")        >>= run h . k
        IncrBy      x :>>= k -> getResult h x (readInt "INCRBY")      >>= run h . k
        IncrByFloat x :>>= k -> getResult h x (readDbl "INCRBYFLOAT") >>= run h . k

        -- Hashes
        HDel         x :>>= k -> getResult h x (readInt "HDEL")           >>= run h . k
        HExists      x :>>= k -> getResult h x (readBool "HEXISTS")       >>= run h . k
        HGet         x :>>= k -> getResult h x (readBulk'Null "HGET")     >>= run h . k
        HGetAll      x :>>= k -> getResult h x (readFields "HGETALL")     >>= run h . k
        HIncrBy      x :>>= k -> getResult h x (readInt "HINCRBY")        >>= run h . k
        HIncrByFloat x :>>= k -> getResult h x (readDbl "HINCRBYFLOAT")   >>= run h . k
        HKeys        x :>>= k -> getResult h x (readList "HKEYS")         >>= run h . k
        HLen         x :>>= k -> getResult h x (readInt "HLEN")           >>= run h . k
        HMGet        x :>>= k -> getResult h x (readListOfMaybes "HMGET") >>= run h . k
        HMSet        x :>>= k -> getResult h x (matchStr "HMSET" "OK")    >>= run h . k
        HSet         x :>>= k -> getResult h x (readBool "HSET")          >>= run h . k
        HSetNx       x :>>= k -> getResult h x (readBool "HSETNX")        >>= run h . k
        HVals        x :>>= k -> getResult h x (readList "HVALS")         >>= run h . k
        HScan        x :>>= k -> getResult h x (readScan "HSCAN")         >>= run h . k

        -- Lists
        BLPop      x :>>= k -> getResult h x (readKeyValue "BLPOP")       >>= run h . k
        BRPop      x :>>= k -> getResult h x (readKeyValue "BRPOP")       >>= run h . k
        BRPopLPush x :>>= k -> getResult h x (readBulk'Null "BRPOPLPUSH") >>= run h . k
        LIndex     x :>>= k -> getResult h x (readBulk'Null "LINDEX")     >>= run h . k
        LInsert    x :>>= k -> getResult h x (readInt "LINSERT")          >>= run h . k
        LLen       x :>>= k -> getResult h x (readInt "LLEN")             >>= run h . k
        LPop       x :>>= k -> getResult h x (readBulk'Null "LPOP")       >>= run h . k
        LPush      x :>>= k -> getResult h x (readInt "LPUSH")            >>= run h . k
        LPushNx    x :>>= k -> getResult h x (readInt "LPUSHNX")          >>= run h . k
        LRange     x :>>= k -> getResult h x (readList "LRANGE")          >>= run h . k
        LRem       x :>>= k -> getResult h x (readInt "LREM")             >>= run h . k
        LSet       x :>>= k -> getResult h x (matchStr "LSET" "OK")       >>= run h . k
        LTrim      x :>>= k -> getResult h x (matchStr "LTRIM" "OK")      >>= run h . k
        RPop       x :>>= k -> getResult h x (readBulk'Null "RPOP")       >>= run h . k
        RPopLPush  x :>>= k -> getResult h x (readBulk'Null "RPOPLPUSH")  >>= run h . k
        RPush      x :>>= k -> getResult h x (readInt "RPUSH")            >>= run h . k
        RPushNx    x :>>= k -> getResult h x (readInt "RPUSHNX")          >>= run h . k

        -- Sets
        SAdd          x :>>= k -> getResult h x (readInt "SADD")                 >>= run h . k
        SCard         x :>>= k -> getResult h x (readInt "SCARD")                >>= run h . k
        SDiff         x :>>= k -> getResult h x (readList "SDIFF")               >>= run h . k
        SDiffStore    x :>>= k -> getResult h x (readInt "SDIFFSTORE")           >>= run h . k
        SInter        x :>>= k -> getResult h x (readList "SINTER")              >>= run h . k
        SInterStore   x :>>= k -> getResult h x (readInt "SINTERSTORE")          >>= run h . k
        SIsMember     x :>>= k -> getResult h x (readBool "SISMEMBER")           >>= run h . k
        SMembers      x :>>= k -> getResult h x (readList "SMEMBERS")            >>= run h . k
        SMove         x :>>= k -> getResult h x (readBool "SMOVE")               >>= run h . k
        SPop          x :>>= k -> getResult h x (readBulk'Null "SPOP")           >>= run h . k
        SRandMember a x :>>= k -> getResult h x (readBulk'Array "SRANDMEMBER" a) >>= run h . k
        SRem          x :>>= k -> getResult h x (readInt "SREM")                 >>= run h . k
        SScan         x :>>= k -> getResult h x (readScan "SSCAN")               >>= run h . k
        SUnion        x :>>= k -> getResult h x (readList "SUNION")              >>= run h . k
        SUnionStore   x :>>= k -> getResult h x (readInt "SUNIONSTORE")          >>= run h . k

        -- Sorted Sets
        ZAdd               x :>>= k -> getResult h x (readInt "ZADD")                     >>= run h . k
        ZCard              x :>>= k -> getResult h x (readInt "ZCARD")                    >>= run h . k
        ZCount             x :>>= k -> getResult h x (readInt "ZCOUNT")                   >>= run h . k
        ZIncrBy            x :>>= k -> getResult h x (readDbl "ZINCRBY")                  >>= run h . k
        ZInterStore        x :>>= k -> getResult h x (readInt "ZINTERSTORE")              >>= run h . k
        ZLexCount          x :>>= k -> getResult h x (readInt "ZLEXCOUNT")                >>= run h . k
        ZRange           a x :>>= k -> getResult h x (readScoreList "ZRANGE" a)           >>= run h . k
        ZRangeByLex        x :>>= k -> getResult h x (readList "ZRANGEBYLEX")             >>= run h . k
        ZRangeByScore    a x :>>= k -> getResult h x (readScoreList "ZRANGEBYSCORE" a)    >>= run h . k
        ZRank              x :>>= k -> getResult h x (readInt'Null  "ZRANK")              >>= run h . k
        ZRem               x :>>= k -> getResult h x (readInt "ZREM")                     >>= run h . k
        ZRemRangeByLex     x :>>= k -> getResult h x (readInt "ZREMRANGEBYLEX")           >>= run h . k
        ZRemRangeByRank    x :>>= k -> getResult h x (readInt "ZREMRANGEBYRANK")          >>= run h . k
        ZRemRangeByScore   x :>>= k -> getResult h x (readInt "ZREMRANGEBYSCORE")         >>= run h . k
        ZRevRange        a x :>>= k -> getResult h x (readScoreList "ZREVRANGE" a)        >>= run h . k
        ZRevRangeByScore a x :>>= k -> getResult h x (readScoreList "ZREVRANGEBYSCORE" a) >>= run h . k
        ZRevRank           x :>>= k -> getResult h x (readInt'Null  "ZREVRANK")           >>= run h . k
        ZScan              x :>>= k -> getResult h x (readScan "ZSCAN")                   >>= run h . k
        ZScore             x :>>= k -> getResult h x (readDbl  "ZSCORE")                  >>= run h . k
        ZUnionStore        x :>>= k -> getResult h x (readInt "ZUNIONSTORE")              >>= run h . k

        -- HyperLogLog
        PfAdd   x :>>= k -> getResult h x (readBool "PFADD")        >>= run h . k
        PfCount x :>>= k -> getResult h x (readInt "PFCOUNT")       >>= run h . k
        PfMerge x :>>= k -> getResult h x (matchStr "PFMERGE" "OK") >>= run h . k

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
