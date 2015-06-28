-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE CPP                        #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}

module Database.Redis.IO.Client where

import Control.Applicative
import Control.Exception (throw, throwIO)
import Control.Monad.Base (MonadBase (..))
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Operational
import Control.Monad.Reader (ReaderT (..), runReaderT, MonadReader, ask, asks)
import Control.Monad.Trans.Class
import Control.Monad.Trans.Control (MonadBaseControl (..))
#if MIN_VERSION_transformers(0,4,0)
import Control.Monad.Trans.Except
#endif
import Data.ByteString.Lazy (ByteString)
import Data.Int
import Data.IORef
import Data.Redis
import Data.Pool hiding (Pool)
import Database.Redis.IO.Connection (Connection)
import Database.Redis.IO.Settings
import Database.Redis.IO.Timeouts (TimeoutManager)
import Database.Redis.IO.Types (ConnectionError (..))
import Prelude hiding (readList)
import System.Logger.Class hiding (Settings, settings, eval)
import System.IO.Unsafe (unsafeInterleaveIO)

import qualified Control.Monad.State.Strict   as S
import qualified Control.Monad.State.Lazy     as L
import qualified Data.List.NonEmpty           as NE
import qualified Data.Pool                    as P
import qualified Database.Redis.IO.Connection as C
import qualified System.Logger                as Logger
import qualified Database.Redis.IO.Timeouts   as TM

-- | Connection pool.
data Pool = Pool
    { settings :: !Settings
    , connPool :: !(P.Pool Connection)
    , logger   :: !Logger.Logger
    , timeouts :: !TimeoutManager
    }

-- | Redis client monad.
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
               , MonadBase IO
               )

instance MonadLogger Client where
    log l m = asks logger >>= \g -> Logger.log g l m

#if MIN_VERSION_monad_control(1,0,0)
instance MonadBaseControl IO Client where
    type StM Client a = StM (ReaderT Pool IO) a
    liftBaseWith f = Client . liftBaseWith $ \run -> f (run . client)
    restoreM = Client . restoreM
#else
instance MonadBaseControl IO Client where
    newtype StM Client a = ClientStM
        { unClientStM :: StM (ReaderT Pool IO) a }

    liftBaseWith f =
        Client . liftBaseWith $ \run -> f (fmap ClientStM . run . client)

    restoreM = Client . restoreM . unClientStM
#endif

-- | Monads in which 'Client' actions may be embedded.
class (Functor m, Applicative m, Monad m, MonadIO m, MonadCatch m) => MonadClient m
  where
    -- | Lift a computation to the 'Client' monad.
    liftClient :: Client a -> m a

instance MonadClient Client where
    liftClient = id

instance MonadClient m => MonadClient (ReaderT r m) where
    liftClient = lift . liftClient

instance MonadClient m => MonadClient (S.StateT s m) where
    liftClient = lift . liftClient

instance MonadClient m => MonadClient (L.StateT s m) where
    liftClient = lift . liftClient

#if MIN_VERSION_transformers(0,4,0)
instance MonadClient m => MonadClient (ExceptT e m) where
    liftClient = lift . liftClient
#endif

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
           <*> pure t
  where
    connOpen t aa = tryAll (NE.fromList aa) $ \a -> do
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

-- | Execute the given redis commands stepwise. I.e. every
-- command is send to the server and the response fetched and parsed before
-- the next command. A failing command which produces a 'RedisError' will
-- interrupt the command sequence and the error will be thrown as an
-- exception.
stepwise :: MonadClient m => Redis IO a -> m a
stepwise a = liftClient $ withConnection (flip (eval getEager) a)

-- | Execute the given redis commands pipelined. I.e. commands are send in
-- batches to the server and the responses are fetched and parsed after
-- a full batch has been sent. A failing command which produces
-- a 'RedisError' will /not/ prevent subsequent commands from being
-- executed by the redis server. However the first error will be thrown as
-- an exception.
pipelined :: MonadClient m => Redis IO a -> m a
pipelined a = liftClient $ withConnection (flip (eval getLazy) a)


-- | Execute the given redis commands in a Redis transaction.
transactional :: MonadClient m => Redis IO a -> m a
transactional a = liftClient $ withConnection (flip (eval getTransaction) a)

-- | Execute the given publish\/subscribe commands. The first parameter is
-- the callback function which will be invoked with channel and message
-- once messages arrive.
pubSub :: MonadClient m => (ByteString -> ByteString -> PubSub IO ()) -> PubSub IO () -> m ()
pubSub f a = liftClient $ withConnection (loop a)
  where
    loop :: PubSub IO () -> Connection -> IO ((), [IO ()])
    loop p h = do
        commands h p
        r <- responses h
        case r of
            Nothing -> return ((), [])
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

eval :: (forall a. Connection -> Resp -> (Resp -> Result a) -> IO (a, IO ()))
     -> Connection
     -> Redis IO b
     -> IO (b, [IO ()])
eval f conn red = run conn [] red
  where
    run :: Connection -> [IO ()] -> Redis IO a -> IO (a, [IO ()])
    run h ii c = do
        r <- viewT c
        case r of
            Return a -> return (a, ii)

            -- Connection
            Ping   x :>>= k -> f h x (matchStr "PING" "PONG") >>= \(a, i) -> run h (i:ii) $ k a
            Echo   x :>>= k -> f h x (readBulk "ECHO")        >>= \(a, i) -> run h (i:ii) $ k a
            Auth   x :>>= k -> f h x (matchStr "AUTH" "OK")   >>= \(a, i) -> run h (i:ii) $ k a
            Quit   x :>>= k -> f h x (matchStr "QUIT" "OK")   >>= \(a, i) -> run h (i:ii) $ k a
            Select x :>>= k -> f h x (matchStr "SELECT" "OK") >>= \(a, i) -> run h (i:ii) $ k a

            -- Server
            BgRewriteAOF x :>>= k -> f h x (anyStr "BGREWRITEAOF")    >>= \(a, i) -> run h (i:ii) $ k a
            BgSave       x :>>= k -> f h x (anyStr "BGSAVE")          >>= \(a, i) -> run h (i:ii) $ k a
            Save         x :>>= k -> f h x (matchStr "SAVE" "OK")     >>= \(a, i) -> run h (i:ii) $ k a
            FlushAll     x :>>= k -> f h x (matchStr "FLUSHALL" "OK") >>= \(a, i) -> run h (i:ii) $ k a
            FlushDb      x :>>= k -> f h x (matchStr "FLUSHDB" "OK")  >>= \(a, i) -> run h (i:ii) $ k a
            DbSize       x :>>= k -> f h x (readInt "DBSIZE")         >>= \(a, i) -> run h (i:ii) $ k a
            LastSave     x :>>= k -> f h x (readInt "LASTSAVE")       >>= \(a, i) -> run h (i:ii) $ k a

            -- Transactions
            Multi   x :>>= k -> f h x (matchStr "MULTI" "OK")   >>= \(a, i) -> run h (i:ii) $ k a
            Watch   x :>>= k -> f h x (matchStr "WATCH" "OK")   >>= \(a, i) -> run h (i:ii) $ k a
            Unwatch x :>>= k -> f h x (matchStr "UNWATCH" "OK") >>= \(a, i) -> run h (i:ii) $ k a
            Discard x :>>= k -> f h x (matchStr "DISCARD" "OK") >>= \(a, i) -> run h (i:ii) $ k a
            Exec    x :>>= k -> f h x (readList "EXEC")         >>= \(a, i) -> run h (i:ii) $ k a
            ExecRaw x :>>= k -> f h x return                    >>= \(a, i) -> run h (i:ii) $ k a

            -- Keys
            Del       x :>>= k -> f h x (readInt "DEL")             >>= \(a, i) -> run h (i:ii) $ k a
            Dump      x :>>= k -> f h x (readBulk'Null "DUMP")      >>= \(a, i) -> run h (i:ii) $ k a
            Exists    x :>>= k -> f h x (readBool "EXISTS")         >>= \(a, i) -> run h (i:ii) $ k a
            Expire    x :>>= k -> f h x (readBool "EXPIRE")         >>= \(a, i) -> run h (i:ii) $ k a
            ExpireAt  x :>>= k -> f h x (readBool "EXPIREAT")       >>= \(a, i) -> run h (i:ii) $ k a
            Persist   x :>>= k -> f h x (readBool "PERSIST")        >>= \(a, i) -> run h (i:ii) $ k a
            Keys      x :>>= k -> f h x (readList "KEYS")           >>= \(a, i) -> run h (i:ii) $ k a
            RandomKey x :>>= k -> f h x (readBulk'Null "RANDOMKEY") >>= \(a, i) -> run h (i:ii) $ k a
            Rename    x :>>= k -> f h x (matchStr "RENAME" "OK")    >>= \(a, i) -> run h (i:ii) $ k a
            RenameNx  x :>>= k -> f h x (readBool "RENAMENX")       >>= \(a, i) -> run h (i:ii) $ k a
            Ttl       x :>>= k -> f h x (readTTL "TTL")             >>= \(a, i) -> run h (i:ii) $ k a
            Type      x :>>= k -> f h x (readType "TYPE")           >>= \(a, i) -> run h (i:ii) $ k a
            Scan      x :>>= k -> f h x (readScan "SCAN")           >>= \(a, i) -> run h (i:ii) $ k a

            -- Strings
            Append   x :>>= k -> f h x (readInt "APPEND")        >>= \(a, i) -> run h (i:ii) $ k a
            Get      x :>>= k -> f h x (readBulk'Null "GET")     >>= \(a, i) -> run h (i:ii) $ k a
            GetRange x :>>= k -> f h x (readBulk "GETRANGE")     >>= \(a, i) -> run h (i:ii) $ k a
            GetSet   x :>>= k -> f h x (readBulk'Null "GETSET")  >>= \(a, i) -> run h (i:ii) $ k a
            MGet     x :>>= k -> f h x (readListOfMaybes "MGET") >>= \(a, i) -> run h (i:ii) $ k a
            MSet     x :>>= k -> f h x (matchStr "MSET" "OK")    >>= \(a, i) -> run h (i:ii) $ k a
            MSetNx   x :>>= k -> f h x (readBool "MSETNX")       >>= \(a, i) -> run h (i:ii) $ k a
            Set      x :>>= k -> f h x fromSet                   >>= \(a, i) -> run h (i:ii) $ k a
            SetRange x :>>= k -> f h x (readInt "SETRANGE")      >>= \(a, i) -> run h (i:ii) $ k a
            StrLen   x :>>= k -> f h x (readInt "STRLEN")        >>= \(a, i) -> run h (i:ii) $ k a

            -- Bits
            BitAnd   x :>>= k -> f h x (readInt "BITOP")    >>= \(a, i) -> run h (i:ii) $ k a
            BitCount x :>>= k -> f h x (readInt "BITCOUNT") >>= \(a, i) -> run h (i:ii) $ k a
            BitNot   x :>>= k -> f h x (readInt "BITOP")    >>= \(a, i) -> run h (i:ii) $ k a
            BitOr    x :>>= k -> f h x (readInt "BITOP")    >>= \(a, i) -> run h (i:ii) $ k a
            BitPos   x :>>= k -> f h x (readInt "BITPOS")   >>= \(a, i) -> run h (i:ii) $ k a
            BitXOr   x :>>= k -> f h x (readInt "BITOP")    >>= \(a, i) -> run h (i:ii) $ k a
            GetBit   x :>>= k -> f h x (readInt "GETBIT")   >>= \(a, i) -> run h (i:ii) $ k a
            SetBit   x :>>= k -> f h x (readInt "SETBIT")   >>= \(a, i) -> run h (i:ii) $ k a

            -- Numeric
            Decr        x :>>= k -> f h x (readInt "DECR")         >>= \(a, i) -> run h (i:ii) $ k a
            DecrBy      x :>>= k -> f h x (readInt "DECRBY")       >>= \(a, i) -> run h (i:ii) $ k a
            Incr        x :>>= k -> f h x (readInt "INCR")         >>= \(a, i) -> run h (i:ii) $ k a
            IncrBy      x :>>= k -> f h x (readInt "INCRBY")       >>= \(a, i) -> run h (i:ii) $ k a
            IncrByFloat x :>>= k -> f h x (readBulk "INCRBYFLOAT") >>= \(a, i) -> run h (i:ii) $ k a

            -- Hashes
            HDel         x :>>= k -> f h x (readInt "HDEL")           >>= \(a, i) -> run h (i:ii) $ k a
            HExists      x :>>= k -> f h x (readBool "HEXISTS")       >>= \(a, i) -> run h (i:ii) $ k a
            HGet         x :>>= k -> f h x (readBulk'Null "HGET")     >>= \(a, i) -> run h (i:ii) $ k a
            HGetAll      x :>>= k -> f h x (readFields "HGETALL")     >>= \(a, i) -> run h (i:ii) $ k a
            HIncrBy      x :>>= k -> f h x (readInt "HINCRBY")        >>= \(a, i) -> run h (i:ii) $ k a
            HIncrByFloat x :>>= k -> f h x (readBulk "HINCRBYFLOAT")  >>= \(a, i) -> run h (i:ii) $ k a
            HKeys        x :>>= k -> f h x (readList "HKEYS")         >>= \(a, i) -> run h (i:ii) $ k a
            HLen         x :>>= k -> f h x (readInt "HLEN")           >>= \(a, i) -> run h (i:ii) $ k a
            HMGet        x :>>= k -> f h x (readListOfMaybes "HMGET") >>= \(a, i) -> run h (i:ii) $ k a
            HMSet        x :>>= k -> f h x (matchStr "HMSET" "OK")    >>= \(a, i) -> run h (i:ii) $ k a
            HSet         x :>>= k -> f h x (readBool "HSET")          >>= \(a, i) -> run h (i:ii) $ k a
            HSetNx       x :>>= k -> f h x (readBool "HSETNX")        >>= \(a, i) -> run h (i:ii) $ k a
            HVals        x :>>= k -> f h x (readList "HVALS")         >>= \(a, i) -> run h (i:ii) $ k a
            HScan        x :>>= k -> f h x (readScan "HSCAN")         >>= \(a, i) -> run h (i:ii) $ k a

            -- Lists
            BLPop      t x :>>= k -> getNow (withTimeout t h) x (readKeyValue "BLPOP")       >>= \(a, i) -> run h (i:ii) $ k a
            BRPop      t x :>>= k -> getNow (withTimeout t h) x (readKeyValue "BRPOP")       >>= \(a, i) -> run h (i:ii) $ k a
            BRPopLPush t x :>>= k -> getNow (withTimeout t h) x (readBulk'Null "BRPOPLPUSH") >>= \(a, i) -> run h (i:ii) $ k a
            LIndex       x :>>= k -> f h x (readBulk'Null "LINDEX")    >>= \(a, i) -> run h (i:ii) $ k a
            LInsert      x :>>= k -> f h x (readInt "LINSERT")         >>= \(a, i) -> run h (i:ii) $ k a
            LLen         x :>>= k -> f h x (readInt "LLEN")            >>= \(a, i) -> run h (i:ii) $ k a
            LPop         x :>>= k -> f h x (readBulk'Null "LPOP")      >>= \(a, i) -> run h (i:ii) $ k a
            LPush        x :>>= k -> f h x (readInt "LPUSH")           >>= \(a, i) -> run h (i:ii) $ k a
            LPushX       x :>>= k -> f h x (readInt "LPUSHX")          >>= \(a, i) -> run h (i:ii) $ k a
            LRange       x :>>= k -> f h x (readList "LRANGE")         >>= \(a, i) -> run h (i:ii) $ k a
            LRem         x :>>= k -> f h x (readInt "LREM")            >>= \(a, i) -> run h (i:ii) $ k a
            LSet         x :>>= k -> f h x (matchStr "LSET" "OK")      >>= \(a, i) -> run h (i:ii) $ k a
            LTrim        x :>>= k -> f h x (matchStr "LTRIM" "OK")     >>= \(a, i) -> run h (i:ii) $ k a
            RPop         x :>>= k -> f h x (readBulk'Null "RPOP")      >>= \(a, i) -> run h (i:ii) $ k a
            RPopLPush    x :>>= k -> f h x (readBulk'Null "RPOPLPUSH") >>= \(a, i) -> run h (i:ii) $ k a
            RPush        x :>>= k -> f h x (readInt "RPUSH")           >>= \(a, i) -> run h (i:ii) $ k a
            RPushX       x :>>= k -> f h x (readInt "RPUSHX")          >>= \(a, i) -> run h (i:ii) $ k a

            -- Sets
            SAdd          x :>>= k -> f h x (readInt "SADD")                 >>= \(a, i) -> run h (i:ii) $ k a
            SCard         x :>>= k -> f h x (readInt "SCARD")                >>= \(a, i) -> run h (i:ii) $ k a
            SDiff         x :>>= k -> f h x (readList "SDIFF")               >>= \(a, i) -> run h (i:ii) $ k a
            SDiffStore    x :>>= k -> f h x (readInt "SDIFFSTORE")           >>= \(a, i) -> run h (i:ii) $ k a
            SInter        x :>>= k -> f h x (readList "SINTER")              >>= \(a, i) -> run h (i:ii) $ k a
            SInterStore   x :>>= k -> f h x (readInt "SINTERSTORE")          >>= \(a, i) -> run h (i:ii) $ k a
            SIsMember     x :>>= k -> f h x (readBool "SISMEMBER")           >>= \(a, i) -> run h (i:ii) $ k a
            SMembers      x :>>= k -> f h x (readList "SMEMBERS")            >>= \(a, i) -> run h (i:ii) $ k a
            SMove         x :>>= k -> f h x (readBool "SMOVE")               >>= \(a, i) -> run h (i:ii) $ k a
            SPop          x :>>= k -> f h x (readBulk'Null "SPOP")           >>= \(a, i) -> run h (i:ii) $ k a
            SRandMember y x :>>= k -> f h x (readBulk'Array "SRANDMEMBER" y) >>= \(a, i) -> run h (i:ii) $ k a
            SRem          x :>>= k -> f h x (readInt "SREM")                 >>= \(a, i) -> run h (i:ii) $ k a
            SScan         x :>>= k -> f h x (readScan "SSCAN")               >>= \(a, i) -> run h (i:ii) $ k a
            SUnion        x :>>= k -> f h x (readList "SUNION")              >>= \(a, i) -> run h (i:ii) $ k a
            SUnionStore   x :>>= k -> f h x (readInt "SUNIONSTORE")          >>= \(a, i) -> run h (i:ii) $ k a

            -- Sorted Sets
            ZAdd               x :>>= k -> f h x (readInt "ZADD")                     >>= \(a, i) -> run h (i:ii) $ k a
            ZCard              x :>>= k -> f h x (readInt "ZCARD")                    >>= \(a, i) -> run h (i:ii) $ k a
            ZCount             x :>>= k -> f h x (readInt "ZCOUNT")                   >>= \(a, i) -> run h (i:ii) $ k a
            ZIncrBy            x :>>= k -> f h x (readBulk "ZINCRBY")                 >>= \(a, i) -> run h (i:ii) $ k a
            ZInterStore        x :>>= k -> f h x (readInt "ZINTERSTORE")              >>= \(a, i) -> run h (i:ii) $ k a
            ZLexCount          x :>>= k -> f h x (readInt "ZLEXCOUNT")                >>= \(a, i) -> run h (i:ii) $ k a
            ZRange           y x :>>= k -> f h x (readScoreList "ZRANGE" y)           >>= \(a, i) -> run h (i:ii) $ k a
            ZRangeByLex        x :>>= k -> f h x (readList "ZRANGEBYLEX")             >>= \(a, i) -> run h (i:ii) $ k a
            ZRangeByScore    y x :>>= k -> f h x (readScoreList "ZRANGEBYSCORE" y)    >>= \(a, i) -> run h (i:ii) $ k a
            ZRank              x :>>= k -> f h x (readInt'Null  "ZRANK")              >>= \(a, i) -> run h (i:ii) $ k a
            ZRem               x :>>= k -> f h x (readInt "ZREM")                     >>= \(a, i) -> run h (i:ii) $ k a
            ZRemRangeByLex     x :>>= k -> f h x (readInt "ZREMRANGEBYLEX")           >>= \(a, i) -> run h (i:ii) $ k a
            ZRemRangeByRank    x :>>= k -> f h x (readInt "ZREMRANGEBYRANK")          >>= \(a, i) -> run h (i:ii) $ k a
            ZRemRangeByScore   x :>>= k -> f h x (readInt "ZREMRANGEBYSCORE")         >>= \(a, i) -> run h (i:ii) $ k a
            ZRevRange        y x :>>= k -> f h x (readScoreList "ZREVRANGE" y)        >>= \(a, i) -> run h (i:ii) $ k a
            ZRevRangeByScore y x :>>= k -> f h x (readScoreList "ZREVRANGEBYSCORE" y) >>= \(a, i) -> run h (i:ii) $ k a
            ZRevRank           x :>>= k -> f h x (readInt'Null  "ZREVRANK")           >>= \(a, i) -> run h (i:ii) $ k a
            ZScan              x :>>= k -> f h x (readScan "ZSCAN")                   >>= \(a, i) -> run h (i:ii) $ k a
            ZScore             x :>>= k -> f h x (readBulk'Null "ZSCORE")             >>= \(a, i) -> run h (i:ii) $ k a
            ZUnionStore        x :>>= k -> f h x (readInt "ZUNIONSTORE")              >>= \(a, i) -> run h (i:ii) $ k a

            -- Sort
            Sort x :>>= k -> f h x (readList "SORT") >>= \(a, i) -> run h (i:ii) $ k a

            -- HyperLogLog
            PfAdd   x :>>= k -> f h x (readBool "PFADD")        >>= \(a, i) -> run h (i:ii) $ k a
            PfCount x :>>= k -> f h x (readInt "PFCOUNT")       >>= \(a, i) -> run h (i:ii) $ k a
            PfMerge x :>>= k -> f h x (matchStr "PFMERGE" "OK") >>= \(a, i) -> run h (i:ii) $ k a

            -- Pub/Sub
            Publish x :>>= k -> getNow h x (readInt "PUBLISH") >>= \(a, i) -> run h (i:ii) $ k a

withConnection :: (Connection -> IO (a, [IO ()])) -> Client a
withConnection f = do
    p <- ask
    let c = connPool p
    x <- liftIO $ tryWithResource c $ \h -> f h >>= \(a, i) -> sequence_ i >> return a
    maybe (throwM ConnectionsBusy) return x

getLazy :: Connection -> Resp -> (Resp -> Result a) -> IO (a, IO ())
getLazy h x g = do
    r <- newIORef (throw $ RedisError "missing response")
    C.request x r h
    a <- unsafeInterleaveIO $ do
        C.sync h
        either throwIO return =<< g <$> readIORef r
    return (a, a `seq` return ())
{-# INLINE getLazy #-}

-- | Just like 'getLazy', but executes the 'Connection' buffer in a Redis
-- transaction.
getTransaction :: Connection -> Resp -> (Resp -> Result a) -> IO (a, IO ())
getTransaction h x g = do
    r <- newIORef (throw $ RedisError "missing response")
    C.request x r h
    a <- unsafeInterleaveIO $ do
        C.transaction h
        either throwIO return =<< g <$> readIORef r
    return (a, a `seq` return ())
{-# INLINE getTransaction #-}

getNow :: Connection -> Resp -> (Resp -> Result a) -> IO (a, IO ())
getNow h x g = do
    r <- newIORef (throw $ RedisError "missing response")
    C.request x r h
    C.sync h
    a <- either throwIO return =<< g <$> readIORef r
    return (a, return ())
{-# INLINE getNow #-}

-- 'getEager' bypasses the connection buffer and directly sends and
-- receives through the underlying socket.
getEager :: Connection -> Resp -> (Resp -> Result a) -> IO (a, IO ())
getEager c r f = do
    C.send c [r]
    a <- either throwIO return =<< f <$> C.receive c
    return (a, return ())
{-# INLINE getEager #-}

-- Update a 'Connection's send/recv timeout. Values > 0 get an additional
-- 10s grace period added to give redis enough time to finish first.
withTimeout :: Int64 -> Connection -> Connection
withTimeout 0 c = c { C.settings = setSendRecvTimeout 0                     (C.settings c) }
withTimeout t c = c { C.settings = setSendRecvTimeout (10 + fromIntegral t) (C.settings c) }
{-# INLINE withTimeout #-}

tryAll :: NonEmpty a -> (a -> IO b) -> IO b
tryAll (a :| []) f = f a
tryAll (a :| aa) f = f a `catchAll` (const $ tryAll (NE.fromList aa) f)
{-# INLINE tryAll #-}
