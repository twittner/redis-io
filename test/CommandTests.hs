-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module CommandTests (tests) where

import Control.Monad (void)
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Data.Monoid
import Test.Tasty
import Test.Tasty.HUnit
import Data.Redis
import Network.Redis.IO

import qualified Data.Set as Set

tests :: Pool -> TestTree
tests p = testGroup "commands"
    [ testGroup "server"
        [ testCase "save"         $ save         $$ (== ())
        , testCase "flushdb"      $ flushdb      $$ (== ())
        , testCase "flushall"     $ flushall     $$ (== ())
        , testCase "bgsave"       $ bgsave       $$ (== ())
        , testCase "bgrewriteaof" $ bgrewriteaof $$ (== ())
        , testCase "dbsize"       $ dbsize       $$ (>= 0)
        , testCase "lastsave"     $ lastsave     $$ (>  1408021976)
        ]
    , testGroup "connection"
        [ testCase "ping"   $ ping         $$ (== ())
        , testCase "echo"   $ echo "True"  $$ (== True)
        , testCase "select" $ select 0     $$ (== ())
        ]
    , testGroup "keys"
        [ testCase "randomkey1" $
            randomkey $$ (== Nothing)
        , testCase "randomkey2" $
            withFoo randomkey (== Just "foo")
        , testCase "exists1" $
            exists "foo" $$ (== False)
        , testCase "exists2" $
            withFoo (exists "foo") (== True)
        , testCase "expire" $
            withFoo (expire "foo" (Seconds 60)) (== True)
        , testCase "expireAt" $
            withFoo (expireat "foo" (Timestamp 9408023026)) (== True)
        , testCase "persist1" $
            withFoo (persist "foo") (== False)
        , testCase "persist2" $
            withFoo (expire "foo" (Seconds 60) >> persist "foo") (== True)
        , testCase "keys" $
            withFoo (keys "foo") (== ["foo"])
        , testCase "rename" $
            withFoo (rename "foo" "bar") (== ())
        , testCase "renamenx" $
            withFoo (renamenx "foo" "baz") (== True)
        , testCase "ttl" $
            withFoo (expire "foo" (Seconds 60) >> ttl "foo") (<= Just (TTL 60))
        , testCase "type" $
            withFoo (typeof "foo") (== Just RedisString)
        , testCase "scan" $
            withFoo (scan zero (match "foo" <> count 10))
                    (\(c, k) -> c == zero && k == ["foo" :: ByteString])
        ]
    , testGroup "strings"
        [ testCase "append" $
            with [("foo", "xx")] (append "foo" "y")(== 3)
        , testCase "get" $
            with [("foo", "42")] (get "foo") (== Just (42 :: Int))
        , testCase "getrange" $
            with [("foo", "42")] (getrange "foo" 0 0) (== (4 :: Int))
        , testCase "getset" $
            with [("foo", "42")] (getset "foo" "0") (== Just (42 ::Int))
        , testCase "mget" $
            with [("foo", "42"), ("bar", "43")]
                 (mget ("foo" :| ["bar", "xxx"]))
                 (== ([Just 42, Just 43, Nothing] :: [Maybe Int]))
        , testCase "msetnx" $
             msetnx (("aaa", "4343") :| [("bcbx", "shsh")]) $$ (== True)
        , testCase "set" $ set "aa" "bb" none $$ (== True)
        , testCase "setrange" $ setrange "aa" 1 "cc" $$ (== 3)
        , testCase "strlen" $ strlen "aa" $$ (== 3)
        ]
    , testGroup "bits"
        [ testCase "bitand" $ do
            with [("n1", "0"), ("n2", "1")] (bitand "r" ("n1" :| ["n2"])) (== 1)
            get "r" $$ (== Just (0 :: Int))
        , testCase "bitor" $ do
            with [("n1", "0"), ("n2", "1")] (bitor "r" ("n1" :| ["n2"])) (== 1)
            get "r" $$ (== Just (1 :: Int))
        , testCase "bitxor" $ do
            with [("n1", "0"), ("n2", "1")] (bitor "r" ("n1" :| ["n2"])) (== 1)
            get "r" $$ (== Just (1 :: Int))
        , testCase "bitnot" $ do
            with [("n1", "0")] (bitnot "r" "n1") (== 1)
            get "r" $$ (== Just ("\xcf" :: ByteString))
        , testCase "bitcount" $ with [("n1", "1")] (bitcount "n1" (range 0 0)) (== 3)
        , testCase "getbit" $ with [("n1", "1")] (getbit "n1" 0)      (== 0)
        , testCase "setbit" $ with [("n1", "1")] (setbit "n1" 0 True) (== 0)
        , testCase "bitpos" $ with [("n1", "123")] (bitpos "n1" True (start 0) (end 10)) (== 2)
        ]
    , testGroup "numeric"
        [ testCase "decr" $ with [("x", "100")] (decr "x") (== 99)
        , testCase "decrby" $ with [("x", "100")] (decrby "x" 50) (== 50)
        , testCase "incr" $ with [("x", "99")] (incr "x") (== 100)
        , testCase "incrby" $ with [("x", "30")] (incrby "x" 20) (== 50)
        , testCase "incrbyfloat" $ with [("x", "2")] (incrbyfloat "x" 0.5) (== 2.5)
        ]
    , testGroup "hashes"
        [ testCase "hset" $ hset "h" "k" "42" $$ (== True)
        , testCase "hget" $
            bracket (hset "h" "k" "4") (del (one "h")) (hget "h" "k") (== Just (4 :: Int))
        , testCase "hexists" $ do
            hexists "h" "x" $$ (== False)
            bracket (hset "h" "k" "4") (del (one "h")) (hexists "h" "k") (== True)
            bracket (hset "h" "k" "4") (del (one "h")) (hexists "h" "j") (== False)
        , testCase "hgetall" $ do
            bracket (hmset "h" (("k", "4") :| [("j", "5")])) (del (one "h"))
                    (hgetall "h")
                    (== ([("k", 4), ("j", 5)] :: [(ByteString, Int)]))
        , testCase "hmget" $ do
            bracket (hmset "h" (("k", "4") :| [("j", "5")])) (del (one "h"))
                    (hmget "h" ("k" :| ["j"]))
                    (== ([Just 4, Just 5] :: [Maybe Int]))
        , testCase "hsetnx" $ do
            hsetnx "h" "k" "42" $$ (== True)
            bracket (hset "h" "k" "4") (del (one "h")) (hsetnx "h" "k" "42") (== False)
        , testCase "hdel" $
            bracket (hset "h" "k" "4") (del (one "h")) (hdel "h" (one "k")) (== 1)
        , testCase "hincrby" $
            bracket (hset "h" "k" "4") (del (one "h")) (hincrby "h" "k" 10) (== 14)
        , testCase "hincrbyfloat" $
            bracket (hset "h" "k" "4") (del (one "h")) (hincrbyfloat "h" "k" 0.5) (== 4.5)
        , testCase "hkeys" $ do
            bracket (hmset "h" (("k", "4") :| [("j", "5")])) (del (one "h"))
                    (hkeys "h")
                    (== ["k", "j"])
        , testCase "hvals" $ do
            bracket (hmset "h" (("k", "4") :| [("j", "5")])) (del (one "h"))
                    (hvals "h")
                    (== ([4, 5] :: [Int]))
        ]
    , testGroup "lists"
        [ testCase "lpush" $ lpush "l" ("0" :| ["1", "2", "3"]) $$ (== 4)
        , testCase "lpop" $
            bracket (lpush "l" ("1" :| ["2", "3"])) (del (one "l")) (lpop "l") (== Just (3 :: Int))
        , testCase "rpop" $
            bracket (rpush "l" ("1" :| ["2", "3"])) (del (one "l")) (rpop "l") (== Just (3 :: Int))
        , testCase "rpoplpush" $
            bracket (rpush "l" ("1" :| ["2", "3"])) (del (one "l")) (rpoplpush "l" "l") (== Just (3 :: Int))
        , testCase "lpushx" $
            bracket (lpush "l" ("1" :| ["2", "3"])) (del (one "l"))
                    (lpushx "l" "5")
                    (== 4)
        , testCase "rpushx" $
            bracket (rpush "l" ("1" :| ["2", "3"])) (del (one "l"))
                    (rpushx "l" "5")
                    (== 4)
        , testCase "lindex" $
            bracket (lpush "l" ("1" :| ["2", "3"])) (del (one "l"))
                    (lindex "l" 0)
                    (== Just (3 :: Int))
        , testCase "linsert" $
            bracket (lpush "l" ("1" :| ["2", "3"])) (del (one "l"))
                    (linsert "l" Before "2" "0")
                    (== 4)
        , testCase "llen" $
            bracket (lpush "l" ("1" :| ["2", "3"])) (del (one "l"))
                    (llen "l")
                    (== 3)
        , testCase "lrange" $
            bracket (lpush "l" ("1" :| ["2", "3"])) (del (one "l"))
                    (lrange "l" 1 2)
                    (== ([2, 1] :: [Int]))
        , testCase "lrem" $ do
            bracket (lpush "l" ("1" :| ["2", "1"])) (del (one "l"))
                    (lrem "l" 1 "1")
                    (== 1)
            bracket (lpush "l" ("1" :| ["2", "1"])) (del (one "l"))
                    (lrem "l" (-1) "1")
                    (== 1)
            bracket (lpush "l" ("1" :| ["2", "1"])) (del (one "l"))
                    (lrem "l" 0 "1")
                    (== 2)
        , testCase "lset" $
            bracket (lpush "l" ("1" :| ["2", "1"])) (del (one "l"))
                    (lset "l" 1 "1" >> lrange "l" 0 3)
                    (== ([1, 1, 1] :: [Int]))
        , testCase "ltrim" $
            bracket (lpush "l" ("1" :| ["2", "1"])) (del (one "l"))
                    (ltrim "l" 0 1 >> lrange "l" 0 3)
                    (== ([1, 2] :: [Int]))
        ]
    , testGroup "sets"
        [ testCase "sadd" $ sadd "a" (one "0") $$ (== 1)
        , testCase "scard" $
            bracket (sadd "s" ("1" :| ["2", "1"])) (del (one "s"))
                    (scard "s")
                    (== 2)
        , testCase "spop" $
            bracket (sadd "s" (one "1")) (del (one "s"))
                    (spop "s")
                    (== Just (1 :: Int))
        , testCase "srandmember" $
            bracket (sadd "s" (one "1")) (del (one "s"))
                    (srandmember "s" One)
                    (== [1 :: Int])
        , testCase "srem" $
            bracket (sadd "s" (one "1")) (del (one "s"))
                    (srem "s" (one "1"))
                    (== 1)
        , testCase "sismember" $
            bracket (sadd "s" ("1" :| ["2", "1"])) (del (one "s"))
                    (sismember "s" "2")
                    (== True)
        , testCase "smembers" $
            bracket (sadd "s" ("1" :| ["2", "1"])) (del (one "s"))
                    (smembers "s")
                    ((== Set.fromList ([1, 2] :: [Int])) . Set.fromList)
        , testCase "sdiff" $
            bracket (sadd "x" ("1" :| ["2"]) >> sadd "y" (one "1")) (del ("x" :| ["y"]))
                    (sdiff ("x" :| ["y"]))
                    (== [2 :: Int])
        , testCase "sdiffstore" $
            bracket (sadd "x" ("1" :| ["2"]) >> sadd "y" (one "1")) (del ("x" :| ["y"]))
                    (sdiffstore "z" ("x" :| ["y"]) >> smembers "z")
                    (== [2 :: Int])
        , testCase "sinter" $
            bracket (sadd "x" ("1" :| ["2"]) >> sadd "y" (one "1")) (del ("x" :| ["y"]))
                    (sinter ("x" :| ["y"]))
                    (== [1 :: Int])
        , testCase "sinterstore" $
            bracket (sadd "x" ("1" :| ["2"]) >> sadd "y" (one "1")) (del ("x" :| ["y"]))
                    (sinterstore "z" ("x" :| ["y"]) >> smembers "z")
                    (== [1 :: Int])
        , testCase "sunion" $
            bracket (sadd "x" ("1" :| ["2"]) >> sadd "y" (one "1")) (del ("x" :| ["y"]))
                    (sunion ("x" :| ["y"]))
                    ((== Set.fromList ([1, 2] :: [Int])) . Set.fromList)
        , testCase "sunionstore" $
            bracket (sadd "x" ("1" :| ["2"]) >> sadd "y" (one "1")) (del ("x" :| ["y"]))
                    (sunionstore "z" ("x" :| ["y"]) >> smembers "z")
                    ((== Set.fromList ([1, 2] :: [Int])) . Set.fromList)
        ]
    , testGroup "sorted sets"
        [ testCase "zadd" $ zadd "w" (one (1.0, "0")) $$ (== 1)
        , testCase "zcard" $
            bracket (zadd "v" ((1, "1") :| [(2, "2"), (3, "1")])) (del (one "v"))
                    (zcard "v")
                    (== 2)
        , testCase "zcount" $
            bracket (zadd "v" ((1, "1") :| [(2, "2"), (3, "3")])) (del (one "v"))
                    (zcount "v" 1 2)
                    (== 2)
        , testCase "zincrby" $
            bracket (zadd "v" ((1, "1") :| [(2, "2"), (3, "3")])) (del (one "v"))
                    (zincrby "v" 0.5 "2")
                    (== (2.5 :: Double))
        , testCase "zinterstore" $
            bracket (zadd "v" ((1, "1") :| [(2, "2"), (3, "3")])) (del (one "v"))
                    (zinterstore "z" (one "v") [3] Max)
                    (== 3)
        , testCase "zunionstore" $
            bracket (zadd "v" ((1, "1") :| [(2, "2"), (3, "3")])) (del (one "v"))
                    (zunionstore "z" (one "v") [3] Max)
                    (== 3)
        , testCase "zlexcount" $
            bracket (zadd "v" ((1, "a") :| [(1, "b"), (1, "c")])) (del (one "v"))
                    (zlexcount "v" (MinIncl "b") (MaxExcl "c"))
                    (== 1)
        , testCase "zrange" $
            bracket (zadd "v" ((1, "a") :| [(10, "b"), (20, "c")])) (del (one "v"))
                    (zrange "v" 0 1 True)
                    (== (ScoreList [1, 10] ["a" :: ByteString, "b"]))
        , testCase "zrangebylex" $
            bracket (zadd "v" ((1, "a") :| [(10, "b"), (20, "c")])) (del (one "v"))
                    (zrangebylex "v" (MinIncl "a") (MaxExcl "c") (limit 0 2))
                    (== (["a", "b"] :: [ByteString]))
        , testCase "zrevrange" $
            bracket (zadd "v" ((1, "a") :| [(10, "b"), (20, "c")])) (del (one "v"))
                    (zrevrange "v" 0 1 True)
                    (== (ScoreList [20, 10] ["c" :: ByteString, "b"]))
        , testCase "zrangebyscore" $
            bracket (zadd "v" ((1, "a") :| [(10, "b"), (20, "c")])) (del (one "v"))
                    (zrangebyscore "v" 1 10 True (limit 0 10))
                    (== (ScoreList [1, 10] ["a" :: ByteString, "b"]))
        , testCase "zrevrangebyscore" $
            bracket (zadd "v" ((1, "a") :| [(10, "b"), (20, "c")])) (del (one "v"))
                    (zrevrangebyscore "v" 10 1 True (limit 0 10))
                    (== (ScoreList [10, 1] ["b" :: ByteString, "a"]))
        , testCase "zrank" $
            bracket (zadd "v" ((1, "a") :| [(10, "b"), (20, "c")])) (del (one "v"))
                    (zrank "v" "b")
                    (== Just 1)
        , testCase "zrevrank" $
            bracket (zadd "v" ((1, "a") :| [(10, "b"), (20, "c")])) (del (one "v"))
                    (zrevrank "v" "b")
                    (== Just 1)
        , testCase "zrem" $
            bracket (zadd "v" ((1, "a") :| [(10, "b"), (20, "c")])) (del (one "v"))
                    (zrem "v" (one "b"))
                    (== 1)
        , testCase "zscore" $
            bracket (zadd "v" ((1, "a") :| [(10, "b"), (20, "c")])) (del (one "v"))
                    (zscore "v" "b")
                    (== Just (10 :: Double))
        ]
    , testGroup "sort"
        [ testCase "sort" $
            bracket (lpush "l" ("5" :| ["2", "3", "1", "7"])) (del (one "l"))
                    (sort "l" (limit 0 10 <> asc))
                    (== ([1, 2, 3, 5, 7] :: [Int]))
        ]
    , testGroup "hyperloglog"
        [ testCase "pfcount" $
            bracket (pfadd "p" ("5" :| ["2", "3", "1", "7"])) (del (one "p"))
                    (pfcount (one "p"))
                    (== 5)
        , testCase "pfmerge" $
            bracket (pfadd "p" (one "5") >> pfadd "q" (one "6")) (del ("p" :| ["q"]))
                    (pfmerge "t" ("p" :| ["q"]) >> pfcount (one "t"))
                    (== 2)
        ]
    , testGroup "pub/sub"
        [ testCase "pub/sub" (pubSubTest p) ]
    ]
  where
    ($$) :: (Eq a, Show a) => Redis Lazy IO (Lazy (Result a)) -> (a -> Bool) -> Assertion
    r $$ f = do
        x <- runRedis p (ensure =<< r)
        assertBool (show x) (f x)

    bracket :: Show c
            => Redis Lazy IO (Lazy (Result a))
            -> Redis Lazy IO (Lazy (Result b))
            -> Redis Lazy IO (Lazy (Result c))
            -> (c -> Bool)
            -> Assertion
    bracket a r f t = runRedis p $ do
        void $ a
        x <- ensure =<< f
        void $ r
        liftIO $ assertBool (show x) (t x)

    with :: Show a => [(Key, ByteString)] -> Redis Lazy IO (Lazy (Result a)) -> (a -> Bool) -> Assertion
    with kv r f = bracket (mset (head kv :| tail kv)) (del (fst (head kv) :| map fst (tail kv))) r f

    withFoo :: Show a => Redis Lazy IO (Lazy (Result a)) -> (a -> Bool) -> Assertion
    withFoo = with [("foo", "42")]

pubSubTest :: Pool -> IO ()
pubSubTest p = do
    a <- async $ runRedis p $ do
        liftIO $ threadDelay 1000000
        void $ publish "a" "hello"
        void $ publish "b" "world"
        void $ publish "z.1" "foo"
        void $ publish "a" "add"
        void $ publish "a" "quit"
    runClient p $ pubSub k $ do
        subscribe  (one "a")
        subscribe  (one "b")
        psubscribe (one "z.*")
    wait a
  where
    k ch ms = do
        liftIO $ print $ "message: " <> ch <> ": " <> ms
        case ms of
            "quit" -> unsubscribe [] >> punsubscribe []
            "add"  -> subscribe (one "x")
            _      -> return ()
