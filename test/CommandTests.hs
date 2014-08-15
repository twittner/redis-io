-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module CommandTests (tests) where

import Control.Monad (void)
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import Data.Monoid
import Test.Tasty
import Test.Tasty.HUnit
import Data.Redis
import Network.Redis.IO

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
        ]
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
