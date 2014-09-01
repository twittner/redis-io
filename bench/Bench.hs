-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE ExtendedDefaultRules       #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}

module Main (main) where

import Control.Applicative
import Control.Monad
import Criterion
import Criterion.Main
import Data.ByteString.Lazy
import Data.Monoid
import Database.Redis.IO

import qualified Database.Redis as Hedis
import qualified System.Logger  as Logger

default (ByteString, Int)

main :: IO ()
main = do
    g <- Logger.new (Logger.setLogLevel Logger.Error Logger.defSettings)
    p <- mkPool g (setMaxConnections 50 . setPoolStripes 1 $ defSettings)
    h <- Hedis.connect Hedis.defaultConnectInfo
    defaultMain
        [ bgroup "ping"
            [ bench "hedis 1"      $ nfIO (runPingH 1 h)
            , bench "redis-io 1"   $ nfIO (runPing  1 p)
            , bench "hedis 4"      $ nfIO (runPingH 4 h)
            , bench "redis-io 4"   $ nfIO (runPing  4 p)
            , bench "hedis 10"     $ nfIO (runPingH 10 h)
            , bench "redis-io 10"  $ nfIO (runPing  10 p)
            , bench "hedis 100"    $ nfIO (runPingH 100 h)
            , bench "redis-io 100" $ nfIO (runPing  100 p)
            ]
        , bgroup "get-and-set"
            [ bench "hedis 1"      $ nfIO (runGetSetH 1 h)
            , bench "redis-io 1"   $ nfIO (runSetGet 1 p)
            , bench "hedis 4"      $ nfIO (runGetSetH 4 h)
            , bench "redis-io 4"   $ nfIO (runSetGet 4 p)
            , bench "hedis 10"     $ nfIO (runGetSetH 10 h)
            , bench "redis-io 10"  $ nfIO (runSetGet 10 p)
            , bench "hedis 100"    $ nfIO (runGetSetH 100 h)
            , bench "redis-io 100" $ nfIO (runSetGet 100 p)
            ]
        ]
    shutdown p
    Logger.close g

runPing :: Int -> Pool -> IO ()
runPing n p = do
    x <- runRedis p $ pipelined $ Prelude.last <$> replicateM n ping
    x `seq` return ()

runPingH :: Int -> Hedis.Connection -> IO ()
runPingH n p = do
    x <- Hedis.runRedis p $ Prelude.last <$> replicateM n Hedis.ping
    x `seq` return ()

runSetGet :: Int -> Pool -> IO ()
runSetGet n p = do
    x <- runRedis p $ pipelined $ do
        replicateM_ n $ set "hello" "world" mempty
        get "hello" :: Redis IO (Maybe ByteString)
    x `seq` return ()

runGetSetH :: Int -> Hedis.Connection -> IO ()
runGetSetH n p = do
    x <- Hedis.runRedis p $ do
        replicateM_ n $ Hedis.set "world" "hello"
        Hedis.get "world"
    x `seq` return ()
