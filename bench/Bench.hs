{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Criterion
import Criterion.Main
import Data.Monoid
import Data.Redis
import Network.Redis.IO

import qualified Database.Redis as Hedis
import qualified System.Logger  as Logger

main :: IO ()
main = do
    g <- Logger.new Logger.defSettings
    p <- mkPool g (setMaxConnections 50 . setPoolStripes 1 $ defSettings)
    h <- Hedis.connect Hedis.defaultConnectInfo
    defaultMainWith defaultConfig (return ())
        [ bgroup "ping"
            [ bench "hedis" $ runPingH h
            , bench "redis-io" $ runPing p
            ]
        , bgroup "get-and-set"
            [ bench "hedis" $ runGetSetH h
            , bench "redis-io" $ runSetGet p
            ]
        ]
    shutdown p
    Logger.close g

runPing :: Pool -> IO ()
runPing p = do
    x <- runRedis p $ do
        ping
        ping
        ping
        ping
        ping
        ping
        ping
        ping
        ping
        ping
        ping
    x `seq` return ()

runPingH :: Hedis.Connection -> IO ()
runPingH p = do
    x <- Hedis.runRedis p $ do
        Hedis.ping
        Hedis.ping
        Hedis.ping
        Hedis.ping
        Hedis.ping
        Hedis.ping
        Hedis.ping
        Hedis.ping
        Hedis.ping
        Hedis.ping
        Hedis.ping
    x `seq` return ()

runSetGet :: Pool -> IO ()
runSetGet p = do
    x <- runRedis p $ do
        set "hello1" "world" mempty
        set "hello2" "world" mempty
        set "hello3" "world" mempty
        set "hello4" "world" mempty
        set "hello5" "world" mempty
        set "hello6" "world" mempty
        set "hello7" "world" mempty
        set "hello8" "world" mempty
        set "hello9" "world" mempty
        set "hello0" "world" mempty
        get "hello5"
    x `seq` return ()

runGetSetH :: Hedis.Connection -> IO ()
runGetSetH p = do
    x <- Hedis.runRedis p $ do
        Hedis.set "helloA" "world"
        Hedis.set "helloB" "world"
        Hedis.set "helloC" "world"
        Hedis.set "helloD" "world"
        Hedis.set "helloE" "world"
        Hedis.set "helloF" "world"
        Hedis.set "helloG" "world"
        Hedis.set "helloH" "world"
        Hedis.set "helloI" "world"
        Hedis.set "helloJ" "world"
        Hedis.set "helloK" "world"
        Hedis.get "helloG"
    x `seq` return ()
