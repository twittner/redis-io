-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Network.Redis.IO.Timeouts
    ( TimeoutManager
    , create
    , destroy
    , Action
    , Milliseconds (..)
    , add
    , cancel
    , withTimeout
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async (Async, async)
import Control.Exception (mask_, bracket)
import Control.Monad
import Data.IORef
import Network.Redis.IO.Types (Milliseconds (..), ignore)

import qualified Control.Concurrent.Async as Async

data TimeoutManager = TimeoutManager
    { elements  :: IORef [Action]
    , loop      :: Async ()
    , roundtrip :: !Int
    }

data Action = Action
    { action :: IO ()
    , state  :: IORef State
    }

data State = Wait !Int | Canceled

create :: Milliseconds -> IO TimeoutManager
create (Ms n) = do
    r <- newIORef []
    l <- async $ forever $ do
            threadDelay (n * 1000)
            mask_ $ do
                prev <- atomicModifyIORef' r $ \x -> ([], x)
                next <- prune prev id
                atomicModifyIORef' r $ \x -> (next x, ())
    return $ TimeoutManager r l n
  where
    prune []     front = return front
    prune (a:aa) front = do
        s <- atomicModifyIORef' (state a) $ \x -> (newState x, x)
        case s of
            Wait 0 -> do
                ignore (action a)
                prune aa front
            Canceled -> prune aa front
            _        -> prune aa (front . (a:))

    newState (Wait k) = Wait (k - 1)
    newState s        = s

destroy :: TimeoutManager -> Bool -> IO ()
destroy tm exec = do
    Async.cancel (loop tm)
    when exec $
        readIORef (elements tm) >>= mapM_ f
  where
    f e = readIORef (state e) >>= \s -> case s of
        Wait  _ -> ignore (action e)
        _       -> return ()

add :: TimeoutManager -> Milliseconds -> IO () -> IO Action
add tm (Ms n) a = do
    r <- Action a <$> newIORef (Wait $ n `div` roundtrip tm)
    atomicModifyIORef' (elements tm) $ \rr -> (r:rr, ())
    return r

cancel :: Action -> IO ()
cancel a = atomicWriteIORef (state a) Canceled

withTimeout :: TimeoutManager -> Milliseconds -> IO () -> IO a -> IO a
withTimeout tm m x a = bracket (add tm m x) cancel $ const a
