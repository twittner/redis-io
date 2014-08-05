-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

-- Like Haxl (http://community.haskell.org/~simonmar/papers/haxl-icfp14.pdf)
-- but worse ...
module Network.Redis.IO.Fetch where

import Control.Applicative
import Control.Monad.IO.Class
import Data.IORef
import Data.Monoid
import Data.Redis.Command
import Data.Sequence (Seq, singleton)
import Data.Traversable

data Request where
    Request :: Command a -> IORef (Status a) -> Request

data Status a = Empty | Value !a

data Result m a
    = Done a
    | Blocked (Seq Request) (Fetch m a)

newtype Fetch m a = Fetch { unFetch :: m (Result m a) }

instance Monad m => Functor (Fetch m) where
    fmap f h = h >>= return . f

instance Monad m => Monad (Fetch m) where
    return a = Fetch $ return (Done a)
    Fetch m >>= k = Fetch $ do
        r <- m
        case r of
            Done       a -> unFetch (k a)
            Blocked br c -> return (Blocked br (c >>= k))

instance Monad m => Applicative (Fetch m) where
    pure = return
    Fetch f <*> Fetch x = Fetch $ do
        f' <- f
        x' <- x
        case (f', x') of
            (Done       g, Done       y) -> return (Done (g y))
            (Done       g, Blocked b  c) -> return (Blocked b (g <$> c))
            (Blocked b  c, Done       y) -> return (Blocked b (c <*> return y))
            (Blocked b1 c, Blocked b2 d) -> return (Blocked (b1 <> b2) (c <*> d))

instance MonadIO m => RunCommand (Fetch m) where
    runCommand r = Fetch $ do
        box <- liftIO $ newIORef Empty
        let b = Request r box
        let k = Fetch $ do
             Value a <- liftIO $ readIORef box
             return (Done a)
        return (Blocked (singleton b) k)

mapM :: (Applicative f, Traversable t) => (a -> f b) -> t a -> f (t b)
mapM = traverse

sequence :: (Applicative f, Traversable t) => t (f a) -> f (t a)
sequence = sequenceA
