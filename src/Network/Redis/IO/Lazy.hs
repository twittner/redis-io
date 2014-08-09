-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Network.Redis.IO.Lazy
    ( Lazy
    , lazy
    , force
    ) where

import Control.Applicative
import Control.Monad.IO.Class
import Data.IORef

newtype Lazy a = Lazy (IORef (Thunk a))

data Thunk a
    = Thunk (IO a)
    | Value !a

lazy :: MonadIO m => IO a -> m (Lazy a)
lazy a = liftIO $ Lazy <$> newIORef (Thunk a)

force :: MonadIO m => Lazy a -> m a
force (Lazy f) = liftIO $ do
    s <- readIORef f
    case s of
        Value a -> return a
        Thunk a -> do
            x <- a
            writeIORef f (Value x)
            return x
