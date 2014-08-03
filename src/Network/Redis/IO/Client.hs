-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Network.Redis.IO.Client where

import Control.Applicative
import Control.Exception (throwIO)
import Control.Monad.IO.Class
import Control.Monad.Catch
import Control.Monad.Reader
import Data.IORef
import Data.Redis.Resp
import Data.Word
import Data.Pool hiding (Pool)
import Network.Redis.IO.Connection (Connection)
import Network.Redis.IO.Settings
import Network.Redis.IO.Timeouts (TimeoutManager)
import Network.Redis.IO.Types (ConnectionError (..))
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

request :: [Resp] -> Client [Either String Resp]
request a = do
    p <- ask
    let c = connPool p
        s = settings p
    liftIO $ case sMaxWaitQueue s of
        Nothing -> withResource c (C.request a)
        Just  q -> tryWithResource c (go p) >>= maybe (retry q c p) return
  where
    go p h = do
        atomicModifyIORef' (failures p) $ \n -> (if n > 0 then n - 1 else 0, ())
        C.request a h

    retry q c p = do
        k <- atomicModifyIORef' (failures p) $ \n -> (n + 1, n)
        unless (k < q) $
            throwIO ConnectionsBusy
        withResource c (go p)
