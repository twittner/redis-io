-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Main (main) where

import Control.Exception (finally)
import CommandTests (tests)
import Network.Redis.IO
import Test.Tasty

import qualified System.Logger as Logger

main :: IO ()
main = do
    g <- Logger.new Logger.defSettings
    p <- mkPool g defSettings
    defaultMain (tests p) `finally` shutdown p `finally` Logger.close g

