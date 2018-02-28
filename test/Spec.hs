{-# LANGUAGE OverloadedStrings #-}
module Main where

import Test.Hspec

import qualified Heh
import Control.Exception (bracket)
import Control.Concurrent.Async (cancel)

import Lib

withDatabase :: (Database -> IO a) -> IO a
withDatabase action =
  Heh.withMySQL "binlog-worker-mysql" 3306 "" $
    \(host, password) -> action (database host password)
  where
    database host password =
      Database
        { dbUser     = "root"
        , dbPassword = password
        , dbHost     = host
        , dbDatabase = ""
        , dbSlaveID  = 2
        }

withWorker :: (Worker -> IO a) -> IO a
withWorker action =
  withDatabase $ \database ->
   bracket (newWorker database) cancelWorker action
  where
    cancelWorker (Worker _ handle) = cancel handle

main :: IO ()
main = hspec $
  describe "Lib" $
    it "spins up a worker" $
      withWorker $ \worker -> do
        id <- attachJob worker $ Job (Range Front Never) (\_ -> return ())
        detachJob worker id
        True `shouldBe` True

