module Main where

import Test.Hspec

import Lib

main :: IO ()
main = hspec $
  describe "Lib" $
    it "spins up a worker" $ do
      worker <- newWorker

