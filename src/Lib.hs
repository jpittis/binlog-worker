module Lib
    ( Job(..)
    , Range(..)
    , RangeStart(..)
    , RangeEnd(..)
    , Position
    , Predicate
    , Database(..)
    , newWorker
    , attachJob
    , detachJob
    , Worker(..)
    ) where

import qualified Database.MySQL.BinLog as BinLog (BinLogTracker, RowBinLogEvent)

import Data.Text (Text)
import Data.Word (Word32)

import Control.Concurrent
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM
import Control.Concurrent.MVar
import Control.Concurrent.Async
import Control.Monad.Reader

import qualified Data.Map.Strict as Map

data Job = Job
  { jobRange    :: Range
  , jobHandler  :: BinLog.RowBinLogEvent -> IO ()
  }

data Range      = Range { start :: RangeStart, end :: RangeEnd }
data RangeStart = Front | StartPos Position
data RangeEnd   = Never | EndPos Position | Trigger Predicate
type Position   = BinLog.BinLogTracker
type Predicate  = BinLog.RowBinLogEvent -> IO Bool

data Database = Database
  { dbUser     :: Text
  , dbPassword :: Text
  , dbHost     :: String
  , dbDatabase :: Text
  , dbSlaveID  :: Word32
  }

data WorkerState = WorkerState
  { wrksDatabase  :: Database
  , wrksCurrent   :: MVar BinLog.BinLogTracker
  , wrksJobs      :: MVar (Map.Map JobID Job)
  , wrksNextJobID :: MVar JobID
  , wrksCommands  :: TChan WorkerCommand
  }

data Worker = Worker
  { wrkCommands :: TChan WorkerCommand
  , wrkHandle   :: Async ()
  }

type JobID = Int

data WorkerCommand =
    Attach { job   :: Job,   attachResp :: MVar JobID }
  | Detach { jobID :: JobID, detachResp :: MVar () }

type Work = ReaderT WorkerState IO

newWorker :: Database -> IO Worker
newWorker db = do
  current   <- newEmptyMVar
  jobs      <- newMVar Map.empty
  nextJobID <- newMVar 0
  commands  <- atomically newTChan
  handle    <- async $ startWorker
    WorkerState
      { wrksDatabase  = db
      , wrksCurrent   = current
      , wrksJobs      = jobs
      , wrksNextJobID = nextJobID
      , wrksCommands  = commands
      }
  return $ Worker commands handle
  where
    startWorker = runReaderT work

work :: Work ()
work = do
  commands     <- asks wrksCommands
  maybeCommand <- liftIO (atomically $ tryReadTChan commands)
  case maybeCommand of
    Just (Attach job resp)   -> attach job resp
    Just (Detach jobID resp) -> detach jobID resp
    _                        -> return ()
  -- Tight loops without memory allocations cause the thread to not be pre-empted.
  -- Force pre-emption!
  liftIO $ threadDelay 1
  work
  where
    attach :: Job -> MVar JobID -> Work ()
    attach job resp = do
      nextJobID <- asks wrksNextJobID
      jobs      <- asks wrksJobs
      jobID     <- liftIO (modifyMVar nextJobID $ \next -> return (next + 1, next))
      liftIO $ modifyMVar_ jobs $ \jobMap -> return $ Map.insert jobID job jobMap
      liftIO $ putMVar resp jobID
    detach :: JobID -> MVar () -> Work ()
    detach jobID resp = do
      jobs <- asks wrksJobs
      liftIO $ modifyMVar_ jobs $ \jobMap -> return $ Map.delete jobID jobMap
      liftIO $ putMVar resp ()

attachJob :: Worker -> Job -> IO JobID
attachJob (Worker commands _) job = do
  resp <- newEmptyMVar
  atomically $ writeTChan commands (Attach job resp)
  readMVar resp

detachJob :: Worker -> JobID -> IO ()
detachJob (Worker commands _) jobID = do
  resp <- newEmptyMVar
  atomically $ writeTChan commands (Detach jobID resp)
  readMVar resp
