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
    ) where

import qualified Database.MySQL.BinLog as BinLog (BinLogTracker, RowBinLogEvent)

import Data.Text (Text)
import Data.Word (Word32)

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

data Worker = Worker
  { wrkDatabase  :: Database
  , wrkCurrent   :: MVar BinLog.BinLogTracker
  , wrkJobs      :: MVar (Map.Map JobID Job)
  , wrkNextJobID :: MVar JobID
  , wrkCommands  :: TChan WorkerCommand
  }

type JobID = Int

data WorkerCommand =
    Attach { job   :: Job,   attachResp :: MVar JobID }
  | Detach { jobID :: JobID, detachResp :: MVar () }

type Work = ReaderT Worker IO

newWorker :: Database -> IO (Async ())
newWorker db = do
  current   <- newEmptyMVar
  jobs      <- newMVar Map.empty
  nextJobID <- newMVar 0
  commands  <- atomically newTChan
  async $ startWorker Worker
    { wrkDatabase  = db
    , wrkCurrent   = current
    , wrkJobs      = jobs
    , wrkNextJobID = nextJobID
    , wrkCommands  = commands
    }
  where
    startWorker = runReaderT work

work :: Work ()
work = do
  commands     <- asks wrkCommands
  maybeCommand <- liftIO (atomically $ tryReadTChan commands)
  case maybeCommand of
    Just (Attach job resp)   -> attach job resp
    Just (Detach jobID resp) -> detach jobID resp
    _                        -> return ()
  work
  where
    attach :: Job -> MVar JobID -> Work ()
    attach job resp = do
      nextJobID <- asks wrkNextJobID
      jobs      <- asks wrkJobs
      jobID     <- liftIO (modifyMVar nextJobID $ \next -> return (next + 1, next))
      liftIO $ modifyMVar_ jobs $ \jobMap -> return $ Map.insert jobID job jobMap
      liftIO $ putMVar resp jobID
    detach :: JobID -> MVar () -> Work ()
    detach jobID resp = do
      jobs <- asks wrkJobs
      liftIO $ modifyMVar_ jobs $ \jobMap -> return $ Map.delete jobID jobMap
      liftIO $ putMVar resp ()

attachJob :: Job -> Work JobID
attachJob job = do
  commands <- asks wrkCommands
  resp     <- liftIO newEmptyMVar
  liftIO $ atomically $ writeTChan commands (Attach job resp)
  liftIO $ readMVar resp

detachJob :: JobID -> Work ()
detachJob jobID = do
  commands <- asks wrkCommands
  resp     <- liftIO newEmptyMVar
  liftIO (atomically $ writeTChan commands (Detach jobID resp))
  liftIO $ readMVar resp
