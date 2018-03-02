module Worker
    ( Worker(..)
    , Database(..)
    , attachJob
    , detachJob
    , newWorker
    ) where

import Data.Text (Text)
import Data.Word (Word32)
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM.TChan (TChan, tryReadTChan, writeTChan, newTChan)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.MVar (MVar, modifyMVar, modifyMVar_, putMVar, newEmptyMVar,
                                readMVar, newMVar)
import Control.Concurrent.Async (async, Async)
import Control.Monad.Reader (ReaderT, runReaderT, liftIO, asks)
import qualified Data.Map.Strict as Map (Map, insert, delete, empty)
import qualified Database.MySQL.BinLog as BinLog (BinLogTracker)

import Job

data Worker = Worker
  { commands :: TChan WorkerCommand
  , handle   :: Async ()
  }

data Database = Database
  { dbUser     :: Text
  , dbPassword :: Text
  , dbHost     :: String
  , dbDatabase :: Text
  , dbSlaveID  :: Word32
  }

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

newWorker :: Database -> IO Worker
newWorker db = do
  current   <- newEmptyMVar
  jobs      <- newMVar Map.empty
  nextJobID <- newMVar 0
  commands  <- atomically newTChan
  handle    <- startWorker
    WorkerState
      { wrksDatabase  = db
      , wrksCurrent   = current
      , wrksJobs      = jobs
      , wrksNextJobID = nextJobID
      , wrksCommands  = commands
      }
  return $ Worker commands handle
  where
    startWorker state = async $ runReaderT work state

data WorkerState = WorkerState
  { wrksDatabase  :: Database
  , wrksCurrent   :: MVar BinLog.BinLogTracker
  , wrksJobs      :: MVar (Map.Map JobID Job)
  , wrksNextJobID :: MVar JobID
  , wrksCommands  :: TChan WorkerCommand
  }

data WorkerCommand =
    Attach { job   :: Job,   attachResp :: MVar JobID }
  | Detach { jobID :: JobID, detachResp :: MVar () }

type Work = ReaderT WorkerState IO

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
