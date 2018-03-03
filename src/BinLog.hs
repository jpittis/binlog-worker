{-# LANGUAGE LambdaCase #-}
module BinLog
    ( newStreamer
    , Position
    , Database(..)
    , Streamer
    , strmStream
    , connFromDatabase
    ) where

import Control.Concurrent.Async (async, Async)
import Control.Concurrent.MVar (MVar, putMVar, newEmptyMVar)
import Prelude hiding (read)
import qualified Database.MySQL.Base as MySQL
import qualified Database.MySQL.BinLog as BinLog
import Database.MySQL.BinLog (RowBinLogEvent)
import System.IO.Streams (InputStream, read)
import qualified Database.MySQL.BinLogProtocol.BinLogEvent as BinLog (fdCreateTime)
import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.Word (Word32)
import Data.IORef (IORef)
import Data.Text.Encoding (encodeUtf8)

data Streamer = Streamer
  { strmCurrentFileName :: !(IORef ByteString)
  , strmCreateTime      :: !Word32
  , strmStream          :: !(InputStream RowBinLogEvent)
  , strmConn            :: !MySQL.MySQLConn
  , strmNextEvent       :: !(MVar (Maybe RowBinLogEvent))
  , strmHandle          :: !(Async ())
  }

type Position = Maybe BinLog.BinLogTracker

data Database = Database
  { dbUser     :: Text
  , dbPassword :: Text
  , dbHost     :: String
  , dbDatabase :: Text
  , dbSlaveID  :: Word32
  }

newStreamer :: Database -> Position -> IO (Maybe Streamer)
newStreamer config pos = do
  conn <- connFromDatabase config
  createStream config pos conn

connFromDatabase :: Database -> IO MySQL.MySQLConn
connFromDatabase (Database user pass host db _) = 
  MySQL.connect 
    MySQL.defaultConnectInfo
      { MySQL.ciUser     = encodeUtf8 user
      , MySQL.ciPassword = encodeUtf8 pass
      , MySQL.ciDatabase = encodeUtf8 db
      , MySQL.ciHost     = host
      }

createStream :: Database -> Position -> MySQL.MySQLConn -> IO (Maybe Streamer)
createStream config pos conn =
  trackerFromPos pos conn >>= \case
    Nothing     -> return Nothing
    Just latest -> do
      (create, current, stream) <- createRowStream latest
      (nextEvent, handle)       <- streamToMVar stream
      return . Just $ Streamer
        { strmCurrentFileName = current
        , strmCreateTime      = create
        , strmStream          = stream
        , strmConn            = conn
        , strmNextEvent       = nextEvent
        , strmHandle          = handle
        }
  where
    createRowStream tracker = do
      stream@(format, current, _) <- BinLog.dumpBinLog conn (dbSlaveID config) tracker False
      rowStream <- BinLog.decodeRowBinLogEvent stream
      return (BinLog.fdCreateTime format, current, rowStream)
    trackerFromPos Nothing conn = BinLog.getLastBinLogTracker conn
    trackerFromPos pos _ = return pos

-- InputStream read is blocking. Forward to MVar so that non blocking read can be
-- performed with tryReadMVar.
streamToMVar :: InputStream a -> IO (MVar (Maybe a), Async ())
streamToMVar stream = do
  var    <- newEmptyMVar
  handle <- async $ forwardEvents stream var
  return (var, handle)
  where
    forwardEvents stream var = do
      event <- read stream
      putMVar var event
      case event of
        Nothing -> return ()
        Just _  -> forwardEvents stream var
