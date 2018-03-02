module Job
    ( Job(..)
    , Range(..)
    , RangeStart(..)
    , RangeEnd(..)
    , Position
    , Predicate
    , JobID
    ) where

import qualified Database.MySQL.BinLog as BinLog (BinLogTracker, RowBinLogEvent)

data Job = Job
  { range   :: Range
  , handler :: BinLog.RowBinLogEvent -> IO ()
  }

type JobID = Int

data Range      = Range { start :: RangeStart, end :: RangeEnd }
data RangeStart = Front | StartPos Position
data RangeEnd   = Never | EndPos Position | Trigger Predicate
type Position   = BinLog.BinLogTracker
type Predicate  = BinLog.RowBinLogEvent -> IO Bool
