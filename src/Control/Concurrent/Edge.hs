module Control.Concurrent.Edge
     ( Edge
     , Sink(..)
     , Source(..)
     , List
     , Item(..)
     , newEdge
     , newSink
     , getSource
     , dupSource
     , readSource
     , writeSink
     , fold, unsafeFold
     ) where

import Control.Concurrent.MVar
import System.IO.Unsafe(unsafeInterleaveIO)

type Edge a = (Source a, Sink a)

type List a = MVar (Item a)

-- In more general settings,  "a" probably ought not be strict
data Item a = Item a (List a)

-- | a sink is something that elements can be written to
newtype Sink   a = Sink   (MVar (List a))

-- | a source is something that elements can be read from
newtype Source a = Source (MVar (List a))

-- | returns a source and a sink
newEdge :: IO (Edge a)
newEdge = do
   hole     <- newEmptyMVar
   readVar  <- newMVar hole
   writeVar <- newMVar hole
   return (Source readVar, Sink writeVar)

newSink :: IO (Sink a)
newSink = Sink `fmap` (newMVar =<< newEmptyMVar)

getSource :: Sink   a -> IO (Source a)
getSource (Sink   a) = Source `fmap` dupMVar a

dupSource :: Source a -> IO (Source a)
dupSource (Source a) = Source `fmap` dupMVar a

dupMVar :: MVar a -> IO (MVar a)
dupMVar x = do
    modifyMVar x $ \a -> do
      b <- newMVar a
      return (a,b)

readSource :: Source a -> IO a
readSource (Source readVar) = do
    modifyMVar readVar $ \read_end -> do
      (Item val new_read_end) <- readMVar read_end
      return (new_read_end, val)

writeSink :: Sink a -> a -> IO ()
writeSink (Sink writeVar) a = do
    modifyMVar_ writeVar $ \hole -> do
      new_hole <- newEmptyMVar
      putMVar hole (Item a new_hole)
      return new_hole

-- | a right fold over a source.  The source is duplicated
-- to avoid race conditions when the source is being read
-- from by a different thread at the same time.

fold :: (a -> b -> b) -> Source a -> IO b
fold f source = unsafeFold f =<< dupSource source

-- | unsafeFold should usually only be called on sources that
-- have one reader.  If there is more than one reader, then
-- the result will be affected by race conditions.

unsafeFold :: (a -> b -> b) -> Source a -> IO b
unsafeFold f = loop
   where
     loop source = unsafeInterleaveIO $ do
       a <- readSource source
       b <- loop source
       return (f a b)
