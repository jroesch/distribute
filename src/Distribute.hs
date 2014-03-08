{-# LANGUAGE OverloadedStrings, ScopedTypeVariables, RankNTypes #-}
module Distribute where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Pipes
import qualified Pipes
import qualified Pipes.Prelude as P
import qualified Pipes.ByteString as PB
import qualified Pipes.Network.TCP as PN
import qualified Data.Serialize as Cereal
import Data.Serialize (Serialize(..))
import Control.Monad
import Control.Monad.Trans.Either

import System.IO
import Data.IORef
import Data.Functor
import Control.Concurrent
import qualified Network as N
import qualified Network.Socket as NS

import Debug.Trace
import qualified Data.Map as M

import qualified Control.Monad.Trans.State as S

type PID = Int
data Location = Local | Remote
              deriving (Eq, Show)

data Process a = Process { _readPipe :: !(IORef (Producer a IO ()))
                         , _writePipe :: !(IORef (Consumer a IO ()))
                         }

data Registry a = Registry (IORef (M.Map Int (Process (DistributeMessage a))))

type Distribute a = S.StateT (Registry a) IO

encodePipe :: (Monad m, Serialize a) => Pipe a ByteString m ()
encodePipe = await >>= Pipes.yield . Cereal.runPut . put

decodePipe :: (Monad m, Serialize a) => Pipe ByteString a m ()
decodePipe = await >>= decodeElem
  where decodeElem = liftDecode (Cereal.runGetPartial get)
        liftDecode k =
          return . k >=> \res ->
            case res of
              Cereal.Fail m _ -> trace "foobar" $ error m
              Cereal.Partial kont ->
                await >>= (liftDecode kont)
              Cereal.Done x rest -> do
                Pipes.yield x
                liftDecode (Cereal.runGetPartial get) rest         

write :: (Serialize a) => Process (DistributeMessage a) -> a -> IO ()
write (Process _ writePipeRef) v = do
    writePipe <- readIORef writePipeRef
    runEffect $ (Pipes.yield (Value v)) >-> writePipe

read :: (Serialize a) => Process (DistributeMessage a) -> IO a
read (Process readPipeRef _) = do
  readPipe <- readIORef readPipeRef
  value <- next readPipe
  case value of
    Left _ -> error "Failure attempting to read from pipe."
    Right (r, pipe') -> do
      writeIORef readPipeRef pipe'
      case r of
        Value v -> return v
        Id i -> error "should not be reading control message here"

-- "node://domain:port"
simpleNameParser :: String -> Either String (String, N.PortNumber)
simpleNameParser s = Right ("localhost", fromIntegral 3000)

mkProcess :: (Serialize a) => N.Socket -> IO (Process a)
mkProcess sock = do
    readP <- newIORef $ PN.fromSocket sock 4096 >-> decodePipe
    writeP <- newIORef $ encodePipe >-> PN.toSocket sock
    return $ Process readP writeP

start :: (Serialize a) => Int -> (Process a -> IO ()) -> Distribute a ()
start port handler = do
    registry <- S.get
    sock <- lift $ N.listenOn (N.PortNumber $ fromIntegral port)
    lift $ forkIO $ do
      (s, _) <- NS.accept sock
      mkProcess s >>= handler
    return ()

registerProcess :: (Serialize a) => Int -> Process (DistributeMessage a) -> Distribute a ()
registerProcess pid process = do
    Registry ref <- S.get 
    lift $ modifyIORef ref (M.insert pid process)
    return ()
    
data DistributeMessage a = Value a
                         | Id Int
                         deriving (Eq, Show)

instance Serialize a => Serialize (DistributeMessage a) where
    get = do
      tag <- Cereal.getWord8
      case tag of
        0 -> Value <$> Cereal.get 
        1 -> Id <$> Cereal.get

    put (Value v) = do
      Cereal.putWord8 0
      Cereal.put v

    put (Id n) = do
      Cereal.putWord8 1
      Cereal.put n

emptyRegistry :: (Serialize a) => IO (Registry a)
emptyRegistry = do
    ref <- newIORef M.empty
    return $ Registry ref
{- do
  start 1 3000 registerProcess
  broadcast v
  results <- receiveAll
  synchronize -}

open :: Serialize a => String -> Int -> IO (Process a)
open host port = do
    (sock, _) <- PN.connectSock host (show port)
    mkProcess sock
    

