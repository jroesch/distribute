{-# LANGUAGE OverloadedStrings, ScopedTypeVariables, RankNTypes, BangPatterns #-}
module Distribute where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Pipes
import qualified Pipes
import qualified Pipes.Prelude as P
import qualified Pipes.ByteString as PB
import qualified Pipes.Network.TCP as PN
import qualified Network.Simple.TCP as PS
import qualified Data.Serialize as Cereal
import Data.Serialize (Serialize(..))
import Control.Monad
import Control.Monad.Trans.Either

import System.IO
import Data.IORef
import Data.Functor
import Control.Concurrent
import Control.Concurrent.MVar
import qualified Network as N
import qualified Network.Socket as NS

import Debug.Trace
import qualified Data.Map as M

import qualified Control.Monad.Trans.State.Strict as S

import Control.Monad.Catch

type PID = Int
data Location = Local | Remote
              deriving (Eq, Show)

data Process a = Process { _readPipe :: !(MVar (Producer a IO ()))
                         , _writePipe :: !(MVar (Consumer a IO ()))
                         }

instance Show (Process a) where
    show p  = "<process>"

data Registry a = Registry !(MVar (M.Map PID (Process (DistributeMessage a))))

{- As a lens? processes :: Lens' (Registry a) (Process (DistributeMessage a))
processes f (Registry ) -}

processes :: Registry a -> IO [Process (DistributeMessage a)]
processes (Registry mvar) = do
    m <- readMVar mvar
    return $ M.elems m

processes' :: Registry a -> IO [(PID, Process (DistributeMessage a))]
processes' (Registry mvar) = do
    m <- readMVar mvar
    return $ M.assocs m

type Distribute a = S.StateT (PID, Registry a) IO

data DistributeMessage a = Value !a
                         | Id !Int
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

type DProcess a = Process (DistributeMessage a)

encodePipe :: (Monad m, Serialize a) => Pipe a ByteString m ()
encodePipe = await >>= Pipes.yield . Cereal.runPut . put

decodePipe :: (Monad m, Serialize a) => Pipe ByteString a m ()
decodePipe = await >>= decodeElem
  where decodeElem = liftDecode (Cereal.runGetPartial get)
        liftDecode k =
          return . k >=> \res ->
            case res of
              Cereal.Fail m _ -> error m
              Cereal.Partial kont ->
                await >>= (liftDecode kont)
              Cereal.Done x rest -> do
                Pipes.yield x
                liftDecode (Cereal.runGetPartial get) rest

write :: (Serialize a) => Process a -> a -> IO ()
write (Process _ writePipeRef) v = do
    writePipe <- takeMVar writePipeRef
    !result <- runEffect $ (Pipes.yield v) >-> writePipe
    putMVar writePipeRef writePipe

readD :: (Serialize a) => Process a -> IO a
readD (Process readPipeVar _) = do
  readPipe <- takeMVar readPipeVar
  value <- next readPipe
  case value of
    Left _ -> error "Failure attempting to readD from pipe."
    Right (r, pipe') -> do
      putMVar readPipeVar pipe'
      return r

-- "node://domain:port"
simpleNameParser :: String -> Either String (String, N.PortNumber)
simpleNameParser s = Right ("localhost", fromIntegral 3000)

mkProcess :: (Serialize a) => N.Socket -> IO (Process a)
mkProcess sock = do
    readP <- newMVar $ PN.fromSocket sock 4096 >-> decodePipe
    writeP <- newMVar $ encodePipe >-> PN.toSocket sock
    return $ Process readP writeP

start :: (Serialize a) => Int -> (DProcess a -> IO ()) -> Distribute a ()
start port handler = do
    registry <- S.get
    sock <- lift $ N.listenOn (N.PortNumber $ fromIntegral port)
    lift $ forkIO $ forever $ do
      (s, _) <- NS.accept sock
      forkIO $ mkProcess s >>= handler
    return ()

registerProcess :: (Serialize a) => PID -> DProcess a -> Distribute a ()
registerProcess pid process = do
    (_, Registry ref) <- S.get
    lift $ modifyMVar_ ref (return . M.insert pid process)
    return ()

emptyRegistry :: (Serialize a) => IO (Registry a)
emptyRegistry = do
    ref <- newMVar M.empty
    return $ Registry ref

lookupProcess :: (Serialize a) => PID -> Distribute a (DProcess a)
lookupProcess pid = do
    (_, Registry ref) <- S.get
    lift $ withMVar ref $ \pmap -> do
      -- putStrLn ("Sending to: " ++ (show pid))
      -- putStrLn ("With registry: " ++ (show $ M.keys pmap))
      case M.lookup pid pmap of
        Nothing -> error $ "attempting to send to nonexistant process: " ++ show pid ++ " has reg: " ++ show (M.keys pmap)
        Just v  -> return v

readP :: (Serialize a) => DProcess a -> IO a
readP process = do
  msg <- readD process
  case msg of
    Value v -> return v
    _ -> error "unhandled control message in readP"

writeP :: (Serialize a) => DProcess a -> a -> IO ()
writeP process value = do
    write process (Value value)

readFrom :: (Serialize a) => PID -> Distribute a a
readFrom pid = do
  process <- lookupProcess pid
  lift $ readP process

sendTo :: (Serialize a) => PID -> a -> Distribute a ()
sendTo pid value = do
  process <- lookupProcess pid
  lift $ writeP process value

open :: Serialize a => String -> Int -> Distribute a (DProcess a)
open host port = do
    (sock, _) <- PN.connectSock host (show port)
    onException (do
      process <- lift $ mkProcess sock
      (pid, _) <- S.get
      -- ordering is important here!
      lift $ write process (Id pid)
      msg <- lift $ readD process
      case msg of
        Id remoteId -> registerProcess remoteId process
        _ -> error "expected control message but found something else"
      return process)
      (PS.closeSock sock)

localState :: Monad m => s -> S.StateT s m a -> S.StateT s m a
localState s action = do
  oldState <- S.get
  S.put s
  result <- action
  S.put oldState
  return result

registerIncoming :: (Serialize a) => (PID, Registry a) -> DProcess a -> IO ()
registerIncoming (pid, Registry ref) p = do
  msg <- readD p
  case msg of
    Id i -> do
      old <- readMVar ref
      -- putStrLn (show old)
      modifyMVar_ ref (return . M.insert i p)
      new <- readMVar ref
      -- putStrLn (show new)
      write p (Id pid)
      return ()
    _ -> error "expected control message found something else"

broadcast :: (Serialize a) => a -> Distribute a ()
broadcast v = do
    (_, Registry ref) <- S.get
    reg <- lift $ readMVar ref
    lift $ mapM_ sendEach (M.elems reg)
  where sendEach p = write p (Value v)

receiveAll :: (Serialize a) => Distribute a [a]
receiveAll = undefined

runDistribute :: (Serialize a) => (PID, Registry a) -> Distribute a b -> IO b
runDistribute st m = S.evalStateT m st
