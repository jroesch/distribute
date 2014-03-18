{-# LANGUAGE BangPatterns #-}
import Test.Tasty
import Test.Tasty.HUnit as HU
import Test.Tasty.QuickCheck as QC
import Test.HUnit as H

import Data.IORef
import Data.Functor.Identity
import Distribute as D
import Pipes
import Pipes.Prelude as P
import qualified Pipes.Network.TCP as PN
import Network.Simple.TCP (closeSock)
import Control.Monad
import Control.Monad.Trans.Either
import Data.Serialize (Serialize(..), encode)
import Control.Concurrent
import System.IO
import Debug.Trace

import qualified Data.ByteString as B

import Control.Monad.Trans.State.Strict as S
main = defaultMain properties

properties :: TestTree
properties = testGroup "Properties" [qcProps, unitTests]

unitTests = testGroup "HUnit" [test_open]

qcProps = testGroup "QuickCheck" [property_encodeDecode]

property_encodeDecode = QC.testProperty "id == encodeDecodeId" prop
  where prop :: [String] -> Bool
        prop v = (id v) == (encodeDecodeId v)

encodeDecodeId :: (Serialize a) => a -> a
encodeDecodeId value =
    case pipe value of
        Left e -> error "Shouldn't happen!"
        Right v -> case v of
          Nothing -> error "Shouldn't happen!"
          Just v' -> v'
  where pipe v = P.head $ (Pipes.yield v >-> encodePipe) >-> decodePipe

test_open = HU.testCase "open should connect to a running process" test
  where test = do
          reg <- emptyRegistry
          evalStateT body (1, reg)
        body = do
          state <- S.get
          D.start 3000 (D.registerIncoming state)
          freshReg <- lift $ emptyRegistry
          D.localState (2, freshReg) $ do
            D.open "localhost" 3000
            D.sendTo 1 (1 :: Int)
            D.sendTo 1 (2 :: Int)
            D.sendTo 1 (3 :: Int)
          one <- D.readFrom 2
          two <- D.readFrom 2
          three <- D.readFrom 2
          lift $ H.assertEqual "should read 1" one (1 :: Int)
          lift $ H.assertEqual "should read 2" two (2 :: Int)
          lift $ H.assertEqual "should read 3" three (3 :: Int)

{- test_receive_message =
    HU.testCase "should be able to read a message from a speicific process" $ do
      emptyRegistry >>= evalStateT $ do
        D.start 3000 _
        readFrom 1 -}
