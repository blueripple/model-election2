{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StandaloneDeriving #-}

module BlueRipple.Model.Election2.ModelCommon2
  (
    module BlueRipple.Model.Election2.ModelCommon2
  )
where

--import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.Small.Loaders as BRDF
import qualified BlueRipple.Data.CachingCore as BRCC
import qualified BlueRipple.Data.LoadersCore as BRLC
import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Data.Types.Demographic as DT
import qualified BlueRipple.Data.Types.Geographic as GT
import qualified BlueRipple.Data.Types.Modeling as MT
import qualified BlueRipple.Model.Election2.ModelCommon as MC
import qualified BlueRipple.Model.StanTools as MST

import qualified Knit.Report as K hiding (elements)

import qualified Numeric
import qualified Control.Foldl as FL
import Control.Lens (view, (^.))
import qualified Data.IntMap.Strict as IM
import qualified Data.List as List
import qualified Data.Map.Strict as M
import qualified Data.Set as S

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as V

import qualified Frames as F
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.Melt as F
import qualified Frames.Serialize as FS
import qualified Frames.Constraints as FC

import qualified CmdStan as CS
import qualified Stan.ModelBuilder as SMB
import qualified Stan.ModelRunner as SMR
import qualified Stan.ModelConfig as SC
import qualified Stan.Parameters as SP
import qualified Stan.RScriptBuilder as SR
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.BuildingBlocks.GroupAlpha as SG
import qualified Stan.ModelBuilder.Distributions as SMD
import qualified Stan.ModelBuilder.Distributions.RealBinomial as SMD
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
--import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TEO
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))
import qualified Flat
import Flat.Instances.Vector ()

--stateG :: SMB.GroupTypeTag Text
--stateG = SMB.GroupTypeTag "State"

data Config a b where
  ActionOnly :: MC.ModelCategory -> MC.ActionConfig a b -> Config a b
  PrefOnly :: MC.ModelCategory -> MC.PrefConfig b -> Config (F.Record DP.CESByCDR) b
  ActionAndPref :: MC.ModelCategory -> MC.ActionConfig a b -> MC.PrefConfig b -> Config a b

configModelCategory :: Config a b -> MC.ModelCategory
configModelCategory (ActionOnly mc _) = mc
configModelCategory (PrefOnly mc _) = mc
configModelCategory (ActionAndPref mc _ _) = mc

actionSurvey :: Config a b -> Maybe (MC.ActionSurvey a)
actionSurvey (ActionOnly _ (MC.ActionConfig rs _)) = Just rs
actionSurvey (PrefOnly _ _) = Nothing
actionSurvey (ActionAndPref _ (MC.ActionConfig ts _) _) = Just ts

usesCPS :: Config a b -> Bool
usesCPS c = case actionSurvey c of
  Nothing -> False
  Just as -> case as of
    MC.CPSSurvey -> True
    _ -> False

usesCES :: Config a b -> Bool
usesCES c = case actionSurvey c of
  Nothing -> False -- here we use only the "Pref" version
  Just as -> case as of
    MC.CESSurvey _ -> True
    _ -> False

usesCESPref :: Config a b -> Bool
usesCESPref (ActionOnly _ _)  = False
usesCESPref (PrefOnly _ _) = True
usesCESPref (ActionAndPref _ _ _) = True

{-
weightingStyle :: Config a b -> DP.WeightingStyle
weightingStyle (ActionOnly _ ws _) = ws
weightingStyle (PrefOnly _ ws _) = ws
weightingStyle (ActionAndPref _ ws _ _) = ws
-}

configText :: forall a b. Config a b -> Text
configText (ActionOnly c (MC.ActionConfig as mc)) = case c of
  MC.Reg -> "Reg" <> MC.actionSurveyText as <> "_" <> MC.modelConfigText mc
  MC.Vote -> "Turnout" <> MC.actionSurveyText as <> "_" <> MC.modelConfigText mc
configText (PrefOnly c (MC.PrefConfig sp mc)) = case c of
  MC.Reg -> "RegPref" <> "_" <> DP.surveyPortionText sp <> "_" <> MC.modelConfigText mc
  MC.Vote -> "Pref" <> "_" <> DP.surveyPortionText sp <> "_" <> MC.modelConfigText mc
configText (ActionAndPref c (MC.ActionConfig as tMC) (MC.PrefConfig sp pMC)) = case c of
  MC.Reg -> "RegBoth" <> MC.actionSurveyText as
            <> "_" <> MC.modelConfigText tMC
            <> "_" <> MC.modelConfigText pMC
  MC.Vote -> "Both" <> MC.actionSurveyText as
             <> "_" <> MC.modelConfigText tMC
             <> "_" <> MC.modelConfigText pMC

groupBuilder :: forall g k lk l a b .
                 (Foldable g
                 , Typeable (DP.PSDataR k)
                 , Show (F.Record l)
                 , Ord (F.Record l)
                 , l F.⊆ DP.PSDataR k
                 , Typeable l
                 , F.ElemOf (DP.PSDataR k) GT.StateAbbreviation
                 , F.ElemOf (DP.CESByR lk) GT.StateAbbreviation
                 , DP.DCatsR F.⊆ DP.PSDataR k
                 , DP.DCatsR F.⊆ DP.CESByR lk
                 , Typeable (DP.CESByR lk)
                 , FSI.RecVec (DP.CESByR lk)
                 , FC.ElemsOf (DP.CESByR lk) [DP.Registered2p, DP.VotesInRace]
                 , FSI.RecVec (DP.PSDataR k)
                 , F.ElemOf (DP.PSDataR k) DT.PopCount
                 )
               => Config a b
               -> g Text
               -> g (F.Record l)
               -> SMB.StanGroupBuilderM (DP.ModelData lk) (DP.PSData k) ()
groupBuilder config states psKeys = do
  let groups' = MC.groups states
  when (usesCPS config) $ SMB.addModelDataToGroupBuilder "CPS" (SMB.ToFoldable DP.cpsData) >>= MC.addGroupIndexesAndIntMaps groups'
  when (usesCES config) $ SMB.addModelDataToGroupBuilder "CES" (SMB.ToFoldable DP.cesData) >>= MC.addGroupIndexesAndIntMaps groups'
  when (usesCESPref config) $ case configModelCategory config of
    MC.Reg -> SMB.addModelDataToGroupBuilder "CESR" (SMB.ToFoldable DP.cesDataRegPref) >>= MC.addGroupIndexesAndIntMaps groups'
    MC.Vote -> SMB.addModelDataToGroupBuilder "CESP" (SMB.ToFoldable DP.cesDataVotePref) >>= MC.addGroupIndexesAndIntMaps groups'
  MC.psGroupBuilder states psKeys

-- Given 2 real numbers x and y , find the two integers k, l
-- such that |x - k| < 1, |y - l| < 1 and minimizing |1 - ky/lx|
-- that is, we minimize the difference of the log of the probabilities
roundFraction :: Double -> Double -> (Int, Int)
roundFraction x y = fst $ List.head $ sortOn snd $ withMetric where
  possible_kl = [(floor x, floor y), (floor x, ceiling y), (ceiling x, floor y), (ceiling x, ceiling y)]
  metric (k, l) = abs $ 1 - ((realToFrac k * y) / (realToFrac l * x))
  withMetric = zip possible_kl $ fmap metric possible_kl

rfVia :: (a -> Double) -> (a -> Double) -> a -> (Int, Int)
rfVia getX getY a = roundFraction (getX a) (getY a)

actionModelData :: forall a b gq lk . MC.ModelCategory -> MC.ActionConfig a b
                 -> SMB.StanBuilderM (DP.ModelData lk) gq (MC.ModelData a b)
actionModelData c (MC.ActionConfig ts mc) = do
  let cpsSurveyDataTag = SMB.dataSetTag @(F.Record DP.CPSByStateR) SC.ModelData "CPS"
      cesSurveyDataTag = SMB.dataSetTag @(F.Record DP.CESByCDR) SC.ModelData "CES"
      uwSurveyed rtt = SBB.addCountData rtt "Surveyed" (view DP.surveyed)
      weightedCount ws = DP.weightedCountF ws (view DP.surveyed) (view DP.surveyWeight) (view DP.surveyedESS)
      weightedQty ws = DP.weightedQtyF ws (view DP.surveyed) (view DP.surveyWeight) (view DP.surveyedESS)
      rwSurveyed ws rtt = SBB.addCountData rtt "Surveyed" (round . weightedCount ws)
      wSurveyed ws rtt = SBB.addRealData rtt "Surveyed" (Just 0) Nothing (weightedCount ws)
      uwAction :: forall rs md. (FC.ElemsOf rs DP.CountDataR) => SMB.RowTypeTag (F.Record rs) -> SMB.StanBuilderM md gq TE.IntArrayE
      uwAction rtt = case c of
        MC.Reg -> SBB.addCountData rtt "Registered" (view DP.registered)
        MC.Vote -> SBB.addCountData rtt "Voted" (view DP.voted)
      rwAction :: forall rs md. (FC.ElemsOf rs DP.CountDataR)
               => DP.WeightingStyle -> SMB.RowTypeTag (F.Record rs) -> SMB.StanBuilderM md gq TE.IntArrayE
      rwAction ws rtt = case c of
        MC.Reg -> SBB.addCountData rtt "Registered" (round . weightedQty ws (view DP.registeredW))
        MC.Vote -> SBB.addCountData rtt "Voted" (round . weightedQty ws (view DP.votedW))
      wAction :: forall rs md. (FC.ElemsOf rs DP.CountDataR)
              => DP.WeightingStyle -> SMB.RowTypeTag (F.Record rs) -> SMB.StanBuilderM md gq TE.VectorE
      wAction ws rtt = case c of
        MC.Reg -> SBB.addRealData rtt "Registered" (Just 0) Nothing (weightedQty ws (view DP.registeredW))
        MC.Vote -> SBB.addRealData rtt "Voted" (Just 0) Nothing (weightedQty ws (view DP.votedW))
  case ts of
    MC.CPSSurvey -> case mc.mcSurveyAggregation of
      MC.UnweightedAggregation -> fmap MC.ModelData $ cpsSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc uwSurveyed uwAction
      MC.RoundedWeightedAggregation ws -> fmap MC.ModelData $ cpsSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc (rwSurveyed ws) (rwAction ws)
      MC.WeightedAggregation _ ws -> fmap MC.ModelData $ cpsSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc (wSurveyed ws) (wAction ws)
    MC.CESSurvey _ -> case mc.mcSurveyAggregation of
      MC.UnweightedAggregation -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc uwSurveyed uwAction
      MC.RoundedWeightedAggregation ws -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc (rwSurveyed ws) (rwAction ws)
      MC.WeightedAggregation _ ws -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc (wSurveyed ws) (wAction ws)

--type PrefModelDataR = [DP.Registered2p, DP.Registered2pW, DP.Registered2pESS, DP.VotesInRaceESS, DP.VotesInRaceW, DP.DVotesW, DP.DRegW]

prefModelData :: forall b gq lk . MC.ModelCategory -> MC.PrefConfig b
              -> SMB.StanBuilderM (DP.ModelData lk) gq (MC.ModelData (F.Record DP.CESByCDR) b)
prefModelData c (MC.PrefConfig _x mc) = do
  let cesSurveyDataTag = SMB.dataSetTag @(F.Record DP.CESByCDR) SC.ModelData "CESP"
      uwAction :: forall rs md. (FC.ElemsOf rs DP.PrefDataR) => SMB.RowTypeTag (F.Record rs) -> SMB.StanBuilderM md gq TE.IntArrayE
      uwAction rtt = case c of
        MC.Reg -> SBB.addCountData rtt "Registered" (view DP.registered2p)
        MC.Vote -> SBB.addCountData rtt "VotesInRace" (view DP.votesInRace)
      uwPrefD :: forall rs md. (FC.ElemsOf rs DP.PrefDataR) => SMB.RowTypeTag (F.Record rs) -> SMB.StanBuilderM md gq TE.IntArrayE
      uwPrefD rtt = case c of
        MC.Reg -> SBB.addCountData rtt "DReg" (view DP.dReg)
        MC.Vote -> SBB.addCountData rtt "DVotes" (view DP.dVotes)
      weightedVotes ws = DP.weightedCountF ws (view DP.votesInRace) (view DP.votesInRaceW) (view DP.votesInRaceESS)
      votesWeighted ws = DP.weightedQtyF ws (view DP.votesInRace) (view DP.votesInRaceW) (view DP.votesInRaceESS)
      weightedReg ws = DP.weightedCountF ws (view DP.registered2p) (view DP.registered2pW) (view DP.registered2pESS)
      regWeighted ws = DP.weightedQtyF ws (view DP.registered2p) (view DP.registered2pW) (view DP.registered2pESS)
      rwAction :: forall rs md. (FC.ElemsOf rs DP.PrefDataR)
               => DP.WeightingStyle -> SMB.RowTypeTag (F.Record rs) -> SMB.StanBuilderM md gq TE.IntArrayE
      rwAction ws rtt = case c of
        MC.Reg -> SBB.addCountData rtt "Registered" (round . weightedReg ws)
        MC.Vote -> SBB.addCountData rtt "VotesInRace" (round . weightedVotes ws)
      rwPrefD :: forall rs md. (FC.ElemsOf rs DP.PrefDataR)
              =>  DP.WeightingStyle -> SMB.RowTypeTag (F.Record rs) -> SMB.StanBuilderM md gq TE.IntArrayE
      rwPrefD ws rtt = case c of
        MC.Reg -> SBB.addCountData rtt "DReg" (round . regWeighted ws (view DP.dRegW))
        MC.Vote -> SBB.addCountData rtt "DVotes" (round . votesWeighted ws (view DP.dVotesW))
      wAction :: forall rs md. (FC.ElemsOf rs DP.PrefDataR)
              =>  DP.WeightingStyle -> SMB.RowTypeTag (F.Record rs) -> SMB.StanBuilderM md gq TE.VectorE
      wAction ws rtt = case c of
        MC.Reg -> SBB.addRealData rtt "Registered" (Just 0) Nothing (weightedReg ws)
        MC.Vote -> SBB.addRealData rtt "VotesInRace" (Just 0) Nothing (weightedVotes ws)
      wPrefD :: forall rs md. (FC.ElemsOf rs DP.PrefDataR)
             =>  DP.WeightingStyle -> SMB.RowTypeTag (F.Record rs) -> SMB.StanBuilderM md gq TE.VectorE
      wPrefD ws rtt = case c of
        MC.Reg -> SBB.addRealData rtt "DReg" (Just 0) Nothing (regWeighted ws (view DP.dRegW))
        MC.Vote -> SBB.addRealData rtt "DVotes" (Just 0) Nothing (votesWeighted ws (view DP.dVotesW))
  case mc.mcSurveyAggregation of
    MC.UnweightedAggregation -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc uwAction uwPrefD
    MC.RoundedWeightedAggregation ws -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc (rwAction ws) (rwPrefD ws)
    MC.WeightedAggregation _ ws -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc (wAction ws) (wPrefD ws)

stdNormalDWA :: (TE.TypeOneOf t [TE.EReal, TE.ECVec, TE.ERVec], TE.GenSType t) => TE.DensityWithArgs t
stdNormalDWA = TE.DensityWithArgs SF.std_normal TNil

type GroupR = GT.StateAbbreviation ': DP.DCatsR

setupAlphaSum :: Maybe Text -> [Text] -> MC.Alphas -> SMB.StanBuilderM md gq (SG.AlphaByDataVecCW md gq)
setupAlphaSum prefixM states alphas = do
  let nStatesE = SMB.groupSizeE MC.stateG
      prefixed t = maybe t (<> "_" <> t) prefixM
      alphaNDS n t = TE.NamedDeclSpec (prefixed "a" <> t) $ TE.vectorSpec n []
      stdNormalBP nds =  DAG.UntransformedP nds [] TNil (\TNil m -> TE.addStmt $ TE.sample m SF.std_normal TNil)
      normalBP :: forall et . (TE.TypeOneOf et [TE.EReal, TE.ECVec, TE.ERVec], TE.GenSType et) => Double -> Double -> TE.NamedDeclSpec et -> DAG.BuildParameter et
      normalBP mean sd nds =  DAG.UntransformedP nds [] TNil (\TNil m -> TE.addStmt $ TE.sample m SF.normalS (TE.realE mean :> TE.realE sd :> TNil))
      defPrior :: forall et . (TE.TypeOneOf et [TE.EReal, TE.ECVec, TE.ERVec], TE.GenSType et) => TE.NamedDeclSpec et -> DAG.BuildParameter et
      defPrior = normalBP 0 2
      enumI :: Enum e => e -> Either Text Int
      enumI e = Right $ fromEnum e + 1
      enumS :: forall e . (Enum e, Bounded e) => Int
      enumS = length [(minBound :: e)..(maxBound :: e)]
      stateIndexMap = M.fromList $ zip states [1..]
      stateI s = maybe (Left $ "setupAlphaSum: " <> s <> " is missing from given list of states") Right $ M.lookup s stateIndexMap
      refSex = DT.Female
      refAge = DT.A5_45To64
      refEducation = DT.E4_HSGrad
      refRace = DT.R5_WhiteNonHispanic
--      stateS = M.size stateIndexMap
      ageAG :: SG.GroupAlpha (F.Record GroupR) TE.ECVec = SG.contramapGroupAlpha (view DT.age5C)
              $ SG.firstOrderAlphaDC MC.ageG enumI refAge (defPrior $ alphaNDS (SMB.groupSizeE MC.ageG `TE.minusE` TE.intE 1) "Age")
--      sexAG  :: SG.GroupAlpha (F.Record GroupR) TE.EReal = SG.contramapGroupAlpha (view DT.sexC)
--              $ SG.binaryAlpha prefixM MC.sexG ((\x -> realToFrac x - 0.5) . fromEnum) (defPrior $ TE.NamedDeclSpec (prefixed "aSex") $ TE.realSpec [])
      sexAG  :: SG.GroupAlpha (F.Record GroupR) TE.ECVec = SG.contramapGroupAlpha (view DT.sexC)
              $ SG.firstOrderAlphaDC MC.sexG enumI refSex (defPrior $ alphaNDS (SMB.groupSizeE MC.sexG `TE.minusE` TE.intE 1) "Sex")
      eduAG  :: SG.GroupAlpha (F.Record GroupR) TE.ECVec = SG.contramapGroupAlpha (view DT.education4C)
              $ SG.firstOrderAlphaDC MC.eduG enumI refEducation (defPrior $ alphaNDS (SMB.groupSizeE MC.eduG `TE.minusE` TE.intE 1) "Edu")
      raceAG  :: SG.GroupAlpha (F.Record GroupR) TE.ECVec = SG.contramapGroupAlpha (view DT.race5C)
               $ SG.firstOrderAlphaDC MC.raceG enumI refRace (defPrior $ alphaNDS (SMB.groupSizeE MC.raceG `TE.minusE` TE.intE 1) "Race")
  let zeroAG_C = do
        let nds = TE.NamedDeclSpec (prefixed "alpha0") $ TE.realSpec []
        pure $ SG.contramapGroupAlpha (view GT.stateAbbreviation) $ SG.zeroOrderAlpha $ normalBP 0 2 nds
  let stateAG_C = do
        muAlphaP <- DAG.simpleParameterWA
                    (TE.NamedDeclSpec (prefixed "muSt") $ TE.realSpec [])
                    stdNormalDWA
        sigmaAlphaP <-  DAG.simpleParameterWA
                      (TE.NamedDeclSpec (prefixed "sigmaSt") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                      stdNormalDWA
        let  aStBP_C :: DAG.Parameters [TE.EReal, TE.EReal] -> DAG.BuildParameter TE.ECVec
             aStBP_C hps = DAG.UntransformedP (alphaNDS (SMB.groupSizeE MC.stateG) "St") [] hps
                           $ \(muAlphaE :> sigmaAlphaE :> TNil) m
                             -> TE.addStmt $ TE.sample m SF.normalS (muAlphaE :> sigmaAlphaE :> TNil)
        pure $ SG.contramapGroupAlpha (view GT.stateAbbreviation) $ SG.firstOrderAlpha MC.stateG stateI (aStBP_C (muAlphaP :> sigmaAlphaP :> TNil))
  let stateAG_NC = do
        muAlphaP <- DAG.simpleParameterWA
                    (TE.NamedDeclSpec (prefixed "muSt") $ TE.realSpec [])
                    stdNormalDWA
        sigmaAlphaP <-  DAG.simpleParameterWA
                      (TE.NamedDeclSpec (prefixed "sigmaSt") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                      stdNormalDWA
        rawP <- DAG.simpleParameterWA
                (TE.NamedDeclSpec (prefixed "alphaST_raw") $ TE.vectorSpec (SMB.groupSizeE MC.stateG) [])
                stdNormalDWA
        let aStBP_NC = DAG.simpleTransformedP (alphaNDS (SMB.groupSizeE MC.stateG) "St") []
                       (muAlphaP :> sigmaAlphaP :> rawP :> TNil) DAG.TransformedParametersBlock
                       (\(mu :> s :> r :> TNil) -> DAG.DeclRHS $ mu `TE.plusE` (s `TE.timesE` r))
        pure $ SG.contramapGroupAlpha (view GT.stateAbbreviation) $ SG.firstOrderAlpha MC.stateG stateI aStBP_NC
  let ageSexAG = do
        sigmaAgeSex <- DAG.simpleParameterWA
                         (TE.NamedDeclSpec (prefixed "sigmaAgeSex") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                         stdNormalDWA
        let as_NDS = alphaNDS (SMB.groupSizeE MC.sexG `TE.timesE`  SMB.groupSizeE MC.ageG `TE.minusE` TE.intE 1) "AgeSex"
            as_BP ::  DAG.BuildParameter TE.ECVec
            as_BP = DAG.UntransformedP
                    as_NDS [] (sigmaAgeSex :> TNil)
                    $ \(sigmaE :> TNil) m -> TE.addStmt $ TE.sample m SF.normalS (TE.realE 0 :> sigmaE :> TNil)
        pure $ SG.contramapGroupAlpha (\r -> (r ^. DT.age5C, r ^. DT.sexC))
          $ SG.secondOrderAlphaDC prefixM MC.ageG (enumI, enumS @DT.Age5) MC.sexG (enumI, enumS @DT.Sex) (refAge, refSex) as_BP
  let sexEduAG = do
        sigmaSexEdu <- DAG.simpleParameterWA
                         (TE.NamedDeclSpec (prefixed "sigmaSexEdu") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                         stdNormalDWA
        let as_NDS = alphaNDS (SMB.groupSizeE MC.sexG `TE.timesE` SMB.groupSizeE MC.eduG `TE.minusE` TE.intE 1) "SexEdu"
            as_BP ::  DAG.BuildParameter TE.ECVec
            as_BP = DAG.UntransformedP
                    as_NDS [] (sigmaSexEdu :> TNil)
                    $ \(sigmaE :> TNil) m -> TE.addStmt $ TE.sample m SF.normalS (TE.realE 0 :> sigmaE :> TNil)
        pure $ SG.contramapGroupAlpha (\r -> (r ^. DT.sexC, r ^. DT.education4C))
          $ SG.secondOrderAlphaDC prefixM MC.sexG (enumI, enumS @DT.Sex) MC.eduG (enumI, enumS @DT.Education4) (refSex, refEducation) as_BP
  let sexRaceAG = do
        sigmaSexRace <- DAG.simpleParameterWA
                         (TE.NamedDeclSpec (prefixed "sigmaSexRace") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                         stdNormalDWA
        let as_NDS = alphaNDS (SMB.groupSizeE MC.sexG `TE.timesE` SMB.groupSizeE MC.raceG `TE.minusE` TE.intE 1) "SexRace"
            as_BP ::  DAG.BuildParameter TE.ECVec
            as_BP = DAG.UntransformedP
                    as_NDS [] (sigmaSexRace :> TNil)
                    $ \(sigmaE :> TNil) m -> TE.addStmt $ TE.sample m SF.normalS (TE.realE 0 :> sigmaE :> TNil)
        pure $ SG.contramapGroupAlpha (\r -> (r ^. DT.sexC, r ^. DT.race5C))
          $ SG.secondOrderAlphaDC prefixM MC.sexG (enumI, enumS @DT.Sex) MC.raceG (enumI, enumS @DT.Race5) (refSex, refRace) as_BP

  let ageEduAG = do
        sigmaAgeEdu <-  DAG.simpleParameterWA
                         (TE.NamedDeclSpec (prefixed "sigmaAgeEdu") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                         stdNormalDWA
        let aAE_NDS = alphaNDS (SMB.groupSizeE MC.ageG `TE.timesE` SMB.groupSizeE MC.eduG `TE.minusE` TE.intE 1) "AgeEdu"
            aAE_BP :: DAG.BuildParameter TE.ECVec
            aAE_BP = DAG.UntransformedP
                     aAE_NDS [] (sigmaAgeEdu :> TNil)
                     $ \(sigmaE :> TNil) m
                       -> TE.addStmt $ TE.sample m SF.normalS (TE.realE 0 :> sigmaE :> TNil)
        pure $ SG.contramapGroupAlpha (\r -> (r ^. DT.age5C, r ^. DT.education4C ))
          $ SG.secondOrderAlphaDC prefixM MC.ageG (enumI, enumS @DT.Age5) MC.eduG (enumI, enumS @DT.Education4) (refAge, refEducation) aAE_BP
      ageRaceAG = do
        sigmaAgeRace <-  DAG.simpleParameterWA
                         (TE.NamedDeclSpec (prefixed "sigmaAgeRace") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                         stdNormalDWA
        let aAR_NDS = alphaNDS (SMB.groupSizeE MC.ageG `TE.timesE` SMB.groupSizeE MC.raceG `TE.minusE` TE.intE 1) "AgeRace"
            aAR_BP :: DAG.BuildParameter TE.ECVec
            aAR_BP = DAG.UntransformedP
                     aAR_NDS [] (sigmaAgeRace :> TNil)
                     $ \(sigmaE :> TNil) m
                       -> TE.addStmt $ TE.sample m SF.normalS (TE.realE 0 :> sigmaE :> TNil)
        pure $ SG.contramapGroupAlpha (\r -> (r ^. DT.age5C, r ^. DT.race5C ))
          $ SG.secondOrderAlphaDC prefixM MC.ageG (enumI, enumS @DT.Age5) MC.raceG (enumI, enumS @DT.Race5) (refAge, refRace) aAR_BP
      eduRaceAG = do
        sigmaEduRace <-  DAG.simpleParameterWA
                         (TE.NamedDeclSpec (prefixed "sigmaEduRace") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                         stdNormalDWA
        let aER_NDS = alphaNDS (SMB.groupSizeE MC.eduG `TE.timesE` SMB.groupSizeE MC.raceG `TE.minusE` TE.intE 1) "EduRace"
            aER_BP :: DAG.BuildParameter TE.ECVec
            aER_BP = DAG.UntransformedP
                     aER_NDS [] (sigmaEduRace :> TNil)
                     $ \(sigmaE :> TNil) m
                       -> TE.addStmt $ TE.sample m SF.normalS (TE.realE 0 :> sigmaE :> TNil)
        pure $ SG.contramapGroupAlpha (\r -> (r ^. DT.education4C, r ^. DT.race5C ))
          $ SG.secondOrderAlphaDC prefixM MC.eduG (enumI, enumS @DT.Education4) MC.raceG (enumI, enumS @DT.Race5) (refEducation, refRace) aER_BP
      stateAgeAG = do
        sigmaStateAge <-  DAG.simpleParameterWA
                           (TE.NamedDeclSpec (prefixed "sigmaStateAge") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                           stdNormalDWA
        let ds = TE.matrixSpec (SMB.groupSizeE MC.stateG) (SMB.groupSizeE MC.ageG) []

            rawNDS = TE.NamedDeclSpec (prefixed "alpha_State_Age_raw") ds
        rawAlphaStateAgeP <- DAG.iidMatrixP rawNDS [] TNil SF.std_normal
        let aStA_NDS = TE.NamedDeclSpec (prefixed "alpha_State_Age") ds
        let aStA_BP :: DAG.BuildParameter TE.EMat
            aStA_BP = DAG.simpleTransformedP aStA_NDS [] (sigmaStateAge :> rawAlphaStateAgeP :> TNil)
                     DAG.TransformedParametersBlock
                     (\(s :> r :> TNil) -> DAG.DeclRHS $ s `TE.timesE` r)
        pure $ SG.contramapGroupAlpha (\r -> (r ^. GT.stateAbbreviation, r ^. DT.age5C))
          $ SG.secondOrderAlpha prefixM MC.stateG stateI MC.ageG enumI aStA_BP
      stateSexAG = do
        sigmaStateSex <-  DAG.simpleParameterWA
                           (TE.NamedDeclSpec (prefixed "sigmaStateSex") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                           stdNormalDWA
        let ds = TE.matrixSpec (SMB.groupSizeE MC.stateG) (SMB.groupSizeE MC.sexG) []
            rawNDS = TE.NamedDeclSpec (prefixed "alpha_State_Sex_raw") ds
        rawAlphaStateSexP <- DAG.iidMatrixP rawNDS [] TNil SF.std_normal
        let aStS_NDS = TE.NamedDeclSpec (prefixed "alpha_State_Sex") ds
        let aStS_BP :: DAG.BuildParameter TE.EMat
            aStS_BP = DAG.simpleTransformedP aStS_NDS [] (sigmaStateSex :> rawAlphaStateSexP :> TNil)
                     DAG.TransformedParametersBlock
                     (\(s :> r :> TNil) -> DAG.DeclRHS $ s `TE.timesE` r)
        pure $ SG.contramapGroupAlpha (\r -> (r ^. GT.stateAbbreviation, r ^. DT.sexC))
          $ SG.secondOrderAlpha prefixM MC.stateG stateI MC.sexG enumI aStS_BP
      stateEduAG = do
        sigmaStateEdu <-  DAG.simpleParameterWA
                           (TE.NamedDeclSpec (prefixed "sigmaStateEdu") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                           stdNormalDWA
        let ds = TE.matrixSpec (SMB.groupSizeE MC.stateG) (SMB.groupSizeE MC.eduG) []

            rawNDS = TE.NamedDeclSpec (prefixed "alpha_State_Edu_raw") ds
        rawAlphaStateEduP <- DAG.iidMatrixP rawNDS [] TNil SF.std_normal
        let aStE_NDS = TE.NamedDeclSpec (prefixed "alpha_State_Edu") ds
        let aStE_BP :: DAG.BuildParameter TE.EMat
            aStE_BP = DAG.simpleTransformedP aStE_NDS [] (sigmaStateEdu :> rawAlphaStateEduP :> TNil)
                     DAG.TransformedParametersBlock
                     (\(s :> r :> TNil) -> DAG.DeclRHS $ s `TE.timesE` r)
        pure $ SG.contramapGroupAlpha (\r -> (r ^. GT.stateAbbreviation, r ^. DT.education4C))
          $ SG.secondOrderAlpha prefixM MC.stateG stateI MC.eduG enumI aStE_BP
      stateRaceAG = do
        sigmaStateRace <-  DAG.simpleParameterWA
                           (TE.NamedDeclSpec (prefixed "sigmaStateRace") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                           stdNormalDWA
        let ds = TE.matrixSpec (SMB.groupSizeE MC.stateG) (SMB.groupSizeE MC.raceG) []

            rawNDS = TE.NamedDeclSpec (prefixed "alpha_State_Race_raw") ds
        rawAlphaStateRaceP <- DAG.iidMatrixP rawNDS [] TNil SF.std_normal
        let aStR_NDS = TE.NamedDeclSpec (prefixed "alpha_State_Race") ds
        let aStR_BP :: DAG.BuildParameter TE.EMat
--            aSR_BP sigma = DAG.iidMatrixBP aSR_NDS [] (DAG.given (TE.realE 0) :> sigma :> TNil) SF.normalS
            aStR_BP = DAG.simpleTransformedP aStR_NDS [] (sigmaStateRace :> rawAlphaStateRaceP :> TNil)
                     DAG.TransformedParametersBlock
                     (\(s :> r :> TNil) -> DAG.DeclRHS $ s `TE.timesE` r)
        pure $ SG.contramapGroupAlpha (\r -> (r ^. GT.stateAbbreviation, r ^. DT.race5C))
          $ SG.secondOrderAlpha prefixM MC.stateG stateI MC.raceG enumI aStR_BP

      stateEduRace :: SMB.StanBuilderM md gq (SG.GroupAlpha (F.Record GroupR) (TE.EArray1 TE.EMat))
      stateEduRace = do
        sigmaStateEduRaceP :: DAG.Parameter TE.EReal  <-  DAG.simpleParameterWA
                                                          (TE.NamedDeclSpec (prefixed "sigmaStateEduRace") $ TE.realSpec [TE.lowerM $ TE.realE 0])
                                                          stdNormalDWA
        let ds :: TE.DeclSpec (TE.EArray1 TE.EMat) = TE.array1Spec nStatesE $ TE.matrixSpec (SMB.groupSizeE MC.eduG) (SMB.groupSizeE MC.raceG) []
            aStER_NDS :: TE.NamedDeclSpec (TE.EArray1 TE.EMat) = TE.NamedDeclSpec (prefixed "alpha_State_Edu_Race") ds
            aStER_raw_NDS :: TE.NamedDeclSpec (TE.EArray1 TE.EMat) = TE.NamedDeclSpec (prefixed "alpha_State_Edu_Race_raw") ds
        rawAlphaStateEduRaceP :: DAG.Parameter (TE.EArray1 TE.EMat) <- DAG.addBuildParameter
          $ DAG.UntransformedP  aStER_raw_NDS [] TNil
          $ \_ t -> TE.addStmt
                    ( TE.for "s" (TE.SpecificNumbered (TE.intE 1) nStatesE)
                      $ \s -> [SF.toVec (t `TE.at` s) `TE.sampleW` stdNormalDWA]
                    )
        let aStER_BP :: DAG.BuildParameter (TE.EArray1 TE.EMat)
            aStER_BP =  DAG.simpleTransformedP aStER_NDS [] (sigmaStateEduRaceP :> rawAlphaStateEduRaceP :> TNil)
                        DAG.TransformedParametersBlock
                        (\(sigma :> raw :> TNil) -> DAG.DeclCodeF
                          $ \t -> TE.addStmt
                                  $ TE.for "s" (TE.SpecificNumbered (TE.intE 1) nStatesE)
                                  $ \s -> [(t `TE.at` s) `TE.assign` (sigma `TE.timesE` (raw `TE.at` s))]
                        )
        pure $ SG.contramapGroupAlpha (\r -> (r ^. GT.stateAbbreviation, r ^. DT.education4C, r ^. DT.race5C))
          $ SG.thirdOrderAlpha prefixM MC.stateG stateI MC.eduG enumI MC.raceG enumI aStER_BP
  case alphas of
    MC.A -> do
      zeroAG <- zeroAG_C
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> ageAG :> TNil)
    MC.S -> do
      zeroAG <- zeroAG_C
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> sexAG :> TNil)
    MC.E -> do
      zeroAG <- zeroAG_C
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> eduAG :> TNil)
    MC.R -> do
      zeroAG <- zeroAG_C
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> raceAG :> TNil)
    MC.A_S -> do
      zeroAG <- zeroAG_C
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> ageAG :> sexAG :> TNil)
    MC.A_E -> do
      zeroAG <- zeroAG_C
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> ageAG :> eduAG :> TNil)
    MC.A_S_AS -> do
      zeroAG <- zeroAG_C
      asAG <- ageSexAG
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> ageAG :> sexAG :> asAG :> TNil)
    MC.AS -> do
      zeroAG <- zeroAG_C
      asAG <- ageSexAG
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> asAG :> TNil)
    MC.S_E -> do
      zeroAG <- zeroAG_C
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> sexAG :> eduAG :> TNil)
    MC.S_R -> do
      zeroAG <- zeroAG_C
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> sexAG :> raceAG :> TNil)
    MC.A_E_AE -> do
      zeroAG <- zeroAG_C
      aeAG <- ageEduAG
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> ageAG :> eduAG :> aeAG :> TNil)
    MC.A_S_E_R -> do
      zeroAG <- zeroAG_C
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> ageAG :> sexAG :> eduAG :> raceAG :> TNil)
    MC.A_S_E_R_AS_AE_AR_SE_SR_ER -> do
      zeroAG <- zeroAG_C
      asAG <- ageSexAG
      aeAG <- ageEduAG
      arAG <- ageRaceAG
      seAG <- sexEduAG
      srAG <- sexRaceAG
      erAG <- eduRaceAG
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (zeroAG :> ageAG :> sexAG :> eduAG :> raceAG :>
                                                     asAG :> aeAG :> arAG :> seAG :> srAG :> erAG :> TNil)
    MC.St_A_S_E_R -> do
      stAG <- stateAG_C
      SG.setupAlphaSum @_ @_ @_ @(F.Record GroupR) (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> TNil)
    MC.St_A_S_E_R_ER -> do
      stAG <- stateAG_C
      erAG <- eduRaceAG
      SG.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> erAG :> TNil)
    MC.St_A_S_E_R_AR -> do
      stAG <- stateAG_C
      arAG <- ageRaceAG
      SG.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> arAG :> TNil)
    MC.St_A_S_E_R_StR -> do
      stAG <- stateAG_C
      srAG <- stateRaceAG
      SG.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> srAG :> TNil)
    MC.St_A_S_E_R_StA -> do
      stAG <- stateAG_C
      saAG <- stateAgeAG
      SG.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> saAG :> TNil)
    MC.St_A_S_E_R_ER_StR -> do
      stAG <- stateAG_C
      srAG <- stateRaceAG
      erAG <- eduRaceAG
      SG.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> erAG :> srAG :> TNil)
    MC.St_A_S_E_R_AE_AR_ER_StR -> do
      stAG <- stateAG_NC
      aeAG <- ageEduAG
      arAG <- ageRaceAG
      srAG <- stateRaceAG
      erAG <- eduRaceAG
      SG.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> aeAG :> arAG :> erAG :> srAG :> TNil)
    MC.St_A_S_E_R_ER_StE_StR -> do
      stAG <- stateAG_C
      srAG <- stateRaceAG
      seAG <- stateEduAG
      erAG <- eduRaceAG
      SG.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> erAG :> seAG :> srAG :> TNil)
    MC.St_A_S_E_R_ER_StA_StE_StR -> do
      stAG <- stateAG_C
      saAG <- stateAgeAG
      seAG <- stateEduAG
      srAG <- stateRaceAG
      erAG <- eduRaceAG
      SG.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> erAG :> saAG :> seAG :> srAG :> TNil)
    MC.St_A_S_E_R_ER_StR_StER -> do
      stAG <- stateAG_C
      srAG <- stateRaceAG
      erAG <- eduRaceAG
      serAG <- stateEduRace
      SG.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> erAG :> srAG :> serAG :> TNil)
    MC.St_A_S_E_R_StA_StS_StE_StR_AS_AE_AR_SE_SR_ER_StER -> do
      stAG <- stateAG_NC
      saAG <- stateAgeAG
      ssAG <- stateSexAG
      seAG <- stateEduAG
      srAG <- stateRaceAG
      asAG <- ageSexAG
      aeAG <- ageEduAG
      arAG <- ageRaceAG
      sxeAG <- sexEduAG
      sxrAG <- sexRaceAG
      erAG <- eduRaceAG
      serAG <- stateEduRace
      SG.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG
                        :> saAG :> ssAG :> seAG :> srAG
                        :> asAG :> aeAG :> arAG :> sxeAG :> sxrAG :> erAG
                        :> serAG :> TNil)

setupBeta :: Maybe Text -> MC.ModelConfig b -> SMB.StanBuilderM md gq (Maybe TE.VectorE)
setupBeta prefixM mc = do
  let dmr = mc.mcDesignMatrixRow
      prefixed t = maybe t (<> "_" <> t) prefixM
      (_, nCovariatesE) = DM.designMatrixColDimBinding dmr Nothing
  betaM <- if DM.rowLength dmr > 0 then
               (Just . DAG.parameterExpr)
               <$> DAG.simpleParameterWA
               (TE.NamedDeclSpec (prefixed "beta") $ TE.vectorSpec nCovariatesE [])
               stdNormalDWA
             else pure $ Nothing
  pure betaM

data ParameterSetup md gq = LogitSetup (SG.AlphaByDataVecCW md gq) (Maybe TE.VectorE)

hasBeta :: ParameterSetup md gq -> Bool
hasBeta (LogitSetup _ mb) = isJust mb

setupParameters :: Maybe Text -> [Text] -> MC.ModelConfig b -> SMB.StanBuilderM md gq (ParameterSetup md gq)
setupParameters prefixM states mc = do
  as <- setupAlphaSum prefixM states mc.mcAlphas
  bs <- setupBeta prefixM mc
  pure $ LogitSetup as bs

logitProbCW :: ParameterSetup md gq -> SMB.RowTypeTag a -> TE.MatrixE -> SMB.StanBuilderM md gq (TE.CodeWriter TE.VectorE)
logitProbCW ps rtt covM =
  case ps of
    LogitSetup (SG.AlphaByDataVecCW f) mBeta -> do
      alphaSumCW <- f rtt
      case mBeta of
        Nothing -> pure alphaSumCW
        Just beta -> pure $ do
          alphaSum <- alphaSumCW
          pure $ alphaSum `TE.plusE` (covM `TE.timesE` beta)


probabilitiesCW :: ParameterSetup md gq -> SMB.RowTypeTag a -> TE.MatrixE -> SMB.StanBuilderM md gq (TE.CodeWriter TE.VectorE)
probabilitiesCW ps rtt covM = fmap (\x -> TE.functionE SF.inv_logit (x :> TNil)) <$> (logitProbCW ps rtt covM)

{-
stateTargetsModel :: Maybe Text
                  -> ParameterSetup md gq
                  -> MC.CovariatesAndCounts a b
                  -> MC.StateTargetsData td
                  -> Maybe (TE.MatrixE -> TE.StanName -> SMB.StanBuilderM md gq TE.MatrixE)
                  -> Maybe TE.MatrixE
                  -> SMB.StanBuilderM md gq ()
stateTargetsModel prefixM ps cc std cM rM = do
  let prefixed t = maybe t (<>  "_" <> t) prefixM
  (dmACS, acsNByState) <- MC.stateTargetsTD prefixM cc std cM rM
  let acsStateIndex = SMB.byGroupIndexE std.stdACSTag MC.stateG
      toVec x = TE.functionE SF.to_vector (x :> TNil)
  pCW <- probabilitiesCW ps std.stdACSTag dmACS
  acsPS <- SBB.postStratifiedParameterF False SMB.SBTransformedParameters
           (Just $ prefixed "psByState") std.stdACSTag MC.stateG acsStateIndex (TE.NoCW $ toVec std.stdACSWgts) pCW Nothing
  let normalDWA = TE.DensityWithArgs SF.normal (TE.realE 1 :> TE.realE 4 :> TNil)
  sigmaTargetsP <-  DAG.simpleParameterWA
                    (TE.NamedDeclSpec (prefixed "sigmaTgt") $ TE.realSpec [TE.lowerM $ TE.realE 1])
                    normalDWA
  SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ do
    let eMult = TE.binaryOpE (TEO.SElementWise TEO.SMultiply)
        eDiv = TE.binaryOpE (TEO.SElementWise TEO.SDivide)
        tP = TE.indexE TEI.s0 (SMB.byGroupIndexE std.stdTargetTypeTag MC.stateG) std.stdTarget
        sd1 = tP `eMult` (TE.realE 1 `TE.minusE` tP) `eDiv` toVec acsNByState
        sd = DAG.parameterExpr sigmaTargetsP `TE.timesE` TE.functionE SF.sqrt (sd1 :> TNil)
    TE.addStmt $ TE.sample acsPS SF.normal (tP :> sd :> TNil)
-}
{-
postStratificationWeights :: forall k md . (Typeable psRow)
                          => SMB.RowTypeTag psRow -> (psRow -> Int) -> SMB.StanBuilderM md (DP.PSData k) TE.IntArrayE
postStratificationWeights psDataTag psWgt = SBB.addCountData psDataTag "PSWgts" psWgt
-}
postStratificationProbsCW :: forall l psRow md gq . (Typeable psRow
                                                 --                                             , F.ElemOf (DP.PSDataR k) DT.PopCount
                                                 --                                             , DP.LPredictorsR F.⊆ DP.PSDataR k
                                                 )
                          => SMB.RowTypeTag psRow
                          -> (psRow -> F.Record DP.LPredictorsR)
                          -> Text
                          -> ParameterSetup md gq
                          -> DM.DesignMatrixRow (F.Record DP.LPredictorsR)
                          -> Maybe (TE.MatrixE -> TE.StanName -> SMB.StanBuilderM md gq TE.MatrixE)
                          -> Maybe TE.MatrixE
                          -> SMB.GroupTypeTag l
                          -> SMB.StanBuilderM md gq (TE.CodeWriter TE.VectorE)
postStratificationProbsCW psRowTag psRowPredictors prefix ps dmr cM rM _gtt = do
  let prefixed t = prefix <> "_" <> t
  let nCovariates = DM.rowLength dmr
  psCovariates <-  if nCovariates > 0
                   then DM.addDesignMatrix psRowTag (contramap psRowPredictors dmr) Nothing
                   else pure $ TE.namedE (prefixed "ERROR") TE.SMat -- this shouldn't show up in stan code at all
  dmPS' <- case cM of
    Nothing -> pure psCovariates
    Just c -> c psCovariates $ prefixed "dmPS_Centered"
  dmPS <- case rM of
    Nothing -> pure dmPS'
    Just r -> SMB.inBlock SMB.SBTransformedDataGQ $ SMB.addFromCodeWriter $ do
      let rowsE = SMB.dataSetSizeE psRowTag
          colsE = SMB.mrfdColumnsE $ DM.matrixFromRowData dmr Nothing
      TE.declareRHSNW (TE.NamedDeclSpec (prefixed "dmPS_QR") $ TE.matrixSpec rowsE colsE []) $ dmPS' `TE.timesE` r
  probabilitiesCW ps psRowTag dmPS


postStratifyOne :: forall l psRow md gq . (Typeable psRow
--                                   , F.ElemOf (DP.PSDataR k) DT.PopCount
--                                   , DP.LPredictorsR F.⊆ DP.PSDataR k
                                   )
                => SMB.RowTypeTag psRow
                -> (psRow -> Int)
                -> (psRow -> F.Record DP.LPredictorsR)
                -> Text
                -> ParameterSetup md gq
                -> Maybe (TE.CodeWriter (TE.VectorE -> TE.VectorE))
                -> Maybe (TE.CodeWriter (TE.IntArrayE -> TE.IntArrayE))
                -> DM.DesignMatrixRow (F.Record DP.LPredictorsR)
                -> Maybe (TE.MatrixE -> TE.StanName -> SMB.StanBuilderM md gq TE.MatrixE)
                -> Maybe TE.MatrixE
                -> SMB.GroupTypeTag l
                -> SMB.StanBuilderM md gq TE.VectorE
postStratifyOne psRowTag psRowWgt psRowPredictors prefix ps modPF modWgtsF dmr cM rM gtt = do
--  psDataTag <- SMB.dataSetTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData"
  psWgts' <- SBB.addCountData psRowTag "PSWgts" psRowWgt --postStratificationWeights psRowTag psWgt
  let psWgts = SF.toVec <$> maybe (TE.NoCW psWgts') (\mwF -> TE.NeedsCW $ mwF <*> pure psWgts') modWgtsF

  psCW' <- postStratificationProbsCW psRowTag psRowPredictors prefix ps dmr cM rM gtt
  let psCW = maybe psCW' (<*> psCW') modPF

  let prefixed t = prefix <> "_" <> t
      psDataGrpIndex = SMB.byGroupIndexE psRowTag gtt
  SBB.postStratifiedParameterF False SMB.SBGeneratedQuantities (Just $ prefixed "byGrp") psRowTag gtt psDataGrpIndex psWgts psCW Nothing


--data RunConfig l = RunConfig { rcIncludePPCheck :: Bool, rcIncludeLL :: Bool, rcIncludeDMSplits :: Bool, rcTurnoutPS :: Maybe (SMB.GroupTypeTag (F.Record l)) }

data Components md gq =
  Components
  {
    coModel :: SMB.StanBuilderM md gq ()
  , coLL :: SMB.StanBuilderM md gq ()
  , coPP :: SMB.StanBuilderM md gq ()
  , coCenterCovariatesF :: SC.InputDataType -> TE.MatrixE -> TE.StanName -> SMB.StanBuilderM md gq TE.MatrixE
  }

components :: Maybe Text
           -> MC.CovariatesAndCounts r b
           -> ParameterSetup md gq
           -> MC.SurveyAggregation b
           -> SMB.StanBuilderM md gq (Components md gq)
components prefixM cc paramSetup sa = do
  let prefixed t = maybe t (<> "_" <> t) prefixM
      nRowsE = SMB.dataSetSizeE cc.ccSurveyDataTag
  (covariatesM, centerF) <- case hasBeta paramSetup of
    True -> do
      (centeredCovariatesE, centerF) <- DM.centerDataMatrix DM.DMCenterOnly cc.ccCovariates Nothing (prefixed "DM")
      pure (centeredCovariatesE, centerF)
    False -> pure (TE.namedE (prefixed "ERROR") TE.SMat, \_ x _ -> pure x)
  let n = cc.ccBinomialData.bdTrials
      k = cc.ccBinomialData.bdSuccesses
      toArray x = TE.functionE SF.to_array_1d (x :> TNil)
  lpCW <- logitProbCW paramSetup cc.ccSurveyDataTag covariatesM
  case sa of
    MC.UnweightedAggregation -> do
      let ssf e lp =  SMB.familySample (SMD.binomialLogitDist @TE.EIntArray) e (n :> lp :> TNil)
          modelCo = SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter (lpCW >>= TE.addStmt . ssf k)
          ppCo =  SBB.generatePosteriorPredictionV
                  (TE.NamedDeclSpec (prefixed "pred") $ TE.array1Spec nRowsE $ TE.intSpec [])
                  SMD.binomialLogitDist
                  (TE.NeedsCW $ lpCW >>= \x -> pure (n :> x :> TNil))
          llCo = SBB.generateLogLikelihood cc.ccSurveyDataTag SMD.binomialLogitDist
                 ((\x -> (\nE -> n `TE.at` nE :> x `TE.at` nE :> TNil)) <$> lpCW)
                 (pure $ (k `TE.at`))
      pure $ Components modelCo llCo (void ppCo) centerF
    MC.RoundedWeightedAggregation _ -> do
      let ssf e lp =  SMB.familySample (SMD.binomialLogitDist @TE.EIntArray) e (n :> lp :> TNil)
          modelCo = SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter (lpCW >>= TE.addStmt . ssf k)
          ppCo =  SBB.generatePosteriorPredictionV
                  (TE.NamedDeclSpec (prefixed "pred") $ TE.array1Spec nRowsE $ TE.intSpec [])
                  SMD.binomialLogitDist
                  (TE.NeedsCW $ lpCW >>= \x -> pure (n :> x :> TNil))
          llCo = SBB.generateLogLikelihood cc.ccSurveyDataTag SMD.binomialLogitDist
                 ((\x -> (\nE -> n `TE.at` nE :> x `TE.at` nE :> TNil)) <$> lpCW)
                 (pure $ (k `TE.at`))
      pure $ Components modelCo llCo (void ppCo) centerF
    MC.WeightedAggregation MC.ContinuousBinomial _ -> do
      realBinomialLogitDistV <- SMD.realBinomialLogitDistM @TE.ECVec
      realBinomialLogitDistS <- SMD.realBinomialLogitDistSM
      let ssf e lp = SMB.familySample realBinomialLogitDistV e (n :> lp :> TNil)
          modelCo = SMB.inBlock SMB.SBModel . SMB.addFromCodeWriter $ lpCW >>= TE.addStmt . ssf k
          ppCo = SBB.generatePosteriorPredictionV'
                 (TE.NamedDeclSpec (prefixed "pred") $ TE.array1Spec nRowsE $ TE.realSpec [])
                 realBinomialLogitDistV
                 (TE.NeedsCW $ lpCW >>= \x -> pure (n :> x :> TNil))
                 toArray
          llCo = SBB.generateLogLikelihood cc.ccSurveyDataTag realBinomialLogitDistS
                 ((\x -> (\nE -> n `TE.at` nE :> x `TE.at` nE :> TNil)) <$> lpCW)
                 (pure $ (k `TE.at`))
      pure $ Components modelCo llCo (void ppCo) centerF
    MC.WeightedAggregation MC.BetaProportion _ -> do
      let eltDivide = TE.binaryOpE (TEO.SElementWise TEO.SDivide)
          inv_logit x = TE.functionE SF.inv_logit (x :> TNil)
          muKappa lp = do
            mu <- TE.declareRHSNW (TE.NamedDeclSpec "mu" $ TE.vectorSpec nRowsE []) $ inv_logit lp
            kappa <- TE.declareRHSNW (TE.NamedDeclSpec "kappa" $ TE.vectorSpec nRowsE []) $ SF.toVec n
            pure (mu :> kappa :> TNil)
      th <- SMB.inBlock SMB.SBTransformedData $ SMB.addFromCodeWriter $ do
        let vecSpec = TE.vectorSpec nRowsE []
            vecOf x = TE.functionE SF.rep_vector (TE.realE x :> nRowsE :> TNil)
            vecMax v1 v2 = TE.functionE SF.fmax (v1 :> v2 :> TNil)
            vecMin v1 v2 = TE.functionE SF.fmin (v1 :> v2 :> TNil)
        TE.declareRHSNW (TE.NamedDeclSpec (prefixed "th") vecSpec) $ vecMax (vecOf 0.0001) $ vecMin (vecOf 0.9999) $ k `eltDivide` n
      let modelCo = SMB.inBlock SMB.SBModel $ SMB.addScopedFromCodeWriter
                    $ lpCW >>= muKappa >>= TE.addStmt . SMB.familySample SMD.betaProportionDist th
          ppCo =  SBB.generatePosteriorPredictionV'
                  (TE.NamedDeclSpec (prefixed "predR") $ TE.array1Spec nRowsE $ TE.realSpec [])
                  SMD.betaProportionDist
                  (TE.NeedsCW $ lpCW >>= muKappa)
                  toArray
          llCo = SBB.generateLogLikelihood cc.ccSurveyDataTag SMD.betaProportionDist
                 (lpCW >>= muKappa >>= (\(mu :> kappa :> TNil) -> (pure $ \nE -> mu `TE.at` nE :> kappa `TE.at` nE :> TNil)))
                 (pure $ (th `TE.at`))
      pure $ Components modelCo llCo (void ppCo) centerF

-- not returning anything for now
model :: forall k lk l a b .
         (Typeable (DP.PSDataR k)
         , F.ElemOf (DP.PSDataR k) DT.PopCount
         , DP.LPredictorsR F.⊆ DP.PSDataR k
         )
      => MC.RunConfig l
      -> Config a b
      -> [Text]
      -> SMB.StanBuilderM (DP.ModelData lk) (DP.PSData k) ()
model rc c states = case c of
  ActionOnly cat actionConfig@(MC.ActionConfig _ mc) -> do
    mData <- actionModelData cat actionConfig
    paramSetup <- setupParameters Nothing states mc
    (Components modelM llM ppM centerF) <- components Nothing (MC.covariatesAndCounts mData) paramSetup mc.mcSurveyAggregation
    modelM
    when rc.rcIncludePPCheck $ void ppM
    when rc.rcIncludeLL llM
    case rc.rcPS of
      Nothing -> pure ()
      Just gtt -> do
        let actionLabel = case cat of
              MC.Reg -> "R" -- registration
              MC.Vote -> "T" --Turnout
        psRowTag <- SMB.dataSetTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData"
        postStratifyOne psRowTag (view DT.popCount) F.rcast actionLabel paramSetup Nothing Nothing mc.mcDesignMatrixRow (Just $ centerF SC.GQData) Nothing gtt >> pure ()

  PrefOnly cat prefConfig@(MC.PrefConfig _ mc) -> do
    mData <- prefModelData cat prefConfig
    paramSetup <- setupParameters Nothing states mc
    (Components modelM llM ppM centerF) <- components Nothing (MC.covariatesAndCounts mData) paramSetup mc.mcSurveyAggregation
    modelM
    when rc.rcIncludePPCheck $ void ppM
    when rc.rcIncludeLL llM
    case rc.rcPS of
      Nothing -> pure ()
      Just gtt -> do
        let prefLabel = case cat of
              MC.Reg -> "RP" -- registration
              MC.Vote -> "P" --Turnout
        psRowTag <- SMB.dataSetTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData"
        postStratifyOne psRowTag (view DT.popCount) F.rcast prefLabel paramSetup Nothing Nothing mc.mcDesignMatrixRow (Just $ centerF SC.GQData) Nothing gtt >> pure ()

  ActionAndPref cat aConfig@(MC.ActionConfig _ aMC) pConfig@(MC.PrefConfig _ pMC) -> do
    let (actionLabel, prefLabel, psLabel) = case cat of
                                     MC.Reg -> ("R", "PR", "RDVS") -- Registration
                                     MC.Vote -> ("T", "P", "DVS") -- Votes
    aData <- actionModelData cat aConfig
    pData <- prefModelData cat pConfig
    aParamS <- setupParameters (Just actionLabel) states aMC
    pParamS <- setupParameters (Just prefLabel) states pMC
    (Components aModelM _ _ aCenterF) <- components (Just actionLabel) (MC.covariatesAndCounts aData) aParamS aMC.mcSurveyAggregation
    (Components pModelM _ _ pCenterF) <- components (Just prefLabel) (MC.covariatesAndCounts pData) pParamS pMC.mcSurveyAggregation
    aModelM
    pModelM
    case rc.rcPS of
      Nothing -> pure ()
      Just gtt -> do
        psRowTag <- SMB.dataSetTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData"
        psWgts <- SBB.addCountData psRowTag "PSWgts" (view DT.popCount) --postStratificationWeights psRowTag (view )
        aProbsCW <- postStratificationProbsCW psRowTag F.rcast actionLabel aParamS aMC.mcDesignMatrixRow (Just $ aCenterF SC.GQData) Nothing gtt
        pProbsCW <- postStratificationProbsCW psRowTag F.rcast prefLabel pParamS pMC.mcDesignMatrixRow (Just $ pCenterF SC.GQData) Nothing gtt
        let psDataGrpIndex = SMB.byGroupIndexE psRowTag gtt
            eltMultiply = TE.binaryOpE (TEO.SElementWise TEO.SMultiply)
            wgtsMCW = TE.NeedsCW $ fmap (`eltMultiply` SF.toVec psWgts) aProbsCW
        SBB.postStratifiedParameterF False SMB.SBGeneratedQuantities (Just $ psLabel <> "_byGrp") psRowTag gtt psDataGrpIndex wgtsMCW pProbsCW Nothing >> pure ()


type ElectionModelC l k lk =
  (
    l F.⊆ DP.PSDataR k
  , F.ElemOf (DP.PSDataR k) DT.PopCount
  , DP.LPredictorsR F.⊆ DP.PSDataR k
  , V.RMap l
  , Ord (F.Record l)
  , FS.RecFlat l
  , Typeable (DP.PSDataR k)
  , F.ElemOf (DP.PSDataR k) GT.StateAbbreviation
  , F.ElemOf (DP.CESByR lk) GT.StateAbbreviation
  , DP.DCatsR F.⊆ DP.PSDataR k
  , DP.DCatsR F.⊆ DP.CESByR lk
  , DP.LPredictorsR F.⊆ DP.CESByR lk
  , Show (F.Record l)
  , Typeable l
  , Typeable (DP.CESByR lk)
  , V.RMap (DP.PSDataR k)
  , V.RecordToList (DP.PSDataR k)
  , V.ReifyConstraint Show F.ElField (DP.PSDataR k)
  )

runModel :: forall l k lk r a b .
            (K.KnitEffects r
            , BRCC.CacheEffects r
            , ElectionModelC l k lk
            , FSI.RecVec (DP.CESByR lk)
            , FC.ElemsOf (DP.CESByR lk) [DP.Registered2p, DP.VotesInRace]
            , FSI.RecVec (DP.PSDataR k)
            , F.ElemOf (DP.PSDataR k) DT.PopCount
            )
         => Either Text Text
         -> Text
         -> Text
         -> MC.RunConfig l
         -> Config a b
         -> K.ActionWithCacheTime r (DP.ModelData lk)
         -> K.ActionWithCacheTime r (DP.PSData k)
         -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval, Maybe ModelParameters))
runModel modelDirE modelName gqName runConfig config modelData_C psData_C = do
  let dataName = configText config
  stanDir <- K.liftKnit MST.stanDir >>= K.knitMaybe "runModel: empty stanDir!" . BRCC.insureFinalSlash
  let runnerInputNames = SC.RunnerInputNames
                         (stanDir <> modelName <> "/")
                         (configText config)
                         (Just $ SC.GQNames "GQ" (dataName <> "_" <> gqName))
                         dataName
--  modelData <- K.ignoreCacheTime modelData_C
  states <- S.toList . FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) . DP.cesData <$> K.ignoreCacheTime modelData_C
  psKeys <- S.toList . FL.fold (FL.premap (F.rcast @l) FL.set) . DP.unPSData <$> K.ignoreCacheTime psData_C
  (dw, code) <- SMR.dataWranglerAndCode modelData_C psData_C
                (groupBuilder config states psKeys)
                (model runConfig config states)
-- HERE
  let datSuffix = SC.rinData runnerInputNames
      jsonData t = "jsonData_" <> datSuffix <> "$" <> t
      rSuffix = SC.rinModel runnerInputNames <> "_" <> datSuffix
      (aFieldName, pFieldName, aNum, aDenom, pNum, pDenom) = case configModelCategory config of
        MC.Reg -> ("Registered", "DReg" :: Text, jsonData "Registered", jsonData "Surveyed", jsonData "DReg", jsonData "Registered")
        MC.Vote -> ("Turnout", "DVotes", jsonData "Voted", jsonData "Surveyed", jsonData "DVotes", jsonData "VotesInRace")
      unwraps = case config of
        ActionOnly _  (MC.ActionConfig _ mc) -> case mc.mcSurveyAggregation of
          MC.WeightedAggregation MC.BetaProportion _ -> [SR.UnwrapExpr (aNum <> " / " <> aDenom) ("y" <> aFieldName <> "Rate_" <> rSuffix)]
          _ -> [SR.UnwrapNamed aFieldName ("y" <> aFieldName <> "_" <> rSuffix)]
        PrefOnly _  (MC.PrefConfig _ mc) -> case mc.mcSurveyAggregation of
          MC.WeightedAggregation MC.BetaProportion _ -> [SR.UnwrapExpr (pNum <> " / " <> pDenom) ("y" <> pFieldName <> "Rate_" <> rSuffix)]
          _ -> [SR.UnwrapNamed pFieldName ("y" <> pFieldName <> "_" <> rSuffix)]
        ActionAndPref _  _ _  -> [] -- what is a PP check for this combo case??
  res_C <- SMR.runModel' @BRCC.SerializerC @BRCC.CacheData
           modelDirE
           (Right runnerInputNames)
           Nothing
           dw
           code
           (modelResultAction config runConfig) --SC.DoNothing -- (stateModelResultAction mcWithId dmr)
           (SMR.Both unwraps) --(SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
           modelData_C
           psData_C
  K.logLE K.Info $ modelName <> " run complete."
  pure res_C

-- for now this just carries beta *and* assumes beta is first-order, does not interact
-- with other predictors.
data ModelParameters =
  ModelParameters
  {
    mpBetaSI :: VU.Vector (Double, Double)
  } deriving stock (Generic)


applyBetas :: VU.Vector (Double, Double) -> VU.Vector Double -> Double
applyBetas vSI vX = VU.sum $ VU.zipWith (\(s, i) x -> s * (x - i)) vSI vX

adjustPredictionsForDensity :: (F.ElemOf rs DT.PWPopPerSqMile, DP.LPredictorsR F.⊆ rs)
                            => (F.Record rs -> Double)
                            -> (Double -> F.Record rs -> F.Record rs)
                            -> ModelParameters
                            -> DM.DesignMatrixRow (F.Record DP.LPredictorsR)
                            -> F.Record rs
                            -> F.Record rs
adjustPredictionsForDensity getP setP mp dmr row = setP p' row
  where
    g x = 1 / (1 + Numeric.exp (negate x))
    f x y |  x == 0 = 0
          |  x == 1 = 1
          |  otherwise = g $ Numeric.log (x / (1 - x)) + y
    p = getP row
--    d = row ^. DT.pWPopPerSqMile
    lps = DM.designMatrixRowF dmr (F.rcast row)
    p' = f p $ applyBetas mp.mpBetaSI lps


deriving anyclass instance Flat.Flat ModelParameters

actionSurveyA :: Config a b -> Maybe (MC.ActionSurvey a)
actionSurveyA (ActionOnly _ (MC.ActionConfig ts _)) = Just ts
actionSurveyA (PrefOnly _ (MC.PrefConfig sp _)) = Just $ MC.CESSurvey sp
actionSurveyA (ActionAndPref _ (MC.ActionConfig _ _) _) = Nothing

dmrA :: Config a b -> Maybe (DM.DesignMatrixRow (F.Record DP.LPredictorsR))
dmrA (ActionOnly _ (MC.ActionConfig _ (MC.ModelConfig _ _ dmr))) = Just dmr
dmrA (PrefOnly _ (MC.PrefConfig _ (MC.ModelConfig _ _ dmr))) = Just dmr
dmrA (ActionAndPref _ _ _) = Nothing

--NB: parsed summary data has stan indexing, i.e., Arrays start at 1.
--NB: Will return no prediction (Nothing) for "both" model for now. Might eventually return both predictions?
modelResultAction :: forall k lk l r a b .
                     (Ord (F.Record l)
                     , K.KnitEffects r
                     , Typeable (DP.PSDataR k)
                     , Typeable l
                     , DP.LPredictorsR F.⊆ DP.CESByR lk
                     )
                  => Config a b
                  -> MC.RunConfig l
                  -> SC.ResultAction r (DP.ModelData lk) (DP.PSData k) SMB.DataSetGroupIntMaps () (MC.PSMap l MT.ConfidenceInterval, Maybe ModelParameters)
modelResultAction config runConfig = SC.UseSummary f where
  f summary _ modelDataAndIndexes_C gqDataAndIndexes_CM = do
    (modelData, _) <- K.ignoreCacheTime modelDataAndIndexes_C
     -- compute means of predictors because model was zero-centered in them
    let mdMeansFld :: DP.LPredictorsR F.⊆ rs
                   => DM.DesignMatrixRow (F.Record DP.LPredictorsR) -> FL.Fold (F.Record rs) [Double]
        mdMeansFld dmr =
          let  covariates = DM.designMatrixRowF $ contramap F.rcast dmr
               nPredictors = DM.rowLength dmr
          in FL.premap (VU.toList . covariates)
             $ traverse (\n -> FL.premap (List.!! n) FL.mean) [0..(nPredictors - 1)]
        mdMeansLM :: MC.ActionSurvey a -> DM.DesignMatrixRow (F.Record DP.LPredictorsR) -> [Double]
        mdMeansLM as dmr = case as of
          MC.CESSurvey _ -> FL.fold (FL.premap (F.rcast @DP.LPredictorsR) $ mdMeansFld dmr) $ DP.cesData modelData
          MC.CPSSurvey -> FL.fold (FL.premap (F.rcast @DP.LPredictorsR) $ mdMeansFld dmr) $ DP.cpsData modelData
        getVector n = K.knitEither $ SP.getVector . fmap CS.mean <$> SP.parse1D n (CS.paramStats summary)
        betaSIF :: DM.DesignMatrixRow (F.Record DP.LPredictorsR) -> [Double] -> K.Sem r (VU.Vector (Double, Double))
        betaSIF dmr mdMeansL = do
          case DM.rowLength dmr of
            0 -> pure VU.empty
            _p -> do
              betaV <- getVector "beta"
              pure $ VU.fromList $ zip (V.toList betaV) mdMeansL
    betaSIM <- sequence $ (betaSIF <$> dmrA config <*> (mdMeansLM <$> actionSurveyA config <*> dmrA config))
    psMap <- case runConfig.rcPS of
      Nothing -> mempty
      Just _ -> case gqDataAndIndexes_CM of
        Nothing -> K.knitError "modelResultAction: Expected gq data and indexes but got Nothing."
        Just gqDaI_C -> do
          let getVectorPcts n = K.knitEither $ SP.getVector . fmap CS.percents <$> SP.parse1D n (CS.paramStats summary)
              psPrefix = case config of
                ActionOnly mCat _ -> if mCat == MC.Reg then "R" else "T"
                PrefOnly mCat _ -> if mCat == MC.Reg then "RP" else "P"
                ActionAndPref mCat _ _ -> if mCat == MC.Reg then "RDVS" else "DVS"
          (_, gqIndexesE) <- K.ignoreCacheTime gqDaI_C
          grpIM <- K.knitEither
             $ gqIndexesE >>= SMB.getGroupIndex (SMB.RowTypeTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData") (MC.psGroupTag @l)
          psTByGrpV <- getVectorPcts $ psPrefix <> "_byGrp"
          K.knitEither $ M.fromList . zip (IM.elems grpIM) <$> (traverse MT.listToCI $ V.toList psTByGrpV)
    pure $ (MC.PSMap psMap, ModelParameters <$> betaSIM)
