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
--import qualified BlueRipple.Data.Small.Loaders as BRDF
import qualified BlueRipple.Data.CachingCore as BRCC
--import qualified BlueRipple.Data.LoadersCore as BRLC
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

import qualified Stan as S
import qualified Stan.Libraries.BuildingBlocks as SBB (rowLength)
import Stan (TypedList(..))
import Stan.Operators
import qualified CmdStan as CS

import Effectful (Eff)
import qualified Flat
import Flat.Instances.Vector ()

--stateG :: S.GroupTypeTag Text
--stateG = S.GroupTypeTag "State"

modelIDT :: S.InputDataType S.ModelDataT (DP.ModelData lk)
modelIDT = S.ModelData

gqIDT :: S.InputDataType S.GQDataT (DP.PSData k)
gqIDT = S.GQData

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
configText (ActionAndPref c (MC.ActionConfig as tMC) (MC.PrefConfig _sp pMC)) = case c of
  MC.Reg -> "RegBoth" <> MC.actionSurveyText as
            <> "_" <> MC.modelConfigText tMC
            <> "_" <> MC.modelConfigText pMC
  MC.Vote -> "Both" <> MC.actionSurveyText as
             <> "_" <> MC.modelConfigText tMC
             <> "_" <> MC.modelConfigText pMC

groupBuilder :: forall g lk a b .
                 (Foldable g
                 , F.ElemOf (DP.CESByR lk) GT.StateAbbreviation
                 , DP.DCatsR F.⊆ DP.CESByR lk
                 , Typeable (DP.CESByR lk)
                 , FSI.RecVec (DP.CESByR lk)
                 , FC.ElemsOf (DP.CESByR lk) [DP.Registered2p, DP.VotesInRace]
                 )
               => Config a b
               -> g Text
               -> S.StanDataBuilderEff S.ModelDataT (DP.ModelData lk) ()
groupBuilder config states = do
  let groups' = MC.groups states
  S.addGroupSizes (modelIDT @lk) groups'
  when (usesCPS config) $ S.addData "CPS" (modelIDT @lk) (S.ToFoldable DP.cpsData) >>= MC.addGroupIndexesAndIntMaps groups' >> pure ()
  when (usesCES config) $ S.addData "CES" (modelIDT @lk) (S.ToFoldable DP.cesData) >>= MC.addGroupIndexesAndIntMaps groups' >> pure ()
  when (usesCESPref config) $ case configModelCategory config of
    MC.Reg -> S.addData "CESR" (modelIDT @lk) (S.ToFoldable DP.cesDataRegPref) >>= MC.addGroupIndexesAndIntMaps groups' >> pure ()
    MC.Vote -> S.addData "CESP" (modelIDT @lk) (S.ToFoldable DP.cesDataVotePref) >>= MC.addGroupIndexesAndIntMaps groups' >> pure ()
  pure ()

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

type ModelRTT rs = S.RowTypeTag (F.Record rs)

actionModelData :: forall a b gq lk . MC.ModelCategory -> MC.ActionConfig a b
                 -> S.StanModelBuilderEff (DP.ModelData lk) gq (MC.ModelData a b)
actionModelData c (MC.ActionConfig ts mc) = do
  let cpsSurveyDataTag :: S.StanModelBuilderEff (DP.ModelData lk) gq (ModelRTT DP.CPSByStateR)
      cpsSurveyDataTag = S.getRTT (modelIDT @lk) "CPS"
      cesSurveyDataTag :: S.StanModelBuilderEff (DP.ModelData lk) gq (ModelRTT DP.CESByCDR)
      cesSurveyDataTag = S.getRTT (modelIDT @lk) "CES"
  let uwSurveyed :: FC.ElemsOf rs [DP.Surveyed, DP.SurveyWeight] => ModelRTT rs -> S.StanModelBuilderEff (DP.ModelData lk) gq S.IntArrayE
      uwSurveyed rtt = S.addCountData (modelIDT @lk) rtt "Surveyed" (view DP.surveyed)
      weightedCount ws = DP.weightedCountF ws (view DP.surveyed) (view DP.surveyWeight) (view DP.surveyedESS)
      weightedQty ws = DP.weightedQtyF ws (view DP.surveyed) (view DP.surveyWeight) (view DP.surveyedESS)
      rwSurveyed :: FC.ElemsOf rs [DP.Surveyed, DP.SurveyWeight, DP.SurveyedESS]
                 => DP.WeightingStyle -> ModelRTT rs -> S.StanModelBuilderEff (DP.ModelData lk) gq S.IntArrayE
      rwSurveyed ws rtt = S.addCountData (modelIDT @lk) rtt "Surveyed" (round . weightedCount ws)
      wSurveyed :: FC.ElemsOf rs '[DP.Surveyed, DP.SurveyWeight, DP.SurveyedESS]
                => DP.WeightingStyle -> ModelRTT rs -> S.StanModelBuilderEff (DP.ModelData lk) gq S.VectorE
      wSurveyed ws rtt = S.addRealData (modelIDT @lk) rtt "Surveyed" (Just 0) Nothing (weightedCount ws)
      uwAction :: forall rs . (FC.ElemsOf rs DP.CountDataR, FC.ElemsOf rs '[DP.Surveyed, DP.SurveyWeight, DP.SurveyedESS])
               => ModelRTT rs -> S.StanModelBuilderEff (DP.ModelData lk) gq S.IntArrayE
      uwAction rtt = case c of
        MC.Reg -> S.addCountData (modelIDT @lk) rtt "Registered" (view DP.registered)
        MC.Vote -> S.addCountData (modelIDT @lk) rtt "Voted" (view DP.voted)
      rwAction :: forall rs . (FC.ElemsOf rs DP.CountDataR)
               => DP.WeightingStyle -> ModelRTT rs -> S.StanModelBuilderEff (DP.ModelData lk) gq S.IntArrayE
      rwAction ws rtt = case c of
        MC.Reg -> S.addCountData (modelIDT @lk) rtt "Registered" (round . weightedQty ws (view DP.registeredW))
        MC.Vote -> S.addCountData (modelIDT @lk) rtt "Voted" (round . weightedQty ws (view DP.votedW))
      wAction :: forall rs . (FC.ElemsOf rs DP.CountDataR)
              => DP.WeightingStyle -> ModelRTT rs -> S.StanModelBuilderEff (DP.ModelData lk) gq S.VectorE
      wAction ws rtt = case c of
        MC.Reg -> S.addRealData (modelIDT @lk) rtt "Registered" (Just 0) Nothing (weightedQty ws (view DP.registeredW))
        MC.Vote -> S.addRealData (modelIDT @lk) rtt "Voted" (Just 0) Nothing (weightedQty ws (view DP.votedW))
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
              -> S.StanModelBuilderEff (DP.ModelData lk) gq (MC.ModelData (F.Record DP.CESByCDR) b)
prefModelData c (MC.PrefConfig _x mc) = do
  let cesSurveyDataTag :: S.StanModelBuilderEff (DP.ModelData lk) gq (ModelRTT DP.CESByCDR)
      cesSurveyDataTag =  S.getRTT (modelIDT @lk) "CESP"
  let uwAction :: forall rs . (FC.ElemsOf rs DP.PrefDataR) => ModelRTT rs -> S.StanModelBuilderEff (DP.ModelData lk) gq S.IntArrayE
      uwAction rtt = case c of
        MC.Reg -> S.addCountData (modelIDT @lk) rtt "Registered" (view DP.registered2p)
        MC.Vote -> S.addCountData (modelIDT @lk) rtt "VotesInRace" (view DP.votesInRace)
      uwPrefD :: forall rs . (FC.ElemsOf rs DP.PrefDataR) => ModelRTT rs -> S.StanModelBuilderEff (DP.ModelData lk) gq S.IntArrayE
      uwPrefD rtt = case c of
        MC.Reg -> S.addCountData (modelIDT @lk) rtt "DReg" (view DP.dReg)
        MC.Vote -> S.addCountData (modelIDT @lk) rtt "DVotes" (view DP.dVotes)
      weightedVotes ws = DP.weightedCountF ws (view DP.votesInRace) (view DP.votesInRaceW) (view DP.votesInRaceESS)
      votesWeighted ws = DP.weightedQtyF ws (view DP.votesInRace) (view DP.votesInRaceW) (view DP.votesInRaceESS)
      weightedReg ws = DP.weightedCountF ws (view DP.registered2p) (view DP.registered2pW) (view DP.registered2pESS)
      regWeighted ws = DP.weightedQtyF ws (view DP.registered2p) (view DP.registered2pW) (view DP.registered2pESS)
      rwAction :: forall rs . (FC.ElemsOf rs DP.PrefDataR)
               => DP.WeightingStyle -> ModelRTT rs -> S.StanModelBuilderEff (DP.ModelData lk) gq S.IntArrayE
      rwAction ws rtt = case c of
        MC.Reg -> S.addCountData (modelIDT @lk) rtt "Registered" (round . weightedReg ws)
        MC.Vote -> S.addCountData (modelIDT @lk) rtt "VotesInRace" (round . weightedVotes ws)
      rwPrefD :: forall rs . (FC.ElemsOf rs DP.PrefDataR)
              =>  DP.WeightingStyle -> ModelRTT rs -> S.StanModelBuilderEff (DP.ModelData lk) gq S.IntArrayE
      rwPrefD ws rtt = case c of
        MC.Reg -> S.addCountData (modelIDT @lk) rtt "DReg" (round . regWeighted ws (view DP.dRegW))
        MC.Vote -> S.addCountData (modelIDT @lk) rtt "DVotes" (round . votesWeighted ws (view DP.dVotesW))
      wAction :: forall rs . (FC.ElemsOf rs DP.PrefDataR)
              =>  DP.WeightingStyle -> ModelRTT rs -> S.StanModelBuilderEff (DP.ModelData lk) gq S.VectorE
      wAction ws rtt = case c of
        MC.Reg -> S.addRealData (modelIDT @lk) rtt "Registered" (Just 0) Nothing (weightedReg ws)
        MC.Vote -> S.addRealData (modelIDT @lk) rtt "VotesInRace" (Just 0) Nothing (weightedVotes ws)
      wPrefD :: forall rs . (FC.ElemsOf rs DP.PrefDataR)
             =>  DP.WeightingStyle -> ModelRTT rs -> S.StanModelBuilderEff (DP.ModelData lk) gq S.VectorE
      wPrefD ws rtt = case c of
        MC.Reg -> S.addRealData (modelIDT @lk) rtt "DReg" (Just 0) Nothing (regWeighted ws (view DP.dRegW))
        MC.Vote -> S.addRealData (modelIDT @lk) rtt "DVotes" (Just 0) Nothing (votesWeighted ws (view DP.dVotesW))
  case mc.mcSurveyAggregation of
    MC.UnweightedAggregation -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc uwAction uwPrefD
    MC.RoundedWeightedAggregation ws -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc (rwAction ws) (rwPrefD ws)
    MC.WeightedAggregation _ ws -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc (wAction ws) (wPrefD ws)

stdNormalDWA :: (S.TypeOneOf t [S.EReal, S.ECVec, S.ERVec], S.GenSType t) => S.DensityWithArgs t
stdNormalDWA = S.DensityWithArgs S.std_normal TNil

type GroupR = GT.StateAbbreviation ': DP.DCatsR

setupAlphaSum :: Maybe Text -> [Text] -> MC.Alphas -> S.StanModelBuilderEff md gq S.AlphaByDataVecCW
setupAlphaSum prefixM states alphas = do
  let nStatesE = S.groupSizeE MC.stateG
      prefixed t = maybe t (<> "_" <> t) prefixM
      alphaNDS n t = S.NamedDeclSpec (prefixed "a" <> t) $ S.vectorSpec n
--      stdNormalBP nds =  S.UntransformedP nds [] TNil (\TNil m -> S.addStmt $ S.sample m S.std_normal TNil)
      normalBP :: forall et . (S.TypeOneOf et [S.EReal, S.ECVec, S.ERVec], S.GenSType et) => Double -> Double -> S.NamedDeclSpec et -> S.BuildParameter et
      normalBP mean sd nds =  S.UntransformedP nds [] TNil (\TNil m -> S.addStmt $ S.sample m S.normalS (S.realE mean :> S.realE sd :> TNil))
      defPrior :: forall et . (S.TypeOneOf et [S.EReal, S.ECVec, S.ERVec], S.GenSType et) => S.NamedDeclSpec et -> S.BuildParameter et
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
      ageAG :: S.GroupAlpha (F.Record GroupR) S.ECVec = S.contramapGroupAlpha (view DT.age5C)
              $ S.firstOrderAlphaDC MC.ageG enumI refAge (defPrior $ alphaNDS (S.groupSizeE MC.ageG |-| S.intE 1) "Age")
--      sexAG  :: S.GroupAlpha (F.Record GroupR) S.EReal = S.contramapGroupAlpha (view DT.sexC)
--              $ S.binaryAlpha prefixM MC.sexG ((\x -> realToFrac x - 0.5) . fromEnum) (defPrior $ S.NamedDeclSpec (prefixed "aSex") $ S.realSpec [])
      sexAG  :: S.GroupAlpha (F.Record GroupR) S.ECVec = S.contramapGroupAlpha (view DT.sexC)
              $ S.firstOrderAlphaDC MC.sexG enumI refSex (defPrior $ alphaNDS (S.groupSizeE MC.sexG |-| S.intE 1) "Sex")
      eduAG  :: S.GroupAlpha (F.Record GroupR) S.ECVec = S.contramapGroupAlpha (view DT.education4C)
              $ S.firstOrderAlphaDC MC.eduG enumI refEducation (defPrior $ alphaNDS (S.groupSizeE MC.eduG |-| S.intE 1) "Edu")
      raceAG  :: S.GroupAlpha (F.Record GroupR) S.ECVec = S.contramapGroupAlpha (view DT.race5C)
               $ S.firstOrderAlphaDC MC.raceG enumI refRace (defPrior $ alphaNDS (S.groupSizeE MC.raceG |-| S.intE 1) "Race")
  let zeroAG_C = do
        let nds = S.NamedDeclSpec (prefixed "alpha0") S.realSpec
        pure $ S.contramapGroupAlpha (view GT.stateAbbreviation) $ S.zeroOrderAlpha $ normalBP 0 2 nds
  let stateAG_C = do
        muAlphaP <- S.simpleParameterWA
                    (S.NamedDeclSpec (prefixed "muSt") S.realSpec)
                    stdNormalDWA
        sigmaAlphaP <-  S.simpleParameterWA
                      (S.NamedDeclSpec (prefixed "sigmaSt")  S.nonNegativeRealSpec)
                      stdNormalDWA
        let  aStBP_C :: S.Parameters [S.EReal, S.EReal] -> S.BuildParameter S.ECVec
             aStBP_C hps = S.UntransformedP (alphaNDS (S.groupSizeE MC.stateG) "St") [] hps
                           $ \(muAlphaE :> sigmaAlphaE :> TNil) m
                             -> S.addStmt $ S.sample m S.normalS (muAlphaE :> sigmaAlphaE :> TNil)
        pure $ S.contramapGroupAlpha (view GT.stateAbbreviation) $ S.firstOrderAlpha MC.stateG stateI (aStBP_C (muAlphaP :> sigmaAlphaP :> TNil))
  let stateAG_NC = do
        muAlphaP <- S.simpleParameterWA
                    (S.NamedDeclSpec (prefixed "muSt") S.realSpec)
                    stdNormalDWA
        sigmaAlphaP <-  S.simpleParameterWA
                      (S.NamedDeclSpec (prefixed "sigmaSt") S.nonNegativeRealSpec)
                      stdNormalDWA
        rawP <- S.simpleParameterWA
                (S.NamedDeclSpec (prefixed "alphaST_raw") $ S.vectorSpec (S.groupSizeE MC.stateG))
                stdNormalDWA
        let aStBP_NC = S.simpleTransformedP (alphaNDS (S.groupSizeE MC.stateG) "St") []
                       (muAlphaP :> sigmaAlphaP :> rawP :> TNil) S.TransformedParametersBlock
                       (\(mu :> s :> r :> TNil) -> S.DeclRHS $ mu |+| (s |*| r))
        pure $ S.contramapGroupAlpha (view GT.stateAbbreviation) $ S.firstOrderAlpha MC.stateG stateI aStBP_NC
  let ageSexAG = do
        sigmaAgeSex <- S.simpleParameterWA
                         (S.NamedDeclSpec (prefixed "sigmaAgeSex") S.nonNegativeRealSpec)
                         stdNormalDWA
        let as_NDS = alphaNDS (S.groupSizeE MC.sexG |*| S.groupSizeE MC.ageG |-| S.intE 1) "AgeSex"
            as_BP ::  S.BuildParameter S.ECVec
            as_BP = S.UntransformedP
                    as_NDS [] (sigmaAgeSex :> TNil)
                    $ \(sigmaE :> TNil) m -> S.addStmt $ S.sample m S.normalS (S.realE 0 :> sigmaE :> TNil)
        pure $ S.contramapGroupAlpha (\r -> (r ^. DT.age5C, r ^. DT.sexC))
          $ S.secondOrderAlphaDC prefixM MC.ageG (enumI, enumS @DT.Age5) MC.sexG (enumI, enumS @DT.Sex) (refAge, refSex) as_BP
  let sexEduAG = do
        sigmaSexEdu <- S.simpleParameterWA
                         (S.NamedDeclSpec (prefixed "sigmaSexEdu")  S.nonNegativeRealSpec)
                         stdNormalDWA
        let as_NDS = alphaNDS (S.groupSizeE MC.sexG |*| S.groupSizeE MC.eduG |-| S.intE 1) "SexEdu"
            as_BP ::  S.BuildParameter S.ECVec
            as_BP = S.UntransformedP
                    as_NDS [] (sigmaSexEdu :> TNil)
                    $ \(sigmaE :> TNil) m -> S.addStmt $ S.sample m S.normalS (S.realE 0 :> sigmaE :> TNil)
        pure $ S.contramapGroupAlpha (\r -> (r ^. DT.sexC, r ^. DT.education4C))
          $ S.secondOrderAlphaDC prefixM MC.sexG (enumI, enumS @DT.Sex) MC.eduG (enumI, enumS @DT.Education4) (refSex, refEducation) as_BP
  let sexRaceAG = do
        sigmaSexRace <- S.simpleParameterWA
                         (S.NamedDeclSpec (prefixed "sigmaSexRace") S.nonNegativeRealSpec)
                         stdNormalDWA
        let as_NDS = alphaNDS (S.groupSizeE MC.sexG |*| S.groupSizeE MC.raceG |-| S.intE 1) "SexRace"
            as_BP ::  S.BuildParameter S.ECVec
            as_BP = S.UntransformedP
                    as_NDS [] (sigmaSexRace :> TNil)
                    $ \(sigmaE :> TNil) m -> S.addStmt $ S.sample m S.normalS (S.realE 0 :> sigmaE :> TNil)
        pure $ S.contramapGroupAlpha (\r -> (r ^. DT.sexC, r ^. DT.race5C))
          $ S.secondOrderAlphaDC prefixM MC.sexG (enumI, enumS @DT.Sex) MC.raceG (enumI, enumS @DT.Race5) (refSex, refRace) as_BP

  let ageEduAG = do
        sigmaAgeEdu <-  S.simpleParameterWA
                         (S.NamedDeclSpec (prefixed "sigmaAgeEdu") S.nonNegativeRealSpec)
                         stdNormalDWA
        let aAE_NDS = alphaNDS (S.groupSizeE MC.ageG |*| S.groupSizeE MC.eduG |-| S.intE 1) "AgeEdu"
            aAE_BP :: S.BuildParameter S.ECVec
            aAE_BP = S.UntransformedP
                     aAE_NDS [] (sigmaAgeEdu :> TNil)
                     $ \(sigmaE :> TNil) m
                       -> S.addStmt $ S.sample m S.normalS (S.realE 0 :> sigmaE :> TNil)
        pure $ S.contramapGroupAlpha (\r -> (r ^. DT.age5C, r ^. DT.education4C ))
          $ S.secondOrderAlphaDC prefixM MC.ageG (enumI, enumS @DT.Age5) MC.eduG (enumI, enumS @DT.Education4) (refAge, refEducation) aAE_BP
      ageRaceAG = do
        sigmaAgeRace <-  S.simpleParameterWA
                         (S.NamedDeclSpec (prefixed "sigmaAgeRace") $ S.nonNegativeRealSpec)
                         stdNormalDWA
        let aAR_NDS = alphaNDS (S.groupSizeE MC.ageG |*| S.groupSizeE MC.raceG |-| S.intE 1) "AgeRace"
            aAR_BP :: S.BuildParameter S.ECVec
            aAR_BP = S.UntransformedP
                     aAR_NDS [] (sigmaAgeRace :> TNil)
                     $ \(sigmaE :> TNil) m
                       -> S.addStmt $ S.sample m S.normalS (S.realE 0 :> sigmaE :> TNil)
        pure $ S.contramapGroupAlpha (\r -> (r ^. DT.age5C, r ^. DT.race5C ))
          $ S.secondOrderAlphaDC prefixM MC.ageG (enumI, enumS @DT.Age5) MC.raceG (enumI, enumS @DT.Race5) (refAge, refRace) aAR_BP
      eduRaceAG = do
        sigmaEduRace <-  S.simpleParameterWA
                         (S.NamedDeclSpec (prefixed "sigmaEduRace") $  S.nonNegativeRealSpec)
                         stdNormalDWA
        let aER_NDS = alphaNDS (S.groupSizeE MC.eduG |*| S.groupSizeE MC.raceG |-| S.intE 1) "EduRace"
            aER_BP :: S.BuildParameter S.ECVec
            aER_BP = S.UntransformedP
                     aER_NDS [] (sigmaEduRace :> TNil)
                     $ \(sigmaE :> TNil) m
                       -> S.addStmt $ S.sample m S.normalS (S.realE 0 :> sigmaE :> TNil)
        pure $ S.contramapGroupAlpha (\r -> (r ^. DT.education4C, r ^. DT.race5C ))
          $ S.secondOrderAlphaDC prefixM MC.eduG (enumI, enumS @DT.Education4) MC.raceG (enumI, enumS @DT.Race5) (refEducation, refRace) aER_BP
      stateAgeAG = do
        sigmaStateAge <-  S.simpleParameterWA
                           (S.NamedDeclSpec (prefixed "sigmaStateAge") S.nonNegativeRealSpec)
                           stdNormalDWA
        let ds = S.matrixSpec (S.groupSizeE MC.stateG) (S.groupSizeE MC.ageG)

            rawNDS = S.NamedDeclSpec (prefixed "alpha_State_Age_raw") ds
        rawAlphaStateAgeP <- S.iidMatrixP rawNDS [] TNil S.std_normal
        let aStA_NDS = S.NamedDeclSpec (prefixed "alpha_State_Age") ds
        let aStA_BP :: S.BuildParameter S.EMat
            aStA_BP = S.simpleTransformedP aStA_NDS [] (sigmaStateAge :> rawAlphaStateAgeP :> TNil)
                     S.TransformedParametersBlock
                     (\(s :> r :> TNil) -> S.DeclRHS $ s |*| r)
        pure $ S.contramapGroupAlpha (\r -> (r ^. GT.stateAbbreviation, r ^. DT.age5C))
          $ S.secondOrderAlpha prefixM MC.stateG stateI MC.ageG enumI aStA_BP
      stateSexAG = do
        sigmaStateSex <-  S.simpleParameterWA
                           (S.NamedDeclSpec (prefixed "sigmaStateSex") S.nonNegativeRealSpec)
                           stdNormalDWA
        let ds = S.matrixSpec (S.groupSizeE MC.stateG) (S.groupSizeE MC.sexG)
            rawNDS = S.NamedDeclSpec (prefixed "alpha_State_Sex_raw") ds
        rawAlphaStateSexP <- S.iidMatrixP rawNDS [] TNil S.std_normal
        let aStS_NDS = S.NamedDeclSpec (prefixed "alpha_State_Sex") ds
        let aStS_BP :: S.BuildParameter S.EMat
            aStS_BP = S.simpleTransformedP aStS_NDS [] (sigmaStateSex :> rawAlphaStateSexP :> TNil)
                     S.TransformedParametersBlock
                     (\(s :> r :> TNil) -> S.DeclRHS $ s |*| r)
        pure $ S.contramapGroupAlpha (\r -> (r ^. GT.stateAbbreviation, r ^. DT.sexC))
          $ S.secondOrderAlpha prefixM MC.stateG stateI MC.sexG enumI aStS_BP
      stateEduAG = do
        sigmaStateEdu <-  S.simpleParameterWA
                           (S.NamedDeclSpec (prefixed "sigmaStateEdu") S.nonNegativeRealSpec)
                           stdNormalDWA
        let ds = S.matrixSpec (S.groupSizeE MC.stateG) (S.groupSizeE MC.eduG)

            rawNDS = S.NamedDeclSpec (prefixed "alpha_State_Edu_raw") ds
        rawAlphaStateEduP <- S.iidMatrixP rawNDS [] TNil S.std_normal
        let aStE_NDS = S.NamedDeclSpec (prefixed "alpha_State_Edu") ds
        let aStE_BP :: S.BuildParameter S.EMat
            aStE_BP = S.simpleTransformedP aStE_NDS [] (sigmaStateEdu :> rawAlphaStateEduP :> TNil)
                     S.TransformedParametersBlock
                     (\(s :> r :> TNil) -> S.DeclRHS $ s |*| r)
        pure $ S.contramapGroupAlpha (\r -> (r ^. GT.stateAbbreviation, r ^. DT.education4C))
          $ S.secondOrderAlpha prefixM MC.stateG stateI MC.eduG enumI aStE_BP
      stateRaceAG = do
        sigmaStateRace <-  S.simpleParameterWA
                           (S.NamedDeclSpec (prefixed "sigmaStateRace") S.nonNegativeRealSpec)
                           stdNormalDWA
        let ds = S.matrixSpec (S.groupSizeE MC.stateG) (S.groupSizeE MC.raceG)

            rawNDS = S.NamedDeclSpec (prefixed "alpha_State_Race_raw") ds
        rawAlphaStateRaceP <- S.iidMatrixP rawNDS [] TNil S.std_normal
        let aStR_NDS = S.NamedDeclSpec (prefixed "alpha_State_Race") ds
        let aStR_BP :: S.BuildParameter S.EMat
--            aSR_BP sigma = S.iidMatrixBP aSR_NDS [] (S.given (S.realE 0) :> sigma :> TNil) S.normalS
            aStR_BP = S.simpleTransformedP aStR_NDS [] (sigmaStateRace :> rawAlphaStateRaceP :> TNil)
                     S.TransformedParametersBlock
                     (\(s :> r :> TNil) -> S.DeclRHS $ s|*| r)
        pure $ S.contramapGroupAlpha (\r -> (r ^. GT.stateAbbreviation, r ^. DT.race5C))
          $ S.secondOrderAlpha prefixM MC.stateG stateI MC.raceG enumI aStR_BP

      stateEduRace :: S.StanModelBuilderEff md gq (S.GroupAlpha (F.Record GroupR) (S.EArray1 S.EMat))
      stateEduRace = do
        sigmaStateEduRaceP :: S.Parameter S.EReal  <-  S.simpleParameterWA
                                                          (S.NamedDeclSpec (prefixed "sigmaStateEduRace") S.nonNegativeRealSpec)
                                                          stdNormalDWA
        let ds = S.array1Spec nStatesE $ S.matrixSpec (S.groupSizeE MC.eduG) (S.groupSizeE MC.raceG)
            aStER_NDS :: S.NamedDeclSpec (S.EArray1 S.EMat) = S.NamedDeclSpec (prefixed "alpha_State_Edu_Race") ds
            aStER_raw_NDS :: S.NamedDeclSpec (S.EArray1 S.EMat) = S.NamedDeclSpec (prefixed "alpha_State_Edu_Race_raw") ds
        rawAlphaStateEduRaceP :: S.Parameter (S.EArray1 S.EMat) <- S.addBuildParameter
          $ S.UntransformedP  aStER_raw_NDS [] TNil
          $ \_ t -> S.addStmt
                    ( S.for "s" (S.SpecificNumbered (S.intE 1) nStatesE)
                      $ \s -> S.to_vector (t !! s) |~| stdNormalDWA
                    )
        let aStER_BP :: S.BuildParameter (S.EArray1 S.EMat)
            aStER_BP =  S.simpleTransformedP aStER_NDS [] (sigmaStateEduRaceP :> rawAlphaStateEduRaceP :> TNil)
                        S.TransformedParametersBlock
                        (\(sigma :> raw :> TNil) -> S.DeclCodeF
                          $ \t -> S.addStmt
                                  $ S.for "s" (S.SpecificNumbered (S.intE 1) nStatesE)
                                  $ \s -> (t !! s) |=| (sigma |*| (raw !! s))
                        )
        pure $ S.contramapGroupAlpha (\r -> (r ^. GT.stateAbbreviation, r ^. DT.education4C, r ^. DT.race5C))
          $ S.thirdOrderAlpha prefixM MC.stateG stateI MC.eduG enumI MC.raceG enumI aStER_BP
  case alphas of
    MC.A -> do
      zeroAG <- zeroAG_C
      S.setupAlphaSum (zeroAG :> ageAG :> TNil)
    MC.S -> do
      zeroAG <- zeroAG_C
      S.setupAlphaSum (zeroAG :> sexAG :> TNil)
    MC.E -> do
      zeroAG <- zeroAG_C
      S.setupAlphaSum (zeroAG :> eduAG :> TNil)
    MC.R -> do
      zeroAG <- zeroAG_C
      S.setupAlphaSum (zeroAG :> raceAG :> TNil)
    MC.A_S -> do
      zeroAG <- zeroAG_C
      S.setupAlphaSum (zeroAG :> ageAG :> sexAG :> TNil)
    MC.A_E -> do
      zeroAG <- zeroAG_C
      S.setupAlphaSum (zeroAG :> ageAG :> eduAG :> TNil)
    MC.A_S_AS -> do
      zeroAG <- zeroAG_C
      asAG <- ageSexAG
      S.setupAlphaSum (zeroAG :> ageAG :> sexAG :> asAG :> TNil)
    MC.AS -> do
      zeroAG <- zeroAG_C
      asAG <- ageSexAG
      S.setupAlphaSum (zeroAG :> asAG :> TNil)
    MC.S_E -> do
      zeroAG <- zeroAG_C
      S.setupAlphaSum (zeroAG :> sexAG :> eduAG :> TNil)
    MC.S_R -> do
      zeroAG <- zeroAG_C
      S.setupAlphaSum (zeroAG :> sexAG :> raceAG :> TNil)
    MC.A_E_AE -> do
      zeroAG <- zeroAG_C
      aeAG <- ageEduAG
      S.setupAlphaSum (zeroAG :> ageAG :> eduAG :> aeAG :> TNil)
    MC.A_S_E_R -> do
      zeroAG <- zeroAG_C
      S.setupAlphaSum (zeroAG :> ageAG :> sexAG :> eduAG :> raceAG :> TNil)
    MC.A_S_E_R_AS_AE_AR_SE_SR_ER -> do
      zeroAG <- zeroAG_C
      asAG <- ageSexAG
      aeAG <- ageEduAG
      arAG <- ageRaceAG
      seAG <- sexEduAG
      srAG <- sexRaceAG
      erAG <- eduRaceAG
      S.setupAlphaSum (zeroAG :> ageAG :> sexAG :> eduAG :> raceAG :>
                        asAG :> aeAG :> arAG :> seAG :> srAG :> erAG :> TNil)
    MC.St_A_S_E_R -> do
      stAG <- stateAG_C
      S.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> TNil)
    MC.St_A_S_E_R_ER -> do
      stAG <- stateAG_C
      erAG <- eduRaceAG
      S.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> erAG :> TNil)
    MC.St_A_S_E_R_AR -> do
      stAG <- stateAG_C
      arAG <- ageRaceAG
      S.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> arAG :> TNil)
    MC.St_A_S_E_R_StR -> do
      stAG <- stateAG_C
      srAG <- stateRaceAG
      S.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> srAG :> TNil)
    MC.St_A_S_E_R_StA -> do
      stAG <- stateAG_C
      saAG <- stateAgeAG
      S.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> saAG :> TNil)
    MC.St_A_S_E_R_ER_StR -> do
      stAG <- stateAG_C
      srAG <- stateRaceAG
      erAG <- eduRaceAG
      S.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> erAG :> srAG :> TNil)
    MC.St_A_S_E_R_AE_AR_ER_StR -> do
      stAG <- stateAG_NC
      aeAG <- ageEduAG
      arAG <- ageRaceAG
      srAG <- stateRaceAG
      erAG <- eduRaceAG
      S.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> aeAG :> arAG :> erAG :> srAG :> TNil)
    MC.St_A_S_E_R_ER_StE_StR -> do
      stAG <- stateAG_C
      srAG <- stateRaceAG
      seAG <- stateEduAG
      erAG <- eduRaceAG
      S.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> erAG :> seAG :> srAG :> TNil)
    MC.St_A_S_E_R_ER_StA_StE_StR -> do
      stAG <- stateAG_C
      saAG <- stateAgeAG
      seAG <- stateEduAG
      srAG <- stateRaceAG
      erAG <- eduRaceAG
      S.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> erAG :> saAG :> seAG :> srAG :> TNil)
    MC.St_A_S_E_R_ER_StR_StER -> do
      stAG <- stateAG_C
      srAG <- stateRaceAG
      erAG <- eduRaceAG
      serAG <- stateEduRace
      S.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG :> erAG :> srAG :> serAG :> TNil)
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
      S.setupAlphaSum (stAG :> ageAG :> sexAG :> eduAG :> raceAG
                        :> saAG :> ssAG :> seAG :> srAG
                        :> asAG :> aeAG :> arAG :> sxeAG :> sxrAG :> erAG
                        :> serAG :> TNil)

setupBeta :: Maybe Text -> MC.ModelConfig b -> S.StanModelBuilderEff md gq (Maybe S.VectorE)
setupBeta prefixM mc = do
  let dmr = mc.mcDesignMatrixRow
      prefixed t = maybe t (<> "_" <> t) prefixM
      (_, nCovariatesE) = S.designMatrixColDimBinding dmr Nothing
  betaM <- if SBB.rowLength dmr > 0 then
               (Just . S.parameterExpr)
               <$> S.simpleParameterWA
               (S.NamedDeclSpec (prefixed "beta") $ S.vectorSpec nCovariatesE)
               stdNormalDWA
             else pure $ Nothing
  pure betaM

data ParameterSetup = LogitSetup S.AlphaByDataVecCW (Maybe S.VectorE)

hasBeta :: ParameterSetup -> Bool
hasBeta (LogitSetup _ mb) = isJust mb

setupParameters :: Maybe Text -> [Text] -> MC.ModelConfig b -> S.StanModelBuilderEff md gq ParameterSetup
setupParameters prefixM states mc = do
  as <- setupAlphaSum prefixM states mc.mcAlphas
  bs <- setupBeta prefixM mc
  pure $ LogitSetup as bs

{-
logitProbCW' :: forall i d a md gq . ParameterSetup -> S.InputDataType i d -> S.RowTypeTag a -> S.MatrixE -> S.StanModelBuilderEff md gq (S.CodeWriter S.VectorE)
logitProbCW' ps idt rtt covM =
  case ps of
    LogitSetup abdcw mBeta -> do
      alphaSumCW <- case abdcw of
        S.AlphaByDataVecCW f -> f @i @d @a @(S.StanBuilderEffs md gq) idt rtt
--      alphaSumCW <- S.alphaByDataVecCW @i @d abdcw idt rtt
      case mBeta of
        Nothing -> pure alphaSumCW
        Just beta -> pure $ do
          alphaSum <- alphaSumCW
          pure $ alphaSum |+| (covM |*| beta)
-}
logitProbCW :: S.AlphaByDataVecC i d es => ParameterSetup -> S.InputDataType i d -> S.RowTypeTag a -> S.MatrixE -> Eff es (S.CodeWriter S.VectorE)
logitProbCW ps idt rtt covM =
  case ps of
    LogitSetup abdcw mBeta -> do
      alphaSumCW <- case abdcw of
        S.AlphaByDataVecCW f -> f idt rtt
--      alphaSumCW <- S.alphaByDataVecCW @i @d abdcw idt rtt
      case mBeta of
        Nothing -> pure alphaSumCW
        Just beta -> pure $ do
          alphaSum <- alphaSumCW
          pure $ alphaSum |+| (covM |*| beta)


probabilitiesCW ::  S.AlphaByDataVecC i d es => ParameterSetup -> S.InputDataType i d -> S.RowTypeTag a -> S.MatrixE -> Eff es (S.CodeWriter S.VectorE)
probabilitiesCW ps idt rtt covM = fmap S.inv_logit <$> (logitProbCW ps idt rtt covM)

{-
stateTargetsModel :: Maybe Text
                  -> ParameterSetup md gq
                  -> MC.CovariatesAndCounts a b
                  -> MC.StateTargetsData td
                  -> Maybe (S.MatrixE -> S.StanName -> S.StanBuilderM md gq S.MatrixE)
                  -> Maybe S.MatrixE
                  -> S.StanBuilderM md gq ()
stateTargetsModel prefixM ps cc std cM rM = do
  let prefixed t = maybe t (<>  "_" <> t) prefixM
  (dmACS, acsNByState) <- MC.stateTargetsTD prefixM cc std cM rM
  let acsStateIndex = S.byGroupIndexE std.stdACSTag MC.stateG
      toVec x = S.functionE S.to_vector (x :> TNil)
  pCW <- probabilitiesCW ps std.stdACSTag dmACS
  acsPS <- S.postStratifiedParameterF False S.SBTransformedParameters
           (Just $ prefixed "psByState") std.stdACSTag MC.stateG acsStateIndex (S.NoCW $ toVec std.stdACSWgts) pCW Nothing
  let normalDWA = S.DensityWithArgs S.normal (S.realE 1 :> S.realE 4 :> TNil)
  sigmaTargetsP <-  S.simpleParameterWA
                    (S.NamedDeclSpec (prefixed "sigmaTgt") $ S.realSpec [S.lowerM $ S.realE 1])
                    normalDWA
  S.inBlock S.SBModel $ S.addFromCodeWriter $ do
    let eMult = S.binaryOpE (S.SElementWise S.SMultiply)
        eDiv = S.binaryOpE (S.SElementWise S.SDivide)
        tP = S.indexE S.s0 (S.byGroupIndexE std.stdTargetTypeTag MC.stateG) std.stdTarget
        sd1 = tP `eMult` (S.realE 1 `S.minusE` tP) `eDiv` toVec acsNByState
        sd = S.parameterExpr sigmaTargetsP `S.timesE` S.functionE S.sqrt (sd1 :> TNil)
    S.addStmt $ S.sample acsPS S.normal (tP :> sd :> TNil)
-}
{-
postStratificationWeights :: forall k md . (Typeable psRow)
                          => S.RowTypeTag psRow -> (psRow -> Int) -> S.StanBuilderM md (DP.PSData k) S.IntArrayE
postStratificationWeights psDataTag psWgt = S.addCountData psDataTag "PSWgts" psWgt
-}
postStratificationProbsCW :: forall l psRow md gq . (Typeable psRow
                                                 --                                             , F.ElemOf (DP.PSDataR k) DT.PopCount
                                                 --                                             , DP.LPredictorsR F.⊆ DP.PSDataR k
                                                 )
                          => S.InputDataType S.GQDataT gq
                          -> S.RowTypeTag psRow
                          -> (psRow -> F.Record DP.LPredictorsR)
                          -> Text
                          -> ParameterSetup
                          -> S.DesignMatrixRow (F.Record DP.LPredictorsR)
                          -> Maybe (S.MatrixE -> S.VarName -> S.StanModelBuilderEff md gq S.MatrixE)
                          -> Maybe S.MatrixE
                          -> S.GroupTypeTag l
                          -> S.StanModelBuilderEff md gq (S.CodeWriter S.VectorE)
postStratificationProbsCW gqIDT' psRowTag psRowPredictors prefix ps dmr cM rM _gtt = do
  let prefixed t = prefix <> "_" <> t
  let nCovariates = SBB.rowLength dmr
  psCovariates <-  if nCovariates > 0
                   then S.addDesignMatrix gqIDT' psRowTag (contramap psRowPredictors dmr) Nothing
                   else pure $ S.namedE (prefixed "ERROR") S.SMat -- this shouldn't show up in stan code at all
  dmPS' <- case cM of
    Nothing -> pure psCovariates
    Just c -> c psCovariates $ prefixed "dmPS_Centered"
  dmPS <- case rM of
    Nothing -> pure dmPS'
    Just r -> S.inBlock S.SBTransformedDataGQ $ S.addFromCodeWriter $ do
      let rowsE = S.dataSetSizeE psRowTag
          colsE = snd $ S.designMatrixColDimBinding dmr Nothing
      S.declareRHSNW (S.NamedDeclSpec (prefixed "dmPS_QR") $ S.matrixSpec rowsE colsE) $ dmPS' |*| r
  probabilitiesCW ps gqIDT' psRowTag dmPS


postStratifyOne :: forall l psRow md gq . (Typeable psRow
--                                   , F.ElemOf (DP.PSDataR k) DT.PopCount
--                                   , DP.LPredictorsR F.⊆ DP.PSDataR k
                                   )
                => S.InputDataType S.GQDataT gq
                -> S.RowTypeTag psRow
                -> (psRow -> Int)
                -> (psRow -> F.Record DP.LPredictorsR)
                -> Text
                -> ParameterSetup
                -> Maybe (S.CodeWriter (S.VectorE -> S.VectorE))
                -> Maybe (S.CodeWriter (S.IntArrayE -> S.IntArrayE))
                -> S.DesignMatrixRow (F.Record DP.LPredictorsR)
                -> Maybe (S.MatrixE -> S.VarName -> S.StanModelBuilderEff md gq S.MatrixE)
                -> Maybe S.MatrixE
                -> S.GroupTypeTag l
                -> S.StanModelBuilderEff md gq S.VectorE
postStratifyOne gqIDT' psRowTag psRowWgt psRowPredictors prefix ps modPF modWgtsF dmr cM rM gtt = do
  psWgts' <- S.addCountData gqIDT' psRowTag "PSWgts" psRowWgt --postStratificationWeights psRowTag psWgt
  let psWgts = S.to_vector <$> maybe (S.NoCW psWgts') (\mwF -> S.NeedsCW $ mwF <*> pure psWgts') modWgtsF
  psCW' <- postStratificationProbsCW gqIDT' psRowTag psRowPredictors prefix ps dmr cM rM gtt
  let psCW = maybe psCW' (<*> psCW') modPF

  let prefixed t = prefix <> "_" <> t
      psDataGrpIndex = S.dataByGroupIndexE psRowTag gtt
  S.postStratifiedParameterF False S.SBGeneratedQuantities (Just $ prefixed "byGrp") psRowTag gtt psDataGrpIndex psWgts psCW Nothing


--data RunConfig l = RunConfig { rcIncludePPCheck :: Bool, rcIncludeLL :: Bool, rcIncludeDMSplits :: Bool, rcTurnoutPS :: Maybe (S.GroupTypeTag (F.Record l)) }

data Components md gq =
  Components
  {
    coModel :: S.StanModelBuilderEff md gq ()
  , coLL :: S.StanModelBuilderEff md gq ()
  , coPP :: S.StanModelBuilderEff md gq ()
  , coCenterCovariatesF :: S.InputDataT -> S.MatrixE -> S.VarName -> S.StanModelBuilderEff md gq S.MatrixE
  }

components :: forall r b md gq .
              Maybe Text
           -> MC.CovariatesAndCounts r b
           -> ParameterSetup
           -> MC.SurveyAggregation b
           -> S.StanModelBuilderEff md gq (Components md gq)
components prefixM cc paramSetup sa = do
  let prefixed t = maybe t (<> "_" <> t) prefixM
      nRowsE = S.dataSetSizeE cc.ccSurveyDataTag
  (covariatesM, centerF) <- case hasBeta paramSetup of
    True -> do
      (centeredCovariatesE, centerF) <- S.centerDataMatrix S.DMCenterOnly cc.ccCovariates Nothing (prefixed "DM")
      pure (centeredCovariatesE, centerF)
    False -> pure (S.namedE (prefixed "ERROR") S.SMat, \_ x _ -> pure x)
  let n = cc.ccBinomialData.bdTrials
      k = cc.ccBinomialData.bdSuccesses
      toArray x = S.to_array_1d x
      modelIDT' :: S.InputDataType S.ModelDataT md = S.ModelData
  modelLPCW <- logitProbCW paramSetup modelIDT' cc.ccSurveyDataTag covariatesM
  case sa of
    MC.UnweightedAggregation -> do
      let ssf e lp =  S.familySample (S.binomialLogitDist @S.EIntArray) e (n :> lp :> TNil)
          modelCo = S.inBlock S.SBModel $ S.addFromCodeWriter (modelLPCW >>= S.addStmt . ssf k)
          ppCo =  S.generatePosteriorPredictionV
                  (S.NamedDeclSpec (prefixed "pred") $ S.array1Spec nRowsE S.intSpec)
                  S.binomialLogitDist
                  (S.NeedsCW $ modelLPCW >>= \x -> pure (n :> x :> TNil))
          llCo = S.generateLogLikelihood cc.ccSurveyDataTag S.binomialLogitDist
                 ((\x -> (\nE -> n `S.at` nE :> x `S.at` nE :> TNil)) <$> modelLPCW)
                 (pure $ (k `S.at`))
      pure $ Components modelCo llCo (void ppCo) centerF
    MC.RoundedWeightedAggregation _ -> do
      let ssf e lp =  S.familySample (S.binomialLogitDist @S.EIntArray) e (n :> lp :> TNil)
          modelCo = S.inBlock S.SBModel $ S.addFromCodeWriter (modelLPCW >>= S.addStmt . ssf k)
          ppCo =  S.generatePosteriorPredictionV
                  (S.NamedDeclSpec (prefixed "pred") $ S.array1Spec nRowsE S.intSpec)
                  S.binomialLogitDist
                  (S.NeedsCW $ modelLPCW >>= \x -> pure (n :> x :> TNil))
          llCo = S.generateLogLikelihood cc.ccSurveyDataTag S.binomialLogitDist
                 ((\x -> (\nE -> n `S.at` nE :> x `S.at` nE :> TNil)) <$> modelLPCW)
                 (pure $ (k `S.at`))
      pure $ Components modelCo llCo (void ppCo) centerF
    MC.WeightedAggregation MC.ContinuousBinomial _ -> do
      realBinomialLogitDistV <- S.realBinomialLogitDistM @S.ECVec
      realBinomialLogitDistS <- S.realBinomialLogitDistSM
      let ssf e lp = S.familySample realBinomialLogitDistV e (n :> lp :> TNil)
          modelCo = S.inBlock S.SBModel . S.addFromCodeWriter $ modelLPCW >>= S.addStmt . ssf k
          ppCo = S.generatePosteriorPredictionV'
                 (S.NamedDeclSpec (prefixed "pred") $ S.array1Spec nRowsE S.realSpec)
                 realBinomialLogitDistV
                 (S.NeedsCW $ modelLPCW >>= \x -> pure (n :> x :> TNil))
                 toArray
          llCo = S.generateLogLikelihood cc.ccSurveyDataTag realBinomialLogitDistS
                 ((\x -> (\nE -> n `S.at` nE :> x `S.at` nE :> TNil)) <$> modelLPCW)
                 (pure $ (k `S.at`))
      pure $ Components modelCo llCo (void ppCo) centerF
    MC.WeightedAggregation MC.BetaProportion _ -> do
      let muKappa lp = do
            mu <- S.declareRHSNW (S.NamedDeclSpec "mu" $ S.vectorSpec nRowsE) $ S.inv_logit lp
            kappa <- S.declareRHSNW (S.NamedDeclSpec "kappa" $ S.vectorSpec nRowsE) $ S.to_vector n
            pure (mu :> kappa :> TNil)
      th <- S.inBlock S.SBTransformedData $ S.addFromCodeWriter $ do
        let vecSpec = S.vectorSpec nRowsE
            vecOf x = S.rep_vector (S.realE x) nRowsE
--            vecMax v1 v2 = S.fmax (v1 :> v2 :> TNil)
  --          vecMin v1 v2 = S.functionE S.fmin (v1 :> v2 :> TNil)
        S.declareRHSNW (S.NamedDeclSpec (prefixed "th") vecSpec) $ S.fmax (vecOf 0.0001) $ S.fmin (vecOf 0.9999) $ k |./| n
      let modelCo = S.inBlock S.SBModel $ S.addScopedFromCodeWriter
                    $ modelLPCW >>= muKappa >>= S.addStmt . S.familySample S.betaProportionDist th
          ppCo =  S.generatePosteriorPredictionV'
                  (S.NamedDeclSpec (prefixed "predR") $ S.array1Spec nRowsE S.realSpec)
                  S.betaProportionDist
                  (S.NeedsCW $ modelLPCW >>= muKappa)
                  toArray
          llCo = S.generateLogLikelihood cc.ccSurveyDataTag S.betaProportionDist
                 (modelLPCW >>= muKappa >>= (\(mu :> kappa :> TNil) -> (pure $ \nE -> mu `S.at` nE :> kappa `S.at` nE :> TNil)))
                 (pure $ (th `S.at`))
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
      -> S.StanModelBuilderEff (DP.ModelData lk) (DP.PSData k) ()
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
            gqIDT' :: S.InputDataType S.GQDataT (DP.PSData k) = S.GQData
        psRowTag <- S.getRTT @(F.Record (DP.PSDataR k)) gqIDT' "PSData" --S.dataSetTag @(F.Record (DP.PSDataR k)) S.GQData "PSData"
        postStratifyOne gqIDT' psRowTag (view DT.popCount) F.rcast actionLabel paramSetup Nothing Nothing mc.mcDesignMatrixRow (Just $ centerF S.GQDataT) Nothing gtt >> pure ()

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
            gqIDT' :: S.InputDataType S.GQDataT (DP.PSData k) = S.GQData
        psRowTag <- S.getRTT @(F.Record (DP.PSDataR k)) gqIDT' "PSData" --S.dataSetTag @(F.Record (DP.PSDataR k)) S.GQData "PSData"
        postStratifyOne gqIDT' psRowTag (view DT.popCount) F.rcast prefLabel paramSetup Nothing Nothing mc.mcDesignMatrixRow (Just $ centerF S.GQDataT) Nothing gtt >> pure ()

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
        let gqIDT' :: S.InputDataType S.GQDataT (DP.PSData k) = S.GQData
        psRowTag <- S.getRTT @(F.Record (DP.PSDataR k)) gqIDT' "PSData" --S.dataSetTag @(F.Record (DP.PSDataR k)) S.GQData "PSData"
        psWgts <- S.addCountData gqIDT' psRowTag "PSWgts" (view DT.popCount) --postStratificationWeights psRowTag (view )
        aProbsCW <- postStratificationProbsCW gqIDT' psRowTag F.rcast actionLabel aParamS aMC.mcDesignMatrixRow (Just $ aCenterF S.GQDataT) Nothing gtt
        pProbsCW <- postStratificationProbsCW gqIDT' psRowTag F.rcast prefLabel pParamS pMC.mcDesignMatrixRow (Just $ pCenterF S.GQDataT) Nothing gtt
        let psDataGrpIndex = S.dataByGroupIndexE psRowTag gtt
            wgtsMCW = S.NeedsCW $ fmap (|.*| S.to_vector psWgts) aProbsCW
        S.postStratifiedParameterF False S.SBGeneratedQuantities (Just $ psLabel <> "_byGrp") psRowTag gtt psDataGrpIndex wgtsMCW pProbsCW Nothing >> pure ()


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
            , Typeable k
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
  let runnerInputNames = S.RunnerInputNames
                         (stanDir <> modelName <> "/")
                         (configText config)
                         (Just $ S.GQNames "GQ" (dataName <> "_" <> gqName))
                         dataName
--  modelData <- K.ignoreCacheTime modelData_C
  states <- S.toList . FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) . DP.cesData <$> K.ignoreCacheTime modelData_C
  psKeys <- S.toList . FL.fold (FL.premap (F.rcast @l) FL.set) . DP.unPSData <$> K.ignoreCacheTime psData_C
  (dw, code) <- S.dataWranglerAndCode modelData_C psData_C
                (groupBuilder config states)
                (const $ MC.psGroupBuilder states psKeys)
                (const . const $ model runConfig config states)
-- HERE
  let datSuffix = S.rinData runnerInputNames
      jsonData t = "jsonData_" <> datSuffix <> "$" <> t
      rSuffix = S.rinModel runnerInputNames <> "_" <> datSuffix
      (aFieldName, pFieldName, aNum, aDenom, pNum, pDenom) = case configModelCategory config of
        MC.Reg -> ("Registered", "DReg" :: Text, jsonData "Registered", jsonData "Surveyed", jsonData "DReg", jsonData "Registered")
        MC.Vote -> ("Turnout", "DVotes", jsonData "Voted", jsonData "Surveyed", jsonData "DVotes", jsonData "VotesInRace")
      unwraps = case config of
        ActionOnly _  (MC.ActionConfig _ mc) -> case mc.mcSurveyAggregation of
          MC.WeightedAggregation MC.BetaProportion _ -> [S.UnwrapExpr (aNum <> " / " <> aDenom) ("y" <> aFieldName <> "Rate_" <> rSuffix)]
          _ -> [S.UnwrapNamed aFieldName ("y" <> aFieldName <> "_" <> rSuffix)]
        PrefOnly _  (MC.PrefConfig _ mc) -> case mc.mcSurveyAggregation of
          MC.WeightedAggregation MC.BetaProportion _ -> [S.UnwrapExpr (pNum <> " / " <> pDenom) ("y" <> pFieldName <> "Rate_" <> rSuffix)]
          _ -> [S.UnwrapNamed pFieldName ("y" <> pFieldName <> "_" <> rSuffix)]
        ActionAndPref _  _ _  -> [] -- what is a PP check for this combo case??
  res_C <- S.runModel' @BRCC.SerializerC @BRCC.CacheData
           modelDirE
           (Right runnerInputNames)
           Nothing
           dw
           code
           (modelResultAction config runConfig) --S.DoNothing -- (stateModelResultAction mcWithId dmr)
           (S.Both unwraps) --(S.Both [S.UnwrapNamed "successes" "yObserved"])
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
                            -> S.DesignMatrixRow (F.Record DP.LPredictorsR)
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
    lps = S.designMatrixRowF dmr (F.rcast row)
    p' = f p $ applyBetas mp.mpBetaSI lps


deriving anyclass instance Flat.Flat ModelParameters

actionSurveyA :: Config a b -> Maybe (MC.ActionSurvey a)
actionSurveyA (ActionOnly _ (MC.ActionConfig ts _)) = Just ts
actionSurveyA (PrefOnly _ (MC.PrefConfig sp _)) = Just $ MC.CESSurvey sp
actionSurveyA (ActionAndPref _ (MC.ActionConfig _ _) _) = Nothing

dmrA :: Config a b -> Maybe (S.DesignMatrixRow (F.Record DP.LPredictorsR))
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
                  -> S.ResultAction (DP.ModelData lk) (DP.PSData k) S.DataSetGroupIntMaps S.DataSetGroupIntMaps r () (MC.PSMap l MT.ConfidenceInterval, Maybe ModelParameters)
modelResultAction config runConfig = S.UseSummary f where
  f summary _ modelDataAndIndexes_C gqDataAndIndexes_CM = do
    (modelData, _) <- K.ignoreCacheTime modelDataAndIndexes_C
     -- compute means of predictors because model was zero-centered in them
    let mdMeansFld :: DP.LPredictorsR F.⊆ rs
                   => S.DesignMatrixRow (F.Record DP.LPredictorsR) -> FL.Fold (F.Record rs) [Double]
        mdMeansFld dmr =
          let  covariates = S.designMatrixRowF $ contramap F.rcast dmr
               nPredictors = SBB.rowLength dmr
          in FL.premap (VU.toList . covariates)
             $ traverse (\n -> FL.premap (List.!! n) FL.mean) [0..(nPredictors - 1)]
        mdMeansLM :: MC.ActionSurvey a -> S.DesignMatrixRow (F.Record DP.LPredictorsR) -> [Double]
        mdMeansLM as dmr = case as of
          MC.CESSurvey _ -> FL.fold (FL.premap (F.rcast @DP.LPredictorsR) $ mdMeansFld dmr) $ DP.cesData modelData
          MC.CPSSurvey -> FL.fold (FL.premap (F.rcast @DP.LPredictorsR) $ mdMeansFld dmr) $ DP.cpsData modelData
        getVector n = K.knitEither $ S.getVector . fmap CS.mean <$> S.parse1D n (CS.paramStats summary)
        betaSIF :: S.DesignMatrixRow (F.Record DP.LPredictorsR) -> [Double] -> K.Sem r (VU.Vector (Double, Double))
        betaSIF dmr mdMeansL = do
          case SBB.rowLength dmr of
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
          let getVectorPcts n = K.knitEither $ S.getVector . fmap CS.percents <$> S.parse1D n (CS.paramStats summary)
              psPrefix = case config of
                ActionOnly mCat _ -> if mCat == MC.Reg then "R" else "T"
                PrefOnly mCat _ -> if mCat == MC.Reg then "RP" else "P"
                ActionAndPref mCat _ _ -> if mCat == MC.Reg then "RDVS" else "DVS"
              rtt :: S.RowTypeTag (F.Record (DP.PSDataR k)) = S.RowTypeTag "PSData"
          (_, gqIndexesE) <- K.ignoreCacheTime gqDaI_C
          grpIM <- K.knitEither $ gqIndexesE >>= S.getGroupIndex rtt (MC.psGroupTag @l)
          psTByGrpV <- getVectorPcts $ psPrefix <> "_byGrp"
          K.knitEither $ M.fromList . zip (IM.elems grpIM) <$> (traverse MT.listToCI $ V.toList psTByGrpV)
    pure $ (MC.PSMap psMap, ModelParameters <$> betaSIM)
