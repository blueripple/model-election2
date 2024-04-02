{-# LANGUAGE AllowAmbiguousTypes #-}
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
import qualified Frames.Melt as F
import qualified Frames.Serialize as FS

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
  RegistrationOnly :: MC.RegistrationConfig a b -> Config a b
  TurnoutOnly :: MC.TurnoutConfig a b -> Config a b
  PrefOnly :: MC.PrefConfig b -> Config (F.Record DP.CESByCDR) b
  TurnoutAndPref :: MC.TurnoutConfig a b -> MC.PrefConfig b -> Config a b

turnoutSurvey :: Config a b -> Maybe (MC.TurnoutSurvey a)
turnoutSurvey (RegistrationOnly (MC.RegistrationConfig rs _)) = Just rs
turnoutSurvey (TurnoutOnly (MC.TurnoutConfig ts _)) = Just ts
turnoutSurvey (PrefOnly _) = Nothing
turnoutSurvey (TurnoutAndPref (MC.TurnoutConfig ts _) _) = Just ts

usesCPS :: Config a b -> Bool
usesCPS c = case c of
  RegistrationOnly (MC.RegistrationConfig rs _) ->  case rs of
    MC.CPSSurvey -> True
    _ -> False
  TurnoutOnly (MC.TurnoutConfig ts _) -> case ts of
    MC.CPSSurvey -> True
    _ -> False
  TurnoutAndPref (MC.TurnoutConfig ts _) _ -> case ts of
    MC.CPSSurvey -> True
    _ -> False
  _ -> False

usesCES :: Config a b -> Bool
usesCES (RegistrationOnly (MC.RegistrationConfig rs _)) =  case rs of
  MC.CESSurvey -> True
  _ -> False
usesCES (TurnoutOnly (MC.TurnoutConfig ts _)) = case ts of
  MC.CESSurvey -> True
  _ -> False
usesCES _ = True

configText :: Config a b -> Text
configText (RegistrationOnly (MC.RegistrationConfig rs mc)) = "Reg" <> MC.turnoutSurveyText rs <> "_" <> MC.modelConfigText mc
configText (TurnoutOnly (MC.TurnoutConfig ts mc)) =
  "Turnout" <> MC.turnoutSurveyText ts <> "_" <> MC.modelConfigText mc
configText (PrefOnly (MC.PrefConfig mc)) =
  "Pref" <> "_" <> MC.modelConfigText mc
configText (TurnoutAndPref (MC.TurnoutConfig ts tMC) (MC.PrefConfig pMC)) =
  "Both" <> MC.turnoutSurveyText ts
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
                 )
               => Config a b
               -> g Text
               -> g (F.Record l)
               -> SMB.StanGroupBuilderM (DP.ModelData lk) (DP.PSData k) ()
groupBuilder config states psKeys = do
  let groups' = MC.groups states
  when (usesCPS config) $ SMB.addModelDataToGroupBuilder "CPS" (SMB.ToFoldable DP.cpsData) >>= MC.addGroupIndexesAndIntMaps groups'
  when (usesCES config) $ SMB.addModelDataToGroupBuilder "CES" (SMB.ToFoldable DP.cesData) >>= MC.addGroupIndexesAndIntMaps groups'
  MC.psGroupBuilder states psKeys

registrationModelData :: forall a b gq lk . MC.RegistrationConfig a b
                 -> SMB.StanBuilderM (DP.ModelData lk) gq (MC.ModelData '[] a b)
registrationModelData (MC.RegistrationConfig ts mc) = do
  let cpsSurveyDataTag = SMB.dataSetTag @(F.Record DP.CPSByStateR) SC.ModelData "CPS"
      cesSurveyDataTag = SMB.dataSetTag @(F.Record DP.CESByCDR) SC.ModelData "CES"
      uwSurveyed rtt = SBB.addCountData rtt "Surveyed" (view DP.surveyed)
      uwVoted rtt = SBB.addCountData rtt "Voted" (view DP.voted)
      wSurveyed rtt = SBB.addRealData rtt "Surveyed" (Just 0) Nothing (view DP.surveyedW)
      wVoted rtt = SBB.addRealData rtt "Voted" (Just 0) Nothing (view DP.votedW)
  case ts of
      MC.CPSSurvey -> case mc.mcSurveyAggregation of
        MC.UnweightedAggregation -> fmap MC.ModelData $ cpsSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc uwSurveyed uwVoted
        MC.WeightedAggregation _  -> fmap MC.ModelData $ cpsSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc wSurveyed wVoted
      MC.CESSurvey -> case mc.mcSurveyAggregation of
        MC.UnweightedAggregation -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc uwSurveyed uwVoted
        MC.WeightedAggregation _ -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc wSurveyed wVoted

turnoutModelData :: forall a b gq lk . MC.TurnoutConfig a b
                 -> SMB.StanBuilderM (DP.ModelData lk) gq (MC.ModelData BRDF.StateTurnoutCols a b)
turnoutModelData (MC.TurnoutConfig ts mc) = do
  let cpsSurveyDataTag = SMB.dataSetTag @(F.Record DP.CPSByStateR) SC.ModelData "CPS"
      cesSurveyDataTag = SMB.dataSetTag @(F.Record DP.CESByCDR) SC.ModelData "CES"
      uwSurveyed rtt = SBB.addCountData rtt "Surveyed" (view DP.surveyed)
      uwVoted rtt = SBB.addCountData rtt "Voted" (view DP.voted)
      wSurveyed rtt = SBB.addRealData rtt "Surveyed" (Just 0) Nothing (view DP.surveyedW)
      wVoted rtt = SBB.addRealData rtt "Voted" (Just 0) Nothing (view DP.votedW)
  case ts of
    MC.CPSSurvey -> case mc.mcSurveyAggregation of
      MC.UnweightedAggregation -> fmap MC.ModelData $ cpsSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc uwSurveyed uwVoted
      MC.WeightedAggregation _  -> fmap MC.ModelData $ cpsSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc wSurveyed wVoted
    MC.CESSurvey -> case mc.mcSurveyAggregation of
      MC.UnweightedAggregation -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc uwSurveyed uwVoted
      MC.WeightedAggregation _  -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc wSurveyed wVoted

prefModelData :: forall b gq lk . MC.PrefConfig b
              -> SMB.StanBuilderM (DP.ModelData lk) gq (MC.ModelData '[] (F.Record DP.CESByCDR) b)
prefModelData (MC.PrefConfig mc) = do
  let cesSurveyDataTag = SMB.dataSetTag @(F.Record DP.CESByCDR) SC.ModelData "CES"
      uwVoted rtt = SBB.addCountData rtt "VotesInRace" (view DP.votesInRace)
      uwVotedD rtt = SBB.addCountData rtt "DVotes" (view DP.dVotes)
      wVoted rtt = SBB.addRealData rtt "VotesInRace" (Just 0) Nothing (view DP.votesInRaceW)
      wVotedD rtt = SBB.addRealData rtt "DVotes" (Just 0) Nothing (view DP.dVotesW)
  case mc.mcSurveyAggregation of
    MC.UnweightedAggregation -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc uwVoted uwVotedD
    MC.WeightedAggregation _  -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc wVoted wVotedD

stdNormalDWA :: (TE.TypeOneOf t [TE.EReal, TE.ECVec, TE.ERVec], TE.GenSType t) => TE.DensityWithArgs t
stdNormalDWA = TE.DensityWithArgs SF.std_normal TNil

type GroupR = GT.StateAbbreviation ': DP.DCatsR

setupAlphaSum :: Maybe Text -> [Text] -> MC.Alphas -> SMB.StanBuilderM md gq (SG.AlphaByDataVecCW md gq)
setupAlphaSum prefixM states alphas = do
  let nStatesE = SMB.groupSizeE MC.stateG
      prefixed t = maybe t (<> "_" <> t) prefixM
      alphaNDS n t = TE.NamedDeclSpec (prefixed "a" <> t) $ TE.vectorSpec n []
      stdNormalBP nds =  DAG.UntransformedP nds [] TNil (\TNil m -> TE.addStmt $ TE.sample m SF.std_normal TNil)
      enumI :: Enum e => e -> Either Text Int
      enumI e = Right $ fromEnum e + 1
      enumS :: forall e . (Enum e, Bounded e) => Int
      enumS = length [(minBound :: e)..(maxBound :: e)]
      stateIndexMap = M.fromList $ zip states [1..]
      stateI s = maybe (Left $ "setupAlphaSum: " <> s <> " is missing from given list of states") Right $ M.lookup s stateIndexMap
--      stateS = M.size stateIndexMap
      ageAG :: SG.GroupAlpha (F.Record GroupR) TE.ECVec = SG.contramapGroupAlpha (view DT.age5C)
              $ SG.firstOrderAlphaDC MC.ageG enumI DT.A5_45To64 (stdNormalBP $ alphaNDS (SMB.groupSizeE MC.ageG `TE.minusE` TE.intE 1) "Age")
      sexAG  :: SG.GroupAlpha (F.Record GroupR) TE.EReal = SG.contramapGroupAlpha (view DT.sexC)
              $ SG.binaryAlpha prefixM MC.sexG ((\x -> realToFrac x - 0.5) . fromEnum) (stdNormalBP $ TE.NamedDeclSpec (prefixed "aSex") $ TE.realSpec [])
      eduAG  :: SG.GroupAlpha (F.Record GroupR) TE.ECVec = SG.contramapGroupAlpha (view DT.education4C)
              $ SG.firstOrderAlphaDC MC.eduG enumI DT.E4_HSGrad (stdNormalBP $ alphaNDS (SMB.groupSizeE MC.eduG `TE.minusE` TE.intE 1) "Edu")
      raceAG  :: SG.GroupAlpha (F.Record GroupR) TE.ECVec = SG.contramapGroupAlpha (view DT.race5C)
               $ SG.firstOrderAlphaDC MC.raceG enumI DT.R5_WhiteNonHispanic (stdNormalBP $ alphaNDS (SMB.groupSizeE MC.raceG `TE.minusE` TE.intE 1) "Race")
      refAge = DT.A5_45To64
      refEducation = DT.E4_HSGrad
      refRace = DT.R5_WhiteNonHispanic
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
    MC.WeightedAggregation MC.ContinuousBinomial -> do
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
    MC.WeightedAggregation MC.BetaProportion -> do
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
  RegistrationOnly regConfig@(MC.RegistrationConfig _ mc) -> do
    mData <- registrationModelData regConfig
    paramSetup <- setupParameters Nothing states mc
    cc <- case mData of
      MC.ModelData x -> pure x
--      _ -> SMB.stanBuildError "ModelCommon2: PT_ModelData given for registration only model"
    (Components modelM llM ppM centerF) <- components Nothing cc paramSetup mc.mcSurveyAggregation
    modelM
    when rc.rcIncludePPCheck $ void ppM
    when rc.rcIncludeLL llM
    case rc.rcPS of
      Nothing -> pure ()
      Just gtt -> do
        psRowTag <- SMB.dataSetTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData"
        postStratifyOne psRowTag (view DT.popCount) F.rcast "R" paramSetup Nothing Nothing mc.mcDesignMatrixRow (Just $ centerF SC.GQData) Nothing gtt >> pure ()

  TurnoutOnly turnoutConfig@(MC.TurnoutConfig _ mc) -> do
    mData <- turnoutModelData turnoutConfig
    paramSetup <- setupParameters Nothing states mc
    (Components modelM llM ppM centerF) <- components Nothing (MC.covariatesAndCounts mData) paramSetup mc.mcSurveyAggregation
    modelM
    when rc.rcIncludePPCheck $ void ppM
    when rc.rcIncludeLL llM
    case rc.rcPS of
      Nothing -> pure ()
      Just gtt -> do
        psRowTag <- SMB.dataSetTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData"
        postStratifyOne psRowTag (view DT.popCount) F.rcast "T" paramSetup Nothing Nothing mc.mcDesignMatrixRow (Just $ centerF SC.GQData) Nothing gtt >> pure ()

  PrefOnly prefConfig@(MC.PrefConfig mc) -> do
    mData <- prefModelData prefConfig
    paramSetup <- setupParameters Nothing states mc
    (Components modelM llM ppM centerF) <- components Nothing (MC.covariatesAndCounts mData) paramSetup mc.mcSurveyAggregation
    modelM
    when rc.rcIncludePPCheck $ void ppM
    when rc.rcIncludeLL llM
    case rc.rcPS of
      Nothing -> pure ()
      Just gtt -> do
        psRowTag <- SMB.dataSetTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData"
        postStratifyOne psRowTag (view DT.popCount) F.rcast "P" paramSetup Nothing Nothing mc.mcDesignMatrixRow (Just $ centerF SC.GQData) Nothing gtt >> pure ()

  TurnoutAndPref tConfig@(MC.TurnoutConfig _ tMC) pConfig@(MC.PrefConfig pMC) -> do
    tData <- turnoutModelData tConfig
    pData <- prefModelData pConfig
    tParamS <- setupParameters (Just "T") states tMC
    pParamS <- setupParameters (Just "P") states pMC
    (Components tModelM _ _ tCenterF) <- components (Just "T") (MC.covariatesAndCounts tData) tParamS tMC.mcSurveyAggregation
    (Components pModelM _ _ pCenterF) <- components (Just "P") (MC.covariatesAndCounts pData) pParamS pMC.mcSurveyAggregation
    tModelM
    pModelM
    case rc.rcPS of
      Nothing -> pure ()
      Just gtt -> do
        psRowTag <- SMB.dataSetTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData"
        psWgts <- SBB.addCountData psRowTag "PSWgts" (view DT.popCount) --postStratificationWeights psRowTag (view )
        tProbsCW <- postStratificationProbsCW psRowTag F.rcast "T" tParamS tMC.mcDesignMatrixRow (Just $ tCenterF SC.GQData) Nothing gtt
        pProbsCW <- postStratificationProbsCW psRowTag F.rcast "P" pParamS pMC.mcDesignMatrixRow (Just $ pCenterF SC.GQData) Nothing gtt
        let psDataGrpIndex = SMB.byGroupIndexE psRowTag gtt
            eltMultiply = TE.binaryOpE (TEO.SElementWise TEO.SMultiply)
            wgtsMCW = TE.NeedsCW $ fmap (`eltMultiply` SF.toVec psWgts) tProbsCW
        SBB.postStratifiedParameterF False SMB.SBGeneratedQuantities (Just "DVS_byGrp") psRowTag gtt psDataGrpIndex wgtsMCW pProbsCW Nothing >> pure ()

runModel :: forall l k lk r a b .
            (K.KnitEffects r
            , BRCC.CacheEffects r
            , l F.⊆ DP.PSDataR k
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

  let datSuffix = SC.rinData runnerInputNames
      jsonData t = "jsonData_" <> datSuffix <> "$" <> t
      registered = jsonData "Registered"
      voted = jsonData "Voted"
      surveyed = jsonData "Surveyed"
      dVotes = jsonData "DVotes"
      votesInRace = jsonData "VotesInRace"
      rSuffix = SC.rinModel runnerInputNames <> "_" <> datSuffix
      unwraps = case config of
        RegistrationOnly (MC.RegistrationConfig _ mc) -> case mc.mcSurveyAggregation of
          MC.WeightedAggregation MC.BetaProportion -> [SR.UnwrapExpr (registered <> " / " <> surveyed) ("yRegistrationRate_" <> rSuffix)]
          _ -> [SR.UnwrapNamed "Registered" ("yRegistered_" <> rSuffix)]
        TurnoutOnly (MC.TurnoutConfig _ mc) -> case mc.mcSurveyAggregation of
          MC.WeightedAggregation MC.BetaProportion -> [SR.UnwrapExpr (voted <> " / " <> surveyed) ("yTurnoutRate_" <> rSuffix)]
          _ -> [SR.UnwrapNamed "Turnout" ("yTurnout_" <> rSuffix)]
        PrefOnly (MC.PrefConfig mc) -> case mc.mcSurveyAggregation of
          MC.WeightedAggregation MC.BetaProportion -> [SR.UnwrapExpr (dVotes <> " / " <> votesInRace) ("yDVotesRate_" <> rSuffix)]
          _ -> [SR.UnwrapNamed "DVotes" ("yDVotes_" <> rSuffix)]
        TurnoutAndPref _ _  -> [] -- what is a PP check for this combo case??

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

turnoutSurveyA :: Config a b -> Maybe (MC.TurnoutSurvey a)
turnoutSurveyA (RegistrationOnly (MC.RegistrationConfig rs _)) = Just rs
turnoutSurveyA (TurnoutOnly (MC.TurnoutConfig ts _)) = Just ts
turnoutSurveyA (PrefOnly _) = Just MC.CESSurvey
turnoutSurveyA (TurnoutAndPref (MC.TurnoutConfig _ _) _) = Nothing

dmrA :: Config a b -> Maybe (DM.DesignMatrixRow (F.Record DP.LPredictorsR))
dmrA (RegistrationOnly (MC.RegistrationConfig _ mc)) = Just mc.mcDesignMatrixRow
dmrA (TurnoutOnly (MC.TurnoutConfig _ (MC.ModelConfig _ _ dmr))) = Just dmr
dmrA (PrefOnly (MC.PrefConfig (MC.ModelConfig _ _ dmr))) = Just dmr
dmrA (TurnoutAndPref (MC.TurnoutConfig _ _) _) = Nothing



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
        mdMeansLM :: MC.TurnoutSurvey a -> DM.DesignMatrixRow (F.Record DP.LPredictorsR) -> [Double]
        mdMeansLM ts dmr = case ts of
          MC.CESSurvey -> FL.fold (FL.premap (F.rcast @DP.LPredictorsR) $ mdMeansFld dmr) $ DP.cesData modelData
          MC.CPSSurvey -> FL.fold (FL.premap (F.rcast @DP.LPredictorsR) $ mdMeansFld dmr) $ DP.cpsData modelData
        getVector n = K.knitEither $ SP.getVector . fmap CS.mean <$> SP.parse1D n (CS.paramStats summary)
        betaSIF :: DM.DesignMatrixRow (F.Record DP.LPredictorsR) -> [Double] -> K.Sem r (VU.Vector (Double, Double))
        betaSIF dmr mdMeansL = do
          case DM.rowLength dmr of
            0 -> pure VU.empty
            _p -> do
              betaV <- getVector "beta"
              pure $ VU.fromList $ zip (V.toList betaV) mdMeansL
    betaSIM <- sequence $ (betaSIF <$> dmrA config <*> (mdMeansLM <$> turnoutSurveyA config <*> dmrA config))
    psMap <- case runConfig.rcPS of
      Nothing -> mempty
      Just _ -> case gqDataAndIndexes_CM of
        Nothing -> K.knitError "modelResultAction: Expected gq data and indexes but got Nothing."
        Just gqDaI_C -> do
          let getVectorPcts n = K.knitEither $ SP.getVector . fmap CS.percents <$> SP.parse1D n (CS.paramStats summary)
              psPrefix = case config of
                RegistrationOnly _ -> "R"
                TurnoutOnly _ -> "T"
                PrefOnly _ -> "P"
                TurnoutAndPref _ _ -> "DVS"
          (_, gqIndexesE) <- K.ignoreCacheTime gqDaI_C
          grpIM <- K.knitEither
             $ gqIndexesE >>= SMB.getGroupIndex (SMB.RowTypeTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData") (MC.psGroupTag @l)
          psTByGrpV <- getVectorPcts $ psPrefix <> "_byGrp"
          K.knitEither $ M.fromList . zip (IM.elems grpIM) <$> (traverse MT.listToCI $ V.toList psTByGrpV)
    pure $ (MC.PSMap psMap, ModelParameters <$> betaSIM)
