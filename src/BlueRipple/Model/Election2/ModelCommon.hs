{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
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

module BlueRipple.Model.Election2.ModelCommon
  (
    module BlueRipple.Model.Election2.ModelCommon
  )
where

import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Data.Types.Demographic as DT
import qualified BlueRipple.Data.Types.Geographic as GT

import Control.Lens (view)
import qualified Data.Map.Strict as M

import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Data.Dependent.Sum as DSum

import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Serialize as FS
import qualified Frames.Transform as FT
import qualified Frames.Streamly.InCore as FSI

import qualified Numeric

import qualified Stan as S
import qualified Stan.Libraries.BuildingBlocks as SBB (rowLength)

import qualified Flat
import Flat.Instances.Vector ()

-- design matrix rows
safeLog :: Double -> Double
safeLog x = if x >= 1 then Numeric.log x else 0

stateG :: S.GroupTypeTag Text
stateG = S.GroupTypeTag "State"

psGroupTag :: forall k . Typeable k => S.GroupTypeTag (F.Record k)
psGroupTag = S.GroupTypeTag "PSGrp"

ageG :: S.GroupTypeTag DT.Age5
ageG = S.GroupTypeTag "Age"

sexG :: S.GroupTypeTag DT.Sex
sexG = S.GroupTypeTag "Sex"

eduG :: S.GroupTypeTag DT.Education4
eduG = S.GroupTypeTag "Edu"

raceG :: S.GroupTypeTag DT.Race5
raceG = S.GroupTypeTag "Race"

--psGroupTag :: forall k . Typeable k => S.GroupTypeTag (F.Record k)
--psGroupTag = S.GroupTypeTag "PSGrp"

data Alphas = A | S | E | R
            | A_S | A_E | S_E | S_R
            | AS
            | A_S_AS
            | A_E_AE
            | A_S_E_R
            | A_S_E_R_AS_AE_AR_SE_SR_ER
            | St_A_S_E_R
            | St_A_S_E_R_ER
            | St_A_S_E_R_AR
            | St_A_S_E_R_StR
            | St_A_S_E_R_StA
            | St_A_S_E_R_ER_StR
            | St_A_S_E_R_AE_AR_ER_StR
            | St_A_S_E_R_ER_StE_StR
            | St_A_S_E_R_ER_StA_StE_StR
            | St_A_S_E_R_ER_StR_StER
            | St_A_S_E_R_StA_StS_StE_StR_AS_AE_AR_SE_SR_ER_StER deriving stock (Eq, Ord, Show)

alphasText :: Alphas -> Text
alphasText = \case
 St_A_S_E_R_StA_StS_StE_StR_AS_AE_AR_SE_SR_ER_StER -> "Fst_2nd_StER"
 x -> show x
{-
alphasText St_A_S_E_R = "St_A_S_E_R"
alphasText St_A_S_E_R_ER = "St_A_S_E_R_ER"
alphasText St_A_S_E_R_StR = "St_A_S_E_R_StR"
alphasText St_A_S_E_R_ER_StR = "St_A_S_E_R_ER_StR"
alphasText St_A_S_E_R_ER_StR_StER = "St_A_S_E_R_ER_StR_StER"
-}

data ModelConfig (b :: S.EType) =
  ModelConfig
  {
    mcSurveyAggregation :: SurveyAggregation b
  , mcAlphas :: Alphas
  , mcDesignMatrixRow :: S.DesignMatrixRow (F.Record DP.LPredictorsR)
  }

modelConfigText :: ModelConfig b -> Text
modelConfigText (ModelConfig sa alphas dmr) =  aggregationText sa <> "_" <> alphasText alphas <> "_" <> dmr.dmName

data ModelCategory = Reg | Vote deriving stock (Show, Eq)

data ActionConfig a b =
  ActionConfig
  {
    acSurvey :: ActionSurvey a
  , acModelConfig :: ModelConfig b
  }

data PrefConfig b =
  PrefConfig
  {
    pcSurveyPortion :: DP.SurveyPortion -- we know it's CES but not what portion
  , pcModelConfig :: ModelConfig b
  }
{-
-- types to terms
class ModelCategoryV a where
  modelCategory :: ModelCategory

instance ModelCategoryV Reg where
  modelCategory = Reg

instance ModelCategoryV Vote where
  modelCategory = Vote

instance ModelCategoryV (ActionConfig Reg a b) where
  modelCategory = Reg

instance ModelCategoryV (ActionConfig Vote a b) where
  modelCategory = Vote


instance ModelCategoryV (PrefConfig Reg b) where
  modelCategory = Reg

instance ModelCategoryV (PrefConfig Vote b) where
  modelCategory = Vote
-}

type GroupsR = GT.StateAbbreviation ': DP.DCatsR

groups :: Foldable g => g Text -> [DSum.DSum S.GroupTypeTag (S.GroupFromData (F.Record GroupsR))]
groups states = [stateG DSum.:=> S.groupFromDataFoldable (view GT.stateAbbreviation) states
                , ageG DSum.:=> S.groupFromDataEnum (view DT.age5C)
                , sexG DSum.:=> S.groupFromDataEnum (view DT.sexC)
                , eduG DSum.:=> S.groupFromDataEnum (view DT.education4C)
                , raceG DSum.:=> S.groupFromDataEnum (view DT.race5C)
                ]

addGroupIndexesAndIntMaps :: forall d rs . (GroupsR F.⊆ rs)
        => [DSum.DSum S.GroupTypeTag (S.GroupFromData (F.Record GroupsR))]
        -> S.RowTypeTag (F.Record rs)
        -> S.StanDataBuilderEff S.ModelDataT d (S.RowTypeTag (F.Record rs))
addGroupIndexesAndIntMaps groups' dataTag = do
  let modelIDT :: S.InputDataType S.ModelDataT d = S.ModelData
  S.addGroupIndexes modelIDT dataTag F.rcast groups'
  S.addGroupIntMaps modelIDT dataTag F.rcast groups'
  pure dataTag

surveyDataGroupBuilder :: (Typeable d, Foldable g, Typeable rs, GroupsR F.⊆ rs)
                       => g Text -> Text -> S.ToFoldable d (F.Record rs) -> S.StanDataBuilderEff S.ModelDataT d  (S.RowTypeTag (F.Record rs))
surveyDataGroupBuilder states sName sTF = S.addData sName S.ModelData sTF >>= addGroupIndexesAndIntMaps (groups states)

-- NB: often l ~ k, e.g., for predicting district turnout/preference
-- But say you want to predict turnout by race, nationally.
-- Now l ~ '[Race5C]
-- How about turnout by Education in each state? Then l ~ [StateAbbreviation, Education4C]
psGroupBuilder :: forall g k l .
                 (Foldable g
                 , Typeable k
                 , Typeable (DP.PSDataR k)
                 , Show (F.Record l)
                 , Ord (F.Record l)
                 , l F.⊆ DP.PSDataR k
                 , Typeable l
                 , F.ElemOf (DP.PSDataR k) GT.StateAbbreviation
                 , DP.DCatsR F.⊆ DP.PSDataR k
                 , FSI.RecVec (DP.PSDataR k)
                 , F.ElemOf (DP.PSDataR k) DT.PopCount
                 )
               => g Text
               -> g (F.Record l)
               -> S.StanDataBuilderEff S.GQDataT (DP.PSData k) (S.RowTypeTag (F.Record (DP.PSDataR k)))
psGroupBuilder states psKeys = do
  let groups' = groups states
      psIDT :: S.InputDataType S.GQDataT (DP.PSData k) = S.GQData
  -- rows with 0 weight add nothing to the sums but add space to json and time to computation
  psTag <- S.addData "PSData" psIDT (S.ToFoldable $ F.filterFrame ((> 0) . view DT.popCount) . DP.unPSData)
  psGtt <- fst <$> S.addGroup @(F.Record l) psIDT "PSGrp" (length psKeys)
  S.addGroupIndexForData psIDT psGtt psTag $ S.makeIndexFromFoldable show F.rcast psKeys
  S.addGroupIntMapForData psIDT psGtt psTag $ S.dataToIntMapFromFoldable F.rcast psKeys
  S.addGroupIndexes psIDT psTag F.rcast groups'
  pure psTag

-- design matrix rows
tDesignMatrixRow_0 :: S.DesignMatrixRow (F.Record DP.LPredictorsR)
tDesignMatrixRow_0 = S.DesignMatrixRow "0" []


tDesignMatrixRow_d :: Text -> S.DesignMatrixRow (F.Record DP.LPredictorsR)
tDesignMatrixRow_d t = S.DesignMatrixRow ("d" <> t) [dRP]
  where
    dRP = S.DesignMatrixRowPart "logDensity" 1 (VU.singleton . safeLog . view DT.pWPopPerSqMile)

data ModelType = ActionMT | PreferenceMT | FullMT deriving stock (Eq, Ord, Show)

data ActionSurvey a where
  CESSurvey :: DP.SurveyPortion -> ActionSurvey (F.Record DP.CESByCDR)
  CPSSurvey :: ActionSurvey (F.Record DP.CPSByStateR)

data RealCountModel = ContinuousBinomial | BetaProportion deriving stock (Eq)

realCountModelText :: RealCountModel -> Text
realCountModelText ContinuousBinomial = "CB"
realCountModelText BetaProportion = "BP"

data SurveyAggregation b where
  UnweightedAggregation :: SurveyAggregation S.EIntArray
  RoundedWeightedAggregation :: DP.WeightingStyle -> SurveyAggregation S.EIntArray
  WeightedAggregation :: RealCountModel -> DP.WeightingStyle -> SurveyAggregation S.ECVec

actionSurveyText :: ActionSurvey a -> Text
actionSurveyText (CESSurvey sp) = "CES_" <> DP.surveyPortionText sp
actionSurveyText CPSSurvey = "CPS"

data PSTargets = NoPSTargets | PSTargets deriving stock (Eq, Ord, Show)
psTargetsText :: PSTargets -> Text
psTargetsText NoPSTargets = "noPSTgt"
psTargetsText PSTargets = "PSTgt"

aggregationText :: SurveyAggregation b -> Text
aggregationText UnweightedAggregation = "UW"
aggregationText (RoundedWeightedAggregation ws) = "RW" <> DP.weightingStyleText ws
aggregationText (WeightedAggregation cm ws ) = "WA" <> realCountModelText cm <> DP.weightingStyleText ws

addAggregationText :: SurveyAggregation b -> Text
addAggregationText UnweightedAggregation = "_UW"
addAggregationText (RoundedWeightedAggregation ws) = "_RW" <> DP.weightingStyleText ws
addAggregationText (WeightedAggregation cm ws) = "_WA" <> realCountModelText cm <> DP.weightingStyleText ws

data BinomialData (b :: S.EType) =
  BinomialData
  {
    bdTrials :: S.UExpr b
  , bdSuccesses :: S.UExpr b
  }

binomialData :: S.RowTypeTag a
             -> (S.RowTypeTag a -> S.StanModelBuilderEff md gq (S.UExpr b))
             -> (S.RowTypeTag a -> S.StanModelBuilderEff md gq (S.UExpr b))
             -> S.StanModelBuilderEff md gq (BinomialData b)
binomialData rtt trialsF succF = do
  trialsE <- trialsF rtt --SBB.addCountData surveyDataTag "Surveyed" (view DP.surveyed)
  successesE <- succF rtt --SBB.addCountData surveyDataTag "Voted" (view DP.voted)
  pure $ BinomialData trialsE successesE

data CovariatesAndCounts a (b :: S.EType) =
  CovariatesAndCounts
  {
    ccSurveyDataTag :: S.RowTypeTag a
  , ccNCovariates :: S.IntE
  , ccCovariates :: S.MatrixE
  , ccBinomialData :: BinomialData b
  }

covariatesAndCountsFromData :: forall md gq rs b . DP.LPredictorsR F.⊆ rs
                            => S.RowTypeTag (F.Record rs)
                            -> ModelConfig b
                            -> (S.RowTypeTag (F.Record rs) -> S.StanModelBuilderEff md gq (S.UExpr b))
                            -> (S.RowTypeTag (F.Record rs) -> S.StanModelBuilderEff md gq (S.UExpr b))
                            -> S.StanModelBuilderEff md gq (CovariatesAndCounts (F.Record rs) b)
covariatesAndCountsFromData rtt modelConfig trialsF succF = do
  trialsE <- trialsF rtt --SBB.addCountData surveyDataTag "Surveyed" (view DP.surveyed)
  successesE <- succF rtt --SBB.addCountData surveyDataTag "Voted" (view DP.voted)
  let (_, nCovariatesE) = S.designMatrixColDimBinding modelConfig.mcDesignMatrixRow Nothing
      modelIDT :: S.InputDataType S.ModelDataT md = S.ModelData
  dmE <- if SBB.rowLength modelConfig.mcDesignMatrixRow > 0
    then S.addDesignMatrix modelIDT rtt (contramap F.rcast modelConfig.mcDesignMatrixRow) Nothing
    else pure $ S.namedE "ERROR" S.SMat -- this shouldn't show up in stan code at all
  pure $ CovariatesAndCounts rtt nCovariatesE dmE $ BinomialData trialsE successesE


data ModelData a (b :: S.EType) where
  ModelData :: CovariatesAndCounts a b -> ModelData a b
--  PT_ModelData :: CovariatesAndCounts a b -> StateTargetsData tds -> ModelData tds a b

covariatesAndCounts :: ModelData a b -> CovariatesAndCounts a b
covariatesAndCounts (ModelData cc) = cc
--covariatesAndCounts (PT_ModelData cc _) = cc

withCC :: (forall x y . CovariatesAndCounts x y -> c) -> ModelData a b -> c
withCC f (ModelData cc) = f cc
--withCC f (PT_ModelData cc _) = f cc

data RunConfig l = RunConfig { rcIncludePPCheck :: Bool, rcIncludeLL :: Bool, rcPS :: Maybe (S.GroupTypeTag (F.Record l)) }

newtype PSMap l a = PSMap { unPSMap :: Map (F.Record l) a} deriving newtype (Functor)

instance (V.RMap l, Ord (F.Record l), FS.RecFlat l, Flat.Flat a) => Flat.Flat (PSMap l a) where
  size (PSMap m) n = Flat.size (fmap (first  FS.toS) $ M.toList m) n
  encode (PSMap m) = Flat.encode (fmap (first  FS.toS) $ M.toList m)
  decode = (\sl -> PSMap $ M.fromList $ fmap (first FS.fromS) sl) <$> Flat.decode

psMapToFrame :: forall t l a . (V.KnownField t, V.Snd t ~ a, FSI.RecVec (l V.++ '[t]))
             => PSMap l a -> F.FrameRec (l V.++ '[t])
psMapToFrame (PSMap m) = psMapRecToFrame $ PSMap $ fmap (FT.recordSingleton @t) m

psMapRecToFrame :: FSI.RecVec (l V.++ rs) =>  PSMap l (F.Record rs) -> F.FrameRec (l V.++ rs)
psMapRecToFrame (PSMap m) = F.toFrame $ fmap toRec $ M.toList m
  where toRec (l, r) = l F.<+> r

{-
-- NB: often l ~ k, e.g., for predicting district turnout/preference
-- But say you want to predict turnout by race, nationally.
-- Now l ~ '[Race5C]
-- How about turnout by Education in each state? Then l ~ [StateAbbreviation, Education4C]
stateGroupBuilder :: forall g k l a b .
                     (Foldable g
                     , Typeable (DP.PSDataR k)
                     , Show (F.Record l)
                     , Ord (F.Record l)
                     , l F.⊆ DP.PSDataR k
                     , Typeable l
                     , F.ElemOf (DP.PSDataR k) GT.StateAbbreviation
                     )
                  => TurnoutConfig a b
                  -> g Text
                  -> g (F.Record l)
                  -> S.StanGroupBuilderM DP.ModelData (DP.PSData k) ()
stateGroupBuilder turnoutConfig states psKeys = do
  let saF :: F.ElemOf rs GT.StateAbbreviation => F.Record rs -> Text
      saF = view GT.stateAbbreviation
      addSurveyIndexes :: F.ElemOf rs GT.StateAbbreviation => S.RowTypeTag (F.Record rs) -> S.StanGroupBuilderM DP.ModelData gq ()
      addSurveyIndexes surveyDataTag = do
        S.addGroupIndexForData stateG surveyDataTag $ S.makeIndexFromFoldable show saF states
        S.addGroupIntMapForDataSet stateG surveyDataTag $ S.dataToIntMapFromFoldable saF states
  -- the return type must be explcit here so GHC knows the GADT type parameter does not escape its scope
  () <- case turnoutConfig.tSurvey of
    CESSurvey -> S.addModelDataToGroupBuilder "SurveyData" (S.ToFoldable DP.cesData) >>= addSurveyIndexes
    CPSSurvey -> S.addModelDataToGroupBuilder "SurveyData" (S.ToFoldable DP.cpsData) >>= addSurveyIndexes
  case turnoutConfig.tPSTargets of
    NoPSTargets -> pure ()
    PSTargets -> do
      turnoutTargetDataTag <- S.addModelDataToGroupBuilder "TurnoutTargetData" (S.ToFoldable DP.stateTurnoutData)
      S.addGroupIndexForData stateG turnoutTargetDataTag $ S.makeIndexFromFoldable show (view GT.stateAbbreviation) states
      acsDataTag <- S.addModelDataToGroupBuilder "ACSData" (S.ToFoldable DP.acsData)
      S.addGroupIndexForData stateG acsDataTag $ S.makeIndexFromFoldable show saF states
  let psGtt = psGroupTag @l
  psTag <- S.addGQDataToGroupBuilder "PSData" (S.ToFoldable DP.unPSData)
  S.addGroupIndexForData psGtt psTag $ S.makeIndexFromFoldable show F.rcast psKeys
  S.addGroupIntMapForDataSet psGtt psTag $ S.dataToIntMapFromFoldable F.rcast psKeys
  S.addGroupIndexForData stateG psTag $ S.makeIndexFromFoldable show saF states
--  S.addGroupIntMapForDataSet stateG psTag $ S.dataToIntMapFromFoldable saF states

turnoutModelData :: forall a b gq . TurnoutConfig a b
                 -> S.StanBuilderM DP.ModelData gq (TurnoutModelData a b)
turnoutModelData tc = do
  let cpsCC_UW :: S.StanBuilderM DP.ModelData gq (CovariatesAndCounts (F.Record DP.CPSByStateR) S.EIntArray)
      cpsCC_UW = do
        surveyDataTag <- S.dataSetTag @(F.Record DP.CPSByStateR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = S.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if S.rowLength tc.tDesignMatrixRow > 0
          then S.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ S.namedE "ERROR" S.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addCountData surveyDataTag "Surveyed" (view DP.surveyed)
        successesE <- SBB.addCountData surveyDataTag "Voted" (view DP.voted)
        pure $ CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
      cpsCC_W :: S.StanBuilderM DP.ModelData gq (CovariatesAndCounts (F.Record DP.CPSByStateR) S.ECVec)
      cpsCC_W = do
        surveyDataTag <- S.dataSetTag @(F.Record DP.CPSByStateR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = S.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if S.rowLength tc.tDesignMatrixRow > 0
          then S.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ S.namedE "ERROR" S.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addRealData surveyDataTag "Surveyed" (Just 0.99) Nothing (view DP.surveyedW)
        successesE <- SBB.addRealData surveyDataTag "Voted" (Just 0) Nothing (view DP.votedW)
        pure $ CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
      cesCC_UW :: S.StanBuilderM DP.ModelData gq (CovariatesAndCounts (F.Record DP.CESByCDR) S.EIntArray)
      cesCC_UW = do
        surveyDataTag <- S.dataSetTag @(F.Record DP.CESByCDR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = S.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if S.rowLength tc.tDesignMatrixRow > 0
          then S.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ S.namedE "ERROR" S.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addCountData surveyDataTag "Surveyed" (view DP.surveyed)
        successesE <- SBB.addCountData surveyDataTag "Voted" (view DP.voted)
        pure $ CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
      cesCC_W :: S.StanBuilderM DP.ModelData gq (CovariatesAndCounts (F.Record DP.CESByCDR) S.ECVec)
      cesCC_W = do
        surveyDataTag <- S.dataSetTag @(F.Record DP.CESByCDR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = S.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if S.rowLength tc.tDesignMatrixRow > 0
          then S.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ S.namedE "ERROR" S.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addRealData surveyDataTag "Surveyed" (Just 0.99) Nothing (view DP.surveyedW)
        successesE <- SBB.addRealData surveyDataTag "Voted" (Just 0) Nothing (view DP.votedW)
        pure $ CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
  case tc.tPSTargets of
    NoPSTargets -> case tc.tSurvey of
      CPSSurvey -> case tc.tSurveyAggregation of
        UnweightedAggregation -> fmap NoPT_TurnoutModelData cpsCC_UW
        WeightedAggregation _ -> fmap NoPT_TurnoutModelData cpsCC_W
      CESSurvey -> case tc.tSurveyAggregation of
        UnweightedAggregation -> fmap NoPT_TurnoutModelData cesCC_UW
        WeightedAggregation _ -> fmap NoPT_TurnoutModelData cesCC_W
    PSTargets -> do
      stateTurnoutTargetTag <- S.dataSetTag @(F.Record BRDF.StateTurnoutCols) SC.ModelData "TurnoutTargetData"
      turnoutBallotsCountedVAP <- SBB.addRealData stateTurnoutTargetTag "BallotsCountedVAP" (Just 0) (Just 1) (view BRDF.ballotsCountedVAP)
      acsTag <- S.dataSetTag @(F.Record DDP.ACSa5ByStateR) SC.ModelData "ACSData"
      acsWgts <- SBB.addCountData acsTag "ACSWgts" (view DT.popCount)
      acsCovariates <- if S.rowLength tc.tDesignMatrixRow > 0
          then S.addDesignMatrix acsTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ S.namedE "ERROR" S.SMat -- this shouldn't show up in stan code at all
      let std = StateTargetsData stateTurnoutTargetTag turnoutBallotsCountedVAP acsTag acsWgts acsCovariates
      case tc.tSurvey of
        CPSSurvey -> case tc.tSurveyAggregation of
          UnweightedAggregation -> fmap (\x -> PT_TurnoutModelData x std) cpsCC_UW
          WeightedAggregation _ -> fmap (\x -> PT_TurnoutModelData x std) cpsCC_W
        CESSurvey -> case tc.tSurveyAggregation of
          UnweightedAggregation -> fmap (\x -> PT_TurnoutModelData x std) cesCC_UW
          WeightedAggregation _ -> fmap (\x -> PT_TurnoutModelData x std) cesCC_W



turnoutModelText :: TurnoutConfig a b -> Text
turnoutModelText (TurnoutConfig ts tsa tPs dmr am) = "Turnout" <> turnoutSurveyText ts
                                                 <> (if (tPs == PSTargets) then "_PSTgt" else "")
                                                 <> addAggregationText tsa
                                                 <> "_" <> dmr.dmName <> "_" <> stateAlphaModelText am

turnoutModelDataText :: TurnoutConfig a b -> Text
turnoutModelDataText (TurnoutConfig ts tsa tPs dmr _) = "Turnout" <> turnoutSurveyText ts
                                                    <> (if (tPs == PSTargets) then "_PSTgt" else "")
                                                    <> addAggregationText tsa
                                                    <> "_" <> dmr.dmName


-- for now we model only alpha hierarchically. Beta will be the same everywhere.
data TurnoutPrediction =
  TurnoutPrediction
  {
    tpAlphaMap :: Map Text Double
  , tpBetaSI :: VU.Vector (Double, Double)
  , tpLogisticAdjMapM :: Maybe (Map Text Double)
  } deriving stock (Generic)

deriving anyclass instance Flat.Flat TurnoutPrediction

predictedTurnoutP :: TurnoutConfig a b -> TurnoutPrediction -> Text -> F.Record DP.PredictorsR -> Either Text Double
predictedTurnoutP tc tp sa p = do
  alpha <- case M.lookup sa tp.tpAlphaMap of
    Nothing -> Left $ "Model.Election2.ModelCommon.predictedP: alphaMap lookup failed for k=" <> sa
    Just x -> pure x
  logisticAdj <- case tp.tpLogisticAdjMapM of
    Nothing -> pure 0
    Just m -> case M.lookup sa m of
      Nothing -> Left $ "Model.Election2.ModelCommon.predictedP: pdLogisticAdjMap lookup failed for k=" <> show sa
      Just x -> pure x
  let covariatesV = S.designMatrixRowF tc.tDesignMatrixRow p
      invLogit x = 1 / (1 + exp (negate x))
      applySI x (s, i) = i + s * x
  pure $ invLogit (alpha + VU.sum (VU.zipWith applySI covariatesV tp.tpBetaSI) + logisticAdj)

-- we can use the designMatrixRow to get (a -> Vector Double) for prediction
data TurnoutConfig a (b :: S.EType) =
  TurnoutConfig
  {
    tSurvey :: TurnoutSurvey a
  , tSurveyAggregation :: SurveyAggregation b
  , tPSTargets :: PSTargets
  , tDesignMatrixRow :: S.DesignMatrixRow (F.Record DP.PredictorsR)
  , tStateAlphaModel :: StateAlpha
  }

tDesignMatrixRow_d_A_S_E_R :: S.DesignMatrixRow (F.Record DP.PredictorsR)
tDesignMatrixRow_d_A_S_E_R = S.DesignMatrixRow "d_A_S_E_R" [dRP, aRP, sRP, eRP, rRP]
  where
    dRP = S.DesignMatrixRowPart "logDensity" 1 (VU.singleton . safeLog . view DT.pWPopPerSqMile)
    aRP = S.boundedEnumRowPart (Just DT.A5_18To24) "Age" (view DT.age5C)
    sRP = S.boundedEnumRowPart Nothing "Sex" (view DT.sexC)
    eRP = S.boundedEnumRowPart (Just DT.E4_NonHSGrad) "Edu" (view DT.education4C)
    rRP = S.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (view DT.race5C)

tDesignMatrixRow_d_A_S_E_R_RE :: S.DesignMatrixRow (F.Record DP.PredictorsR)
tDesignMatrixRow_d_A_S_E_R_RE = S.DesignMatrixRow "d_A_S_E_R_RE" [dRP, aRP, sRP, eRP, rRP, reRP]
  where
    dRP = S.DesignMatrixRowPart "logDensity" 1 (VU.singleton . safeLog . view DT.pWPopPerSqMile)
    aRP = S.boundedEnumRowPart (Just DT.A5_18To24) "Age" (view DT.age5C)
    sRP = S.boundedEnumRowPart Nothing "Sex" (view DT.sexC)
    eRP = S.boundedEnumRowPart (Just DT.E4_NonHSGrad) "Edu" (view DT.education4C)
    rRP = S.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (view DT.race5C)
    re r = S.BEProduct2 (r ^. DT.race5C, r ^. DT.education4C)
    reRP = S.boundedEnumRowPart (Just (S.BEProduct2 (DT.R5_WhiteNonHispanic, DT.E4_NonHSGrad))) "RaceEdu" re

tDesignMatrixRow_d_A_S_RE :: S.DesignMatrixRow (F.Record DP.PredictorsR)
tDesignMatrixRow_d_A_S_RE = S.DesignMatrixRow "d_A_S_RE" [dRP, aRP, sRP, reRP]
  where
    dRP = S.DesignMatrixRowPart "logDensity" 1 (VU.singleton . safeLog . view DT.pWPopPerSqMile)
    aRP = S.boundedEnumRowPart (Just DT.A5_18To24) "Age" (view DT.age5C)
    sRP = S.boundedEnumRowPart Nothing "Sex" (view DT.sexC)
    re r = S.BEProduct2 (r ^. DT.race5C, r ^. DT.education4C)
    reRP = S.boundedEnumRowPart (Just (S.BEProduct2 (DT.R5_WhiteNonHispanic, DT.E4_NonHSGrad))) "RaceEdu" re

data StateAlpha = StateAlphaSimple | StateAlphaHierCentered | StateAlphaHierNonCentered deriving stock (Show)

stateAlphaModelText :: StateAlpha -> Text
stateAlphaModelText StateAlphaSimple = "AS"
stateAlphaModelText StateAlphaHierCentered = "AHC"
stateAlphaModelText StateAlphaHierNonCentered = "AHNC"

turnoutTargetsModel :: ModelParameters
                    -> CovariatesAndCounts a b
                    -> StateTargetsData td
                    -> Maybe (S.MatrixE -> S.StanName -> S.StanBuilderM md gq S.MatrixE)
                    -> Maybe S.MatrixE -> S.StanBuilderM md gq ()
turnoutTargetsModel mp cc std cM rM = do
  (dmACS, acsNByState) <- turnoutTargetsTD cc std cM rM
  let acsStateIndex = S.byGroupIndexE std.stdACSTag stateG
      toVec x = S.functionE SF.to_vector (x :> TNil)
      pE = probabilitiesExpr mp std.stdACSTag dmACS
  acsPS <- SBB.postStratifiedParameterF False S.SBTransformedParameters (Just "psTByState") std.stdACSTag stateG acsStateIndex (toVec std.stdACSWgts) (pure pE) Nothing
  let normalDWA = S.DensityWithArgs SF.normal (S.realE 1 :> S.realE 4 :> TNil)
  sigmaTargetsP <-  DAG.simpleParameterWA
                    (S.NamedDeclSpec "sigmaTTgt" $ S.realSpec [S.lowerM $ S.realE 1])
                    normalDWA
  S.inBlock S.SBModel $ S.addFromCodeWriter $ do
    let eMult = S.binaryOpE (TEO.SElementWise TEO.SMultiply)
        eDiv = S.binaryOpE (TEO.SElementWise TEO.SDivide)
        tP = S.indexE TEI.s0 (S.byGroupIndexE std.stdTargetTypeTag stateG) std.stdStateBallotsCountedVAP
        sd1 = tP `eMult` (S.realE 1 `S.minusE` tP) `eDiv` toVec acsNByState
        sd = DAG.parameterExpr sigmaTargetsP `S.timesE` S.functionE SF.sqrt (sd1 :> TNil)
    S.addStmt $ S.sample acsPS SF.normal (tP :> sd :> TNil)

turnoutPS :: forall l md k . (Typeable (DP.PSDataR k)
                             , F.ElemOf (DP.PSDataR k) DT.PopCount
                             , DP.PredictorsR F.⊆ DP.PSDataR k
                             )
          => ModelParameters
          -> S.DesignMatrixRow (F.Record DP.PredictorsR)
          -> Maybe (S.MatrixE -> S.StanName -> S.StanBuilderM md (DP.PSData k) S.MatrixE)
          -> Maybe S.MatrixE
          -> S.GroupTypeTag l
          -> S.StanBuilderM md (DP.PSData k) ()
turnoutPS mp dmr cM rM gtt = do
  psDataTag <- S.dataSetTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData"
  let psDataGrpIndex = S.byGroupIndexE psDataTag gtt
  psWgts <- SBB.addCountData psDataTag "PSWgts" (view DT.popCount)
  let nCovariates = S.rowLength dmr
  psCovariates <-  if nCovariates > 0
                   then S.addDesignMatrix psDataTag (contramap F.rcast dmr) Nothing
                   else pure $ S.namedE "ERROR" S.SMat -- this shouldn't show up in stan code at all
  dmPS' <- case cM of
    Nothing -> pure psCovariates
    Just c -> c psCovariates "dmPS_Centered"
  dmPS <- case rM of
    Nothing -> pure dmPS'
    Just r -> S.inBlock S.SBTransformedDataGQ $ S.addFromCodeWriter $ do
      let rowsE = S.dataSetSizeE psDataTag
          colsE = S.mrfdColumnsE $ S.matrixFromRowData dmr Nothing
      S.declareRHSNW (S.NamedDeclSpec "dmPS_QR" $ S.matrixSpec rowsE colsE []) $ dmPS' `S.timesE` r
  let toVec x = S.functionE SF.to_vector (x :> TNil)
      psPE = probabilitiesExpr mp psDataTag dmPS
  _ <- SBB.postStratifiedParameterF False S.SBGeneratedQuantities (Just "tByGrp") psDataTag gtt psDataGrpIndex (toVec psWgts) (pure psPE) Nothing
  pure ()

-- given S states
-- alpha is a scalar or S col-vector
data Alpha where
  SimpleAlpha :: DAG.Parameter S.EReal -> Alpha
  HierarchicalAlpha :: S.IntArrayE -> DAG.Parameter S.ECVec -> Alpha

-- and K predictors
-- theta is K col-vector (or Nothing)
newtype Theta = Theta (Maybe (DAG.Parameter S.ECVec))

data ModelParameters where
  BinomialLogitModelParameters :: Alpha -> Theta -> ModelParameters

paramAlpha :: ModelParameters -> Alpha
paramAlpha (BinomialLogitModelParameters a _) = a

paramTheta :: ModelParameters -> Theta
paramTheta (BinomialLogitModelParameters _ t) = t

modelParameters :: S.DesignMatrixRow a -> S.RowTypeTag a -> StateAlpha -> S.StanBuilderM md gq ModelParameters
modelParameters dmr rtt sa = do
  let stdNormalDWA :: (S.TypeOneOf t [S.EReal, S.ECVec, S.ERVec], S.GenSType t) => S.DensityWithArgs t
      stdNormalDWA = S.DensityWithArgs SF.std_normal TNil
      numPredictors = S.rowLength dmr
      (_, nCovariatesE) = S.designMatrixColDimBinding dmr Nothing

  -- for now all the thetas are iid std normals
  theta <- if numPredictors > 0 then
               (Theta . Just)
               <$> DAG.simpleParameterWA
               (S.NamedDeclSpec "theta" $ S.vectorSpec nCovariatesE [])
               stdNormalDWA
             else pure $ Theta Nothing
  let nStatesE = S.groupSizeE stateG
      hierAlphaNDS = S.NamedDeclSpec "alpha" $ S.vectorSpec nStatesE []
      hierAlphaPs = do
        muAlphaP <- DAG.simpleParameterWA
                    (S.NamedDeclSpec "muAlpha" $ S.realSpec [])
                    stdNormalDWA
        sigmaAlphaP <-  DAG.simpleParameterWA
                        (S.NamedDeclSpec "sigmaAlpha" $ S.realSpec [S.lowerM $ S.realE 0])
                        stdNormalDWA
        pure (muAlphaP :> sigmaAlphaP :> TNil)
  alpha <- case sa of
    StateAlphaSimple -> do
      fmap SimpleAlpha
        $ DAG.simpleParameterWA
        (S.NamedDeclSpec "alpha" $ S.realSpec [])
        stdNormalDWA
    StateAlphaHierCentered -> do
      alphaPs <- hierAlphaPs
      indexE <- S.getGroupIndexVar rtt stateG
      fmap (HierarchicalAlpha indexE)
        $ DAG.addBuildParameter
        $ DAG.UntransformedP hierAlphaNDS [] alphaPs
        $ \(muAlphaE :> sigmaAlphaE :> TNil) m
          -> S.addStmt $ S.sample m SF.normalS (muAlphaE :> sigmaAlphaE :> TNil)
    StateAlphaHierNonCentered -> do
      alphaPs <- hierAlphaPs
      indexE <- S.getGroupIndexVar rtt stateG
      let rawNDS = S.NamedDeclSpec (S.declName hierAlphaNDS <> "_raw") $ S.decl hierAlphaNDS
      rawAlphaP <- DAG.simpleParameterWA rawNDS stdNormalDWA
      fmap (HierarchicalAlpha indexE)
        $ DAG.addBuildParameter
        $ DAG.TransformedP hierAlphaNDS []
        (rawAlphaP :> alphaPs) DAG.TransformedParametersBlock
        (\(rawE :> muAlphaE :> muSigmaE :> TNil) -> DAG.DeclRHS $ muAlphaE `S.plusE` (muSigmaE `S.timesE` rawE))
        TNil (\_ _ -> pure ())
  pure $ BinomialLogitModelParameters alpha theta


probabilitiesExpr :: ModelParameters -> S.RowTypeTag a -> S.MatrixE -> S.VectorE
probabilitiesExpr mps rtt covariatesM = S.functionE SF.inv_logit (lp :> TNil)
  where
    stateIndexE = S.byGroupIndexE rtt stateG
    lp =
      let aV = case paramAlpha mps of
            SimpleAlpha saP -> S.functionE SF.rep_vector (DAG.parameterExpr saP :> (S.dataSetSizeE rtt) :> TNil)
            HierarchicalAlpha _ haP -> S.indexE TEI.s0 stateIndexE $ DAG.parameterExpr haP
      in case paramTheta mps of
           (Theta (Just thetaP)) -> aV `S.plusE` (covariatesM `S.timesE` DAG.parameterExpr thetaP)
           _ -> aV

-- not returning anything for now
turnoutModel :: (Typeable (DP.PSDataR k)
--                , k F.⊆ DP.PSDataR k
                , F.ElemOf (DP.PSDataR k) DT.PopCount
                , DP.PredictorsR F.⊆ DP.PSDataR k
                )
             => RunConfig l
             -> TurnoutConfig a b
             -> S.StanBuilderM DP.ModelData (DP.PSData k) ()
turnoutModel rc tmc = do
  mData <- turnoutModelData tmc
  mParams <- case tmc.tSurvey of
    CESSurvey -> case mData of
      NoPT_TurnoutModelData cc -> modelParameters (contramap F.rcast tmc.tDesignMatrixRow) cc.ccSurveyDataTag tmc.tStateAlphaModel
      PT_TurnoutModelData cc _ -> modelParameters (contramap F.rcast tmc.tDesignMatrixRow) cc.ccSurveyDataTag tmc.tStateAlphaModel
    CPSSurvey -> case mData of
      NoPT_TurnoutModelData cc -> modelParameters (contramap F.rcast tmc.tDesignMatrixRow) cc.ccSurveyDataTag tmc.tStateAlphaModel
      PT_TurnoutModelData cc _ -> modelParameters (contramap F.rcast tmc.tDesignMatrixRow) cc.ccSurveyDataTag tmc.tStateAlphaModel
  let --betaNDS = S.NamedDeclSpec "beta" $ S.vectorSpec (withCC ccNCovariates mData) []
      nRowsE = withCC (S.dataSetSizeE . ccSurveyDataTag) mData
      pExpr = DAG.parameterExpr

  -- to apply the same transformation to another matrix, we center with centerF and then post-multiply by r
  -- or we could apply beta to centered matrix ?

{-
  (covariatesM, centerF) <- case paramTheta mParams of
    Theta (Just thetaP) -> do
      (centeredCovariatesE, centerF) <- S.centerDataMatrix S.DMCenterOnly (withCC ccCovariates mData) Nothing "DM"
      pure (centeredCovariatesE, centerF)
    Theta Nothing -> pure (S.namedE "ERROR" S.SMat, \_ x _ -> pure x)
-}
{-
  (covariatesM, r, centerF, mBeta) <- case paramTheta mParams of
    Theta (Just thetaP) -> do
      (centeredCovariatesE, centerF) <- S.centerDataMatrix S.DMCenterOnly (withCC ccCovariates mData) Nothing "DM"
      (dmQ, r, _, mBeta') <- S.thinQR centeredCovariatesE "DM" $ Just (pExpr thetaP, betaNDS)
      pure (dmQ, r, centerF, mBeta')
    Theta Nothing -> pure (S.namedE "ERROR" S.SMat, S.namedE "ERROR" S.SMat, \_ x _ -> pure x, Nothing)
-}
  case mData of
    PT_TurnoutModelData cc st -> turnoutTargetsModel mParams cc st Nothing Nothing --(Just $ centerF SC.ModelData) (Just r)
    _ -> pure ()

  -- model
  let covariatesM = withCC ccCovariates mData
      lpE :: Alpha -> Theta -> S.VectorE
      lpE a t =  case a of
       SimpleAlpha alphaP -> case t of
         Theta Nothing -> S.functionE SF.rep_vector (pExpr alphaP :> nRowsE :> TNil)
         Theta (Just thetaP) -> pExpr alphaP `S.plusE` (covariatesM `S.timesE` pExpr thetaP)
       HierarchicalAlpha indexE alpha -> case t of
         Theta Nothing -> S.indexE TEI.s0 indexE $ pExpr alpha
         Theta (Just thetaP) -> S.indexE TEI.s0 indexE (pExpr alpha) `S.plusE` (covariatesM `S.timesE` pExpr thetaP)

      llF :: SMD.StanDist t pts rts
          -> S.CodeWriter (S.IntE -> S.ExprList pts)
          -> S.CodeWriter (S.IntE -> S.UExpr t)
          -> S.StanBuilderM md gq ()
      llF = withCC (SBB.generateLogLikelihood . ccSurveyDataTag) mData

  () <- case tmc.tSurveyAggregation of
    UnweightedAggregation -> do
      let a = paramAlpha mParams
          t = paramTheta mParams
          model :: S.RowTypeTag a -> S.IntArrayE -> S.IntArrayE -> S.StanBuilderM md gq ()
          model rtt n k = do
            let ssF e = S.familySample (SMD.binomialLogitDist @S.EIntArray) e (n :> lpE a t :> TNil)
                rpF :: S.CodeWriter (S.IntE -> S.ExprList '[S.EInt, S.EReal])
                rpF = pure $ \nE -> n `S.at` nE :> lpE a t `S.at` nE :> TNil
                ppF = SBB.generatePosteriorPrediction rtt
                      (S.NamedDeclSpec ("predVotes") $ S.array1Spec nRowsE $ S.intSpec [])
                      SMD.binomialLogitDist rpF
                ll = llF SMD.binomialLogitDist rpF (pure $ \nE -> k `S.at` nE)
            S.inBlock S.SBModel $ S.addFromCodeWriter $ S.addStmt $ ssF k
            when rc.rcIncludePPCheck $ void ppF
            when rc.rcIncludeLL ll
      case mData of
        NoPT_TurnoutModelData cc -> model cc.ccSurveyDataTag cc.ccTrials cc.ccSuccesses
        PT_TurnoutModelData cc _ -> model cc.ccSurveyDataTag cc.ccTrials cc.ccSuccesses
    WeightedAggregation -> do
      let a = paramAlpha mParams
          t = paramTheta mParams
      realBinomialLogitDistV <- SMD.realBinomialLogitDistM @S.ECVec
      realBinomialLogitDistS <- SMD.realBinomialLogitDistSM
      let model ::  S.RowTypeTag a -> S.VectorE -> S.VectorE -> S.StanBuilderM md gq ()
          model rtt n k = do
            let ssF e = S.familySample realBinomialLogitDistV e (n :> lpE a t :> TNil)
                rpF :: S.CodeWriter (S.IntE -> S.ExprList '[S.EReal, S.EReal])
                rpF = pure $ \nE -> n `S.at` nE :> lpE a t `S.at` nE :> TNil
                ppF = SBB.generatePosteriorPrediction rtt
                      (S.NamedDeclSpec ("predVotes") $ S.array1Spec nRowsE $ S.realSpec [])
                      realBinomialLogitDistS rpF
                ll = llF realBinomialLogitDistS rpF (pure $ \nE -> k `S.at` nE)
            S.inBlock S.SBModel $ S.addFromCodeWriter $ S.addStmt $ ssF k
            when rc.rcIncludePPCheck $ void ppF
            when rc.rcIncludeLL ll
      case mData of
        NoPT_TurnoutModelData cc -> model cc.ccSurveyDataTag cc.ccTrials cc.ccSuccesses
        PT_TurnoutModelData cc _ -> model cc.ccSurveyDataTag cc.ccTrials cc.ccSuccesses

  case rc.rcTurnoutPS of
    Nothing -> pure ()
    Just gtt -> turnoutPS mParams tmc.tDesignMatrixRow Nothing Nothing gtt --(Just $ centerF SC.GQData) (Just r)
  pure ()



runModel :: forall l k r a b .
            (K.KnitEffects r
            , BRKU.CacheEffects r
            , l F.⊆ DP.PSDataR k
            , F.ElemOf (DP.PSDataR k) DT.PopCount
            , DP.PredictorsR F.⊆ DP.PSDataR k
            , V.RMap l
            , Ord (F.Record l)
            , FS.RecFlat l
            , Typeable (DP.PSDataR k)
            , F.ElemOf (DP.PSDataR k) GT.StateAbbreviation
            , Show (F.Record l)
            , Typeable l
            )
         => Either Text Text
         -> Text
         -> Text
         -> BR.CommandLine
         -> RunConfig l
         -> TurnoutConfig a b
         -> K.ActionWithCacheTime r DP.ModelData
         -> K.ActionWithCacheTime r (DP.PSData k)
         -> K.Sem r (K.ActionWithCacheTime r (TurnoutPrediction, PSMap l MT.ConfidenceInterval))
runModel modelDirE modelName gqName _cmdLine runConfig turnoutConfig modelData_C psData_C = do
  let dataName = turnoutModelDataText turnoutConfig
      runnerInputNames = SC.RunnerInputNames
                         ("br-2023-electionModel/stan/" <> modelName <> "/")
                         (turnoutModelText turnoutConfig)
                         (Just $ SC.GQNames "GQ" (dataName <> "_" <> gqName))
                         dataName
--  modelData <- K.ignoreCacheTime modelData_C
  states <- S.toList . FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) . DP.cesData <$> K.ignoreCacheTime modelData_C
  psKeys <- S.toList . FL.fold (FL.premap (F.rcast @l) FL.set) . DP.unPSData <$> K.ignoreCacheTime psData_C
  (dw, code) <-  case turnoutConfig.tSurvey of
    CESSurvey -> SMR.dataWranglerAndCode modelData_C psData_C
                 (stateGroupBuilder turnoutConfig states psKeys)
                 (turnoutModel runConfig turnoutConfig)
    CPSSurvey -> SMR.dataWranglerAndCode modelData_C psData_C
                 (stateGroupBuilder turnoutConfig states psKeys)
                 (turnoutModel runConfig turnoutConfig)

  let unwraps = [SR.UnwrapNamed "Voted" "yVoted"]

  res_C <- SMR.runModel' @BRKU.SerializerC @BRKU.CacheData
           modelDirE
           (Right runnerInputNames)
           Nothing
           dw
           code
           (modelResultAction turnoutConfig runConfig) --SC.DoNothing -- (stateModelResultAction mcWithId dmr)
           (SMR.Both unwraps) --(SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
           modelData_C
           psData_C
  K.logLE K.Info $ modelName <> " run complete."
  pure res_C

--NB: parsed summary data has stan indexing, i.e., Arrays start at 1.
modelResultAction :: forall k l r a b .
                     (Ord (F.Record l)
                     , K.KnitEffects r
                     , Typeable (DP.PSDataR k)
                     , Typeable l
                     )
                  => TurnoutConfig a b
                  -> RunConfig l
                  -> SC.ResultAction r DP.ModelData (DP.PSData k) S.DataSetGroupIntMaps () (TurnoutPrediction, PSMap l MT.ConfidenceInterval)
modelResultAction turnoutConfig runConfig = SC.UseSummary f where
  f summary _ modelDataAndIndexes_C gqDataAndIndexes_CM = do
    (modelData, resultIndexesE) <- K.ignoreCacheTime modelDataAndIndexes_C
    -- compute means of predictors because model was zero-centered in them
    let mdMeansFld :: S.DesignMatrixRow (F.Record rs) -> FL.Fold (F.Record rs) [Double]
        mdMeansFld dmr =
          let  covariates = S.designMatrixRowF dmr
               nPredictors = S.rowLength dmr
          in FL.premap (VU.toList . covariates)
             $ traverse (\n -> FL.premap (List.!! n) FL.mean) [0..(nPredictors - 1)]
        mdMeansL = case turnoutConfig.tSurvey of
          CESSurvey -> FL.fold (FL.premap (F.rcast @DP.PredictorsR) $ mdMeansFld turnoutConfig.tDesignMatrixRow) $ DP.cesData modelData
          CPSSurvey -> FL.fold (FL.premap (F.rcast @DP.PredictorsR) $ mdMeansFld turnoutConfig.tDesignMatrixRow) $ DP.cpsData modelData
    stateIM <- case turnoutConfig.tSurvey of
      CESSurvey -> K.knitEither
                   $ resultIndexesE >>= S.getGroupIndex (S.RowTypeTag @(F.Record DP.CESByCDR) SC.ModelData "SurveyData") stateG
      CPSSurvey -> K.knitEither
                   $ resultIndexesE >>= S.getGroupIndex (S.RowTypeTag @(F.Record DP.CPSByStateR) SC.ModelData "SurveyData") stateG
    let allStates = IM.elems stateIM
        getScalar n = K.knitEither $ SP.getScalar . fmap CS.mean <$> SP.parseScalar n (CS.paramStats summary)
        getVector n = K.knitEither $ SP.getVector . fmap CS.mean <$> SP.parse1D n (CS.paramStats summary)
    K.logLE K.Info $ "stateIM=" <> show stateIM
    geoMap <- case turnoutConfig.tStateAlphaModel of
      StateAlphaSimple -> do
        alpha <- getScalar "alpha"
        pure $ M.fromList $ fmap (, alpha) allStates
      _ -> do
        alphaV <- getVector "alpha"
        pure $ M.fromList $ fmap (\(stateIdx, stateAbbr) -> (stateAbbr, alphaV V.! (stateIdx - 1))) $ IM.toList stateIM
    betaSI <- case S.rowLength turnoutConfig.tDesignMatrixRow of
      0 -> pure V.empty
      p -> do
        betaV <- getVector "theta"
        pure $ V.fromList $ zip (V.toList betaV) mdMeansL
    psMap <- case runConfig.rcTurnoutPS of
      Nothing -> mempty
      Just gtt -> case gqDataAndIndexes_CM of
        Nothing -> K.knitError "modelResultAction: Expected gq data and indexes but got Nothing."
        Just gqDaI_C -> do
          let getVectorPcts n = K.knitEither $ SP.getVector . fmap CS.percents <$> SP.parse1D n (CS.paramStats summary)
          (_, gqIndexesE) <- K.ignoreCacheTime gqDaI_C
          grpIM <- K.knitEither
             $ gqIndexesE >>= S.getGroupIndex (S.RowTypeTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData") (psGroupTag @l)
          psTByGrpV <- getVectorPcts "tByGrp"
          K.knitEither $ M.fromList . zip (IM.elems grpIM) <$> (traverse MT.listToCI $ V.toList psTByGrpV)
    pure $ (TurnoutPrediction geoMap (VU.convert betaSI) Nothing, PSMap psMap)
-}
