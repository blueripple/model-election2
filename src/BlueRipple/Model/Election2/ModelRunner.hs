{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StandaloneDeriving #-}

module BlueRipple.Model.Election2.ModelRunner
  (
    module BlueRipple.Model.Election2.ModelRunner
  )
where

import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Election2.ModelCommon as MC
import qualified BlueRipple.Model.Election2.ModelCommon2 as MC2
import qualified BlueRipple.Data.Small.DataFrames as BRDF
import qualified BlueRipple.Data.Small.Loaders as BRDF
import qualified BlueRipple.Data.RDH_Voterfiles as RDH
--import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.CachingCore as BRCC
import qualified BlueRipple.Data.Types.Geographic as GT
import qualified BlueRipple.Data.Types.Demographic as DT
import qualified BlueRipple.Data.Types.Election as ET
import qualified BlueRipple.Data.Types.Modeling as MT
import qualified BlueRipple.Data.ACS_PUMS as ACS
import qualified BlueRipple.Data.CES as CES
import qualified BlueRipple.Data.Keyed as Keyed
import qualified BlueRipple.Utilities.HvegaJsonData as BRHJ
import qualified BlueRipple.Model.TurnoutAdjustment as TA

import qualified Knit.Report as K hiding (elements)

import qualified Control.Foldl as FL
import Control.Lens (Lens', view, (^.), over)

import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Set as Set
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as MM
import qualified Numeric

import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Constraints as FC
import qualified Frames.Folds as FF
import qualified Frames.Streamly.TH as FTH
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.Serialize as FS

import qualified Frames.MapReduce as FMR

import Flat.Instances.Vector ()

import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.JSON as VJ

FTH.declareColumn "ModelPr" ''Double
FTH.declareColumn "PSNumer" ''Double
FTH.declareColumn "PSDenom" ''Double
FTH.declareColumn "ModelCI" ''MT.ConfidenceInterval
FTH.declareColumn "ModelT" ''Double
FTH.declareColumn "ModelP" ''Double

data CacheStructure a b =
  CacheStructure
  {
    csModelDirE :: Either Text Text
  , csProjectCacheDirE :: Either Text Text
  , csPSName :: Text
  , csAllCellPSName :: a
  , csAllCellPSPrefix :: b
  }

modelCacheStructure :: CacheStructure a b -> CacheStructure () ()
modelCacheStructure (CacheStructure x y z _ _) = CacheStructure x y z () ()

allCellCacheStructure :: CacheStructure a b -> CacheStructure a ()
allCellCacheStructure (CacheStructure x y z xx _) = CacheStructure x y z xx ()

allCellCS :: CacheStructure Text () -> CacheStructure () ()
allCellCS (CacheStructure a b _ c _) = CacheStructure a b c () ()

cachedPreppedModelData :: (K.KnitEffects r, BRCC.CacheEffects r)
                       => CacheStructure () ()
                       -> K.Sem r (K.ActionWithCacheTime r (DP.ModelData DP.CDKeyR))
cachedPreppedModelData cacheStructure = K.wrapPrefix "cachedPreppedModelData" $ do
  cacheDirE' <- K.knitMaybe "Empty cacheDir given!" $ BRCC.insureFinalSlashE $ csProjectCacheDirE cacheStructure
  let appendCacheFile :: Text -> Text -> Text
      appendCacheFile t d = d <> t
      cpsModelCacheE = bimap (appendCacheFile "CPSModelData.bin") (appendCacheFile "CPSModelData.bin") cacheDirE'
      cesByStateModelCacheE = bimap (appendCacheFile "CESModelData.bin") (appendCacheFile "CESByStateModelData.bin") cacheDirE'
      cesByCDModelCacheE = bimap (appendCacheFile "CESModelData.bin") (appendCacheFile "CESByCDModelData.bin") cacheDirE'
  rawCESByCD_C <- DP.cesCountedDemPresVotesByCD False
  rawCESByState_C <- DP.cesCountedDemPresVotesByState False
  rawCPS_C <- DP.cpsCountedTurnoutByState
  DP.cachedPreppedModelDataCD cpsModelCacheE rawCPS_C cesByStateModelCacheE rawCESByState_C cesByCDModelCacheE rawCESByCD_C

runBaseModel ::  forall l r ks a b .
                   (K.KnitEffects r
                   , BRCC.CacheEffects r
                   , V.RMap l
                   , Ord (F.Record l)
                   , FS.RecFlat l
                   , Typeable l
                   , Typeable (DP.PSDataR ks)
                   , F.ElemOf (DP.PSDataR ks) GT.StateAbbreviation
                   , DP.LPredictorsR F.⊆ DP.PSDataR ks
                   , F.ElemOf (DP.PSDataR ks) DT.PopCount
                   , DP.DCatsR F.⊆ DP.PSDataR ks
                   , l F.⊆ DP.PSDataR ks --'[GT.StateAbbreviation]
                   , Show (F.Record l)
                   )
                => Int
                -> CacheStructure () ()
                -> MC2.Config a b
                -> K.ActionWithCacheTime r (DP.PSData ks)
                -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval, Maybe MC2.ModelParameters))
runBaseModel year cacheStructure config psData_C = do
  let runConfig = MC.RunConfig False False (Just $ MC.psGroupTag @l)
      modelName = case config of
        MC2.ActionOnly cat ac -> case cat of
          MC.Reg -> MC.actionSurveyText ac.acSurvey <> "R_" <> show year
          MC.Vote -> MC.actionSurveyText ac.acSurvey <> "T_" <> show year
        MC2.PrefOnly cat _ -> case cat of
          MC.Reg -> "RP_" <> show year
          MC.Vote -> "P_" <> show year
        MC2.ActionAndPref cat ac _ -> case cat of
          MC.Reg -> MC.actionSurveyText ac.acSurvey <> "RF_" <> show year
          MC.Vote -> MC.actionSurveyText ac.acSurvey <> "F_" <> show year
  modelData_C <- cachedPreppedModelData cacheStructure
  MC2.runModel (csModelDirE cacheStructure) modelName
    (csPSName cacheStructure) runConfig config modelData_C psData_C


type PSResultR = [ModelPr, DT.PopCount, PSNumer, PSDenom]

psFold :: forall ks rs .
          (ks F.⊆ rs
          , Ord (F.Record ks)
          , rs F.⊆ rs
          , FSI.RecVec (ks V.++ PSResultR)
          )
       => (F.Record rs -> Double)
       -> (F.Record rs -> Double)
       -> (F.Record rs -> Int)
       -> FL.Fold (F.Record rs) (F.FrameRec (ks V.++ PSResultR))
psFold num den pop = FMR.concatFold
                     $ FMR.mapReduceFold
                     FMR.noUnpack
                     (FMR.assignKeysAndData @ks @rs)
                     (FMR.foldAndAddKey innerFld)
  where
    innerFld :: FL.Fold (F.Record rs) (F.Record PSResultR)
    innerFld =
      let nSum = FL.premap num FL.sum
          dSum = FL.premap den FL.sum
          popSum = FL.premap pop FL.sum
          safeDiv x y = if y > 0 then x / y else 0
      in (\n d p -> safeDiv n d F.&: p F.&: n F.&: d F.&: V.RNil) <$> nSum <*> dSum <*> popSum


adjustP :: Double -> Double -> Double
adjustP dp p = f p logitDelta where
  logitDelta | dp == 0 || dp == 1 = 0
             | otherwise = 2 * Numeric.log ((0.5 + (dp / 2)) / (0.5 - (dp / 2)))
  g x = 1 / (1 + Numeric.exp (negate x))
  f x y |  x == 0 = 0
        |  x == 1 = 1
        |  otherwise = g $ Numeric.log (x / (1 - x)) + y

adjustCI :: Double -> MT.ConfidenceInterval -> MT.ConfidenceInterval
adjustCI p (MT.ConfidenceInterval lo mid hi) = MT.ConfidenceInterval (f lo logitDelta) p (f hi logitDelta) where
  logitDelta | p == 0 || mid == 0 || p == 1 || mid == 1 = 0
             | otherwise = Numeric.log (p * (1 - mid) / (mid * (1 - p)))
  g x = 1 / (1 + Numeric.exp (negate x))
  f x y |  x == 0 = 0
        |  x == 1 = 1
        |  otherwise = g $ Numeric.log (x / (1 - x)) + y

{-
adjustCIR :: (F.ElemOf rs ModelPr, F.ElemOf rs ModelCI)
          => F.Record rs -> F.Record (F.RDeleteAll [ModelPr] rs)
adjustCIR r = F.rcast $ F.rputField @ModelCI newCI r where
  newCI = adjustCI (r ^. modelPr) (r ^. modelCI)
-}
type StateAndCats = (GT.StateAbbreviation ': DP.DCatsR)
type StateCatsPlus as = StateAndCats V.++ as

type JoinR ks ms = FJ.JoinResult StateAndCats (DP.PSDataR ks) (StateCatsPlus ms)

type AHrs ks ms = F.RDeleteAll '[GT.StateAbbreviation] (JoinR ks ms)
type AH ks ms = GT.StateAbbreviation ': AHrs ks ms


type PSTypeC l ks ms = (V.RMap l
                       , Ord (F.Record l)
                       , FS.RecFlat l
                       , Typeable l
                       , l F.⊆ DP.PSDataR ks
                       , Show (F.Record l)
                       , l F.⊆ (l V.++ PSResultR)
                       , F.ElemOf (l V.++ PSResultR) ModelPr
                       , FSI.RecVec (l V.++ PSResultR)
                       , l F.⊆ AH ks ms
                     )



type AHC ks ms = (FC.ElemsOf (AHrs ks ms) ('[DT.PopCount] V.++ ms V.++ DP.DCatsR)
                 , FSI.RecVec (AHrs ks ms)
                 , FS.RecFlat (AHrs ks ms)
                 , V.RMap (AHrs ks ms)
                 , AHrs ks ms F.⊆ AH ks ms
                 , F.ElemOf (((ks V.++ (DT.PWPopPerSqMile ': DP.DCatsR)) V.++ '[DT.PopCount]) V.++ ms) GT.StateAbbreviation
                 , V.RMap ks
                 , Show (F.Record ks)
                 , Ord (F.Record ks)
                 , ks F.⊆ (ks V.++ (DP.DCatsR V.++ [ModelPr, DT.PopCount]))
                 , AHrs ks ms F.⊆ (((ks V.++ (DT.PWPopPerSqMile ': DP.DCatsR)) V.++ '[DT.PopCount]) V.++ ms)
                 , ks V.++ (DP.DCatsR V.++ PSResultR) F.⊆ (ks V.++ (DP.DCatsR V.++ PSResultR))
                 )

type PSDataTypeTC ks = ( Typeable (DP.PSDataR ks)
                       , FC.ElemsOf (DP.PSDataR ks) [GT.StateAbbreviation, DT.PopCount]
                       , DP.LPredictorsR F.⊆ DP.PSDataR ks
                       , DP.DCatsR F.⊆ DP.PSDataR ks
                       , FJ.CanLeftJoinWithMissing StateAndCats (DP.PSDataR ks) (StateCatsPlus '[ModelPr])
                       , FC.ElemsOf (JoinR ks '[ModelPr]) [DT.PWPopPerSqMile, ModelPr, DT.PopCount]
                       , StateCatsPlus [ModelPr, DT.PopCount] F.⊆ JoinR ks '[ModelPr]
                       , AHC ks '[ModelPr]
                       , DP.PredictorsR F.⊆ AH ks '[ModelPr]
                       )


modelCPs :: forall r a b .
            (K.KnitEffects r
            , BRCC.CacheEffects r
            )
         => Int
         -> CacheStructure Text ()
         -> MC2.Config a b
         -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap StateAndCats MT.ConfidenceInterval, Maybe MC2.ModelParameters))
modelCPs year cacheStructure config = K.wrapPrefix "modelCPs" $ do
  modelData <- K.ignoreCacheTimeM $ cachedPreppedModelData $ modelCacheStructure cacheStructure
  (allStates, avgPWPopPerSqMile) <- case config of
        MC2.ActionOnly _ ac -> case ac.acSurvey of
          MC.CESSurvey -> pure $ FL.fold ((,) <$> FL.premap (view GT.stateAbbreviation) FL.set <*> FL.premap (view DT.pWPopPerSqMile) FL.mean) modelData.cesData
          MC.CPSSurvey -> pure $ FL.fold ((,) <$> FL.premap (view GT.stateAbbreviation) FL.set <*> FL.premap (view DT.pWPopPerSqMile) FL.mean) modelData.cpsData
        MC2.PrefOnly _ _ ->  pure $ FL.fold ((,) <$> FL.premap (view GT.stateAbbreviation) FL.set <*> FL.premap (view DT.pWPopPerSqMile) FL.mean) modelData.cesData
        MC2.ActionAndPref _ _ _ -> K.knitError "modelCPs called with TurnoutAndPref config."
  cellPSDataCacheKey <- case config of
    MC2.ActionOnly _ ac -> pure $ "allCell_" <>  MC.actionSurveyText ac.acSurvey <> "PSData.bin"
    MC2.PrefOnly _ _ ->  pure $ "allCell_CESPSData.bin"
    MC2.ActionAndPref _ _ _ -> K.knitError "modelCPs called with TurnoutAndPref config."
  allCellProbsCK <- BRCC.cacheFromDirE (csProjectCacheDirE cacheStructure) cellPSDataCacheKey
  allCellProbsPS_C <-  BRCC.retrieveOrMakeD allCellProbsCK (pure ()) $ \_ -> pure $ allCellProbsPS allStates avgPWPopPerSqMile
  K.logLE K.Diagnostic "Running all cell model, if necessary"
  runBaseModel @StateAndCats year (allCellCS cacheStructure) config allCellProbsPS_C


-- represents a key dependent probability shift
data Scenario ks where
  SimpleScenario :: Text -> (F.Record ks -> (Double -> Double)) -> Scenario ks

applyScenario :: ks F.⊆ rs => Lens' (F.Record rs) Double -> Scenario ks -> F.Record rs -> F.Record rs
applyScenario pLens (SimpleScenario _ f) r = over pLens (f (F.rcast r)) r

scenarioCacheText :: Scenario ks -> Text
scenarioCacheText (SimpleScenario t _) = t

type TotalReg = "TotalReg" F.:-> Int

stateActionTargets ::  (K.KnitEffects r, BRCC.CacheEffects r)
                   => Int -> MC.ModelCategory -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec [GT.StateAbbreviation, DP.ActionTarget]))
stateActionTargets year cat = do
  let stFilter y r = r ^. BRDF.year == y && r ^. GT.stateAbbreviation /= "US"
  stateTurnout_C <- BRDF.stateTurnoutLoader
  case cat of
    MC.Reg -> do
      let stateRegCacheKey = "data/stateReg2022.bin"
          innerFoldR :: FL.Fold (F.Record [RDH.VFPartyDem, RDH.VFPartyRep, RDH.VFPartyOth]) (F.Record '[TotalReg])
          innerFoldR = (\d r o -> FT.recordSingleton @TotalReg $ d + r + o)
                       <$> FL.premap (view RDH.vFPartyDem) FL.sum
                       <*> FL.premap (view RDH.vFPartyRep) FL.sum
                       <*> FL.premap (view RDH.vFPartyOth) FL.sum
          outerFoldR :: FL.Fold RDH.VF_Raw (F.FrameRec [GT.StateAbbreviation, TotalReg])
          outerFoldR = FMR.concatFold
                       $ FMR.mapReduceFold
                       FMR.noUnpack
                       (FMR.assignKeysAndData @'[GT.StateAbbreviation])
                       (FMR.foldAndAddKey innerFoldR)
      regData_C <- RDH.voterfileByTracts Nothing
      let deps = (,) <$> regData_C <*> fmap (F.filterFrame $ stFilter 2022) stateTurnout_C
      BRCC.retrieveOrMakeFrame stateRegCacheKey deps $ \(regData, stateTurnoutData) -> do
        let totalRegByState = FL.fold outerFoldR regData
            (joined, missing) = FJ.leftJoinWithMissing @'[GT.StateAbbreviation] totalRegByState stateTurnoutData
        when (not $ null missing) $ K.knitError $ "stateActionTargets: missing keys in totalRegByState/stateTurnoutData: " <> show missing
        let f r = let vep = realToFrac (F.rgetField @BRDF.VEP r)
                  in if vep > 0 then realToFrac (F.rgetField @TotalReg r) / vep else 0
            g r = FT.recordSingleton @DP.ActionTarget $ f r
        pure $ fmap (F.rcast . FT.mutate g) joined
    MC.Vote -> pure
               $ fmap (F.rcast . FT.replaceColumn @BRDF.BallotsCountedVEP @DP.ActionTarget id)
               <$> fmap (F.filterFrame $ stFilter year) stateTurnout_C

runActionModelCPAH :: forall ks r a b .
                       (K.KnitEffects r
                       , BRCC.CacheEffects r
                       , PSDataTypeTC ks
                       )
                    => Int
                    -> CacheStructure Text Text
                    -> MC.ModelCategory
                    -> MC.ActionConfig a b
                    -> Maybe (Scenario DP.PredictorsR)
                    -> K.ActionWithCacheTime r (DP.PSData ks)
                    -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (AH ks '[ModelPr])))
runActionModelCPAH year cacheStructure mc ac scenarioM psData_C = K.wrapPrefix "runTurnoutModelCPAH" $ do
  actionCPs_C <- modelCPs year (allCellCacheStructure cacheStructure) (MC2.ActionOnly mc ac)
--  let stFilter r = r ^. BRDF.year == year && r ^. GT.stateAbbreviation /= "US"
  stateActionTargets_C <- stateActionTargets year mc --fmap (fmap (F.filterFrame stFilter)) BRDF.stateTurnoutLoader
  let actionCacheDir = if mc == MC.Reg then "Registration/" else "Turnout/"
      cacheSuffix = actionCacheDir <> MC.actionSurveyText ac.acSurvey <> show year <> "_"
        <> MC.modelConfigText ac.acModelConfig <> "/" <> csAllCellPSPrefix cacheStructure
        <> maybe "" (("_" <>) .  scenarioCacheText) scenarioM
        <> "_ACProbsAH.bin"
  cacheKey <- BRCC.cacheFromDirE (csProjectCacheDirE cacheStructure) cacheSuffix
  let ahDeps = (,,) <$> actionCPs_C <*> psData_C <*> stateActionTargets_C
  BRCC.retrieveOrMakeFrame cacheKey ahDeps $ \((tCP, tMPm), psD, stateActionTargets) -> do
    K.logLE K.Info "(Re)building AH adjusted all-cell probs."
    let probFrame =  fmap (\(ks, p) -> ks F.<+> FT.recordSingleton @ModelPr p) $ M.toList $ fmap MT.ciMid $ MC.unPSMap tCP
    tMP <- K.knitMaybe "runActionModelCPAH: Nothing in turnout ModelParameters after allCellProbs run!" $ tMPm
    let (joined, missing) = FJ.leftJoinWithMissing
                            @StateAndCats
                            @(DP.PSDataR ks)
                            @(StateCatsPlus '[ModelPr])
                            (DP.unPSData psD) (F.toFrame probFrame)
        dmr = ac.acModelConfig.mcDesignMatrixRow
    when (not $ null missing) $ K.knitError $ "runActionModelCPAH: missing keys in psData/prob-frame join: " <> show missing
    let densAdjProbFrame = fmap (MC2.adjustPredictionsForDensity (view modelPr) (over modelPr . const) tMP dmr) joined
    ahProbs <- FL.foldM (TA.adjTurnoutFoldG @ModelPr @'[GT.StateAbbreviation] @_  @(AHrs ks '[ModelPr])
                         (realToFrac . view DT.popCount) (view DP.actionTarget) stateActionTargets) (fmap F.rcast densAdjProbFrame)
    case scenarioM of
      Nothing -> pure ahProbs
      Just s -> pure $ fmap (applyScenario modelPr s) ahProbs


runActionModelAH :: forall l ks r a b .
                    (K.KnitEffects r
                    , BRCC.CacheEffects r
                    , PSTypeC l ks '[ModelPr]
                    , PSDataTypeTC ks
                    )
                 => Int
                 -> CacheStructure Text Text
                 -> MC.ModelCategory
                 -> MC.ActionConfig a b
                 -> Maybe (Scenario DP.PredictorsR)
                 -> K.ActionWithCacheTime r (DP.PSData ks)
                 -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval))
runActionModelAH year cacheStructure mc ac scenarioM psData_C = K.wrapPrefix "runTurnoutModelAH" $ do
  actionCPAH_C <- runActionModelCPAH year cacheStructure mc ac scenarioM psData_C
  let psNum r = (realToFrac $ r ^. DT.popCount) * r ^. modelPr
      psDen r = realToFrac $ r ^. DT.popCount
      actionAHPS_C = fmap (FL.fold (psFold @l psNum psDen (view DT.popCount))) actionCPAH_C
  K.logLE K.Diagnostic "Running action model for CIs, if necessary"
  actionPSForCI_C <- runBaseModel @l year (modelCacheStructure cacheStructure) (MC2.ActionOnly mc ac) psData_C
  let resMapDeps = (,) <$> actionAHPS_C <*> actionPSForCI_C
      actionCacheDir = if mc == MC.Reg then "Registration/" else "Turnout/"
      cacheSuffix = actionCacheDir <> MC.actionSurveyText ac.acSurvey <> show year <> "_" <> MC.modelConfigText ac.acModelConfig
                    <> csPSName cacheStructure
                    <> maybe "" (("_" <>) .  scenarioCacheText) scenarioM
                    <> "_resMap.bin"
  ahResultCachePrefix <- BRCC.cacheFromDirE (csProjectCacheDirE cacheStructure) cacheSuffix
  BRCC.retrieveOrMakeD ahResultCachePrefix resMapDeps $ \(ahps, (cisM, _)) -> do
    K.logLE K.Info "merging AH probs and CIs"
    let psProbMap = M.fromList $ fmap (\r -> (F.rcast @l r, r ^. modelPr)) $ FL.fold FL.list ahps
        whenMatched _ p ci = Right $ adjustCI p ci
        whenMissingPS l _ = Left $ "runActionModelAH: key present in model CIs is missing from AHPS: " <> show l
        whenMissingCI l _ = Left $ "runActionModelAH: key present in AHPS is missing from CIs: " <> show l
    MC.PSMap
      <$> (K.knitEither
            $ MM.mergeA (MM.traverseMissing whenMissingPS) (MM.traverseMissing whenMissingCI) (MM.zipWithAMatched whenMatched) psProbMap (MC.unPSMap cisM))

type JoinPR ks = FJ.JoinResult3 StateAndCats (DP.PSDataR ks) (StateCatsPlus '[ModelPr]) (StateCatsPlus '[ModelT])

type PSDataTypePC ks = ( FJ.CanLeftJoinWithMissing3 StateAndCats (DP.PSDataR ks) (StateCatsPlus '[ModelPr]) (StateCatsPlus '[ModelT])
                       , FC.ElemsOf (JoinPR ks) [DT.PWPopPerSqMile, ModelPr, ModelT]
                       , StateCatsPlus [ModelPr, ModelT, DT.PopCount] F.⊆ JoinPR ks
                       , FC.ElemsOf (JoinPR ks) [GT.StateAbbreviation, ModelT]
                       , AHC ks '[ModelPr, ModelT]
                       , FC.ElemsOf (FT.Rename "ModelPr" "ModelT" (AH ks '[ModelPr])) (StateCatsPlus '[ModelT])
                       , (GT.StateAbbreviation ': AHrs ks [ModelPr, ModelT]) F.⊆ JoinPR ks
                       , DP.PredictorsR F.⊆ AH ks [ModelPr, ModelT]
                       )

-- For registration we use partisan id as the preference since some states don't have voters register for a particular party
-- And we use 2-party share, D / (D + R) rather than D / (D + R + O)
data PrefDTargetCategory r = RegDTargets | VoteDTargets (DP.DShareTargetConfig r)

catFromPrefTargets :: PrefDTargetCategory r -> MC.ModelCategory
catFromPrefTargets RegDTargets = MC.Reg
catFromPrefTargets (VoteDTargets _) = MC.Vote

statePrefDTargets :: (K.KnitEffects r, BRCC.CacheEffects r)
                  => PrefDTargetCategory r -> CacheStructure Text Text -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec [GT.StateAbbreviation, DP.PrefDTarget]))
statePrefDTargets cat cacheStructure = case cat of
  RegDTargets -> do
    let statePIDCacheKey = "data/statePID2022.bin"
        regF = FL.prefilter ((== CES.VR_Active) . F.rgetField @CES.VRegistrationC) FL.length
        innerFold :: FL.Fold (F.Record [CES.VRegistrationC, CES.PartisanId3]) (F.Record '[DP.PrefDTarget])
        innerFold = (\a d r -> FT.recordSingleton @DP.PrefDTarget $ if d + r > 0 then realToFrac d / realToFrac (d + r) else 0.5)
                    <$> regF
                    <*> FL.prefilter ((== CES.PI3_Democrat) . F.rgetField @CES.PartisanId3) regF
                    <*> FL.prefilter ((== CES.PI3_Republican) . F.rgetField @CES.PartisanId3) regF
        outerFold :: FL.Fold (F.Record CES.CESR) (F.FrameRec [GT.StateAbbreviation, DP.PrefDTarget])
        outerFold = FMR.concatFold
                    $ FMR.mapReduceFold
                    FMR.noUnpack
                    (FMR.assignKeysAndData @'[GT.StateAbbreviation])
                    (FMR.foldAndAddKey innerFold)
    cesData_C <- CES.ces22Loader
    BRCC.retrieveOrMakeFrame statePIDCacheKey cesData_C $ pure . FL.fold outerFold
  VoteDTargets dShareTargetConfig -> do
    dVoteTargets_C <- DP.dShareTarget (csProjectCacheDirE cacheStructure) dShareTargetConfig
    pure $ fmap (F.rcast . FT.replaceColumn @ET.DemShare @DP.PrefDTarget id) <$> dVoteTargets_C

runPrefModelCPAH :: forall ks r a b .
                  (K.KnitEffects r
                  , BRCC.CacheEffects r
                  , PSDataTypeTC ks
                  , PSDataTypePC ks
                  )
                 => Int
                 -> CacheStructure Text Text
                 -> MC.ActionConfig a b  -- we need a turnout model for the AH adjustment
                 -> Maybe (Scenario DP.PredictorsR)
                 -> MC.PrefConfig b
                 -> Maybe (Scenario DP.PredictorsR)
                 -> PrefDTargetCategory r -- DP.DShareTargetConfig r
                 -> K.ActionWithCacheTime r (DP.PSData ks)
                 -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (AH ks '[ModelPr, ModelT])))
runPrefModelCPAH year cacheStructure ac aScenarioM pc pScenarioM prefDTargetCategory psData_C = K.wrapPrefix "runPrefModelCPAH" $ do
  let mc = catFromPrefTargets prefDTargetCategory
  actionCPAH_C <- runActionModelCPAH year cacheStructure mc ac aScenarioM psData_C
  prefCPs_C <- modelCPs year (allCellCacheStructure cacheStructure) (MC2.PrefOnly mc pc)
  prefDTargets_C <- statePrefDTargets prefDTargetCategory cacheStructure --DP.dShareTarget (csProjectCacheDirE cacheStructure) dShareTargetConfig
  let ahDeps = (,,,) <$> actionCPAH_C <*> prefCPs_C <*> psData_C <*> prefDTargets_C
      (prefCacheDir, prefTargetText) = case prefDTargetCategory of
        RegDTargets -> ("Reg/CES", "PID")
        VoteDTargets dShareTargetConfig -> ("Pref/CES", DP.dShareTargetText dShareTargetConfig)
      cacheSuffix = prefCacheDir <> show year <> "_" <> MC.modelConfigText pc.pcModelConfig <> "/"
                    <> csAllCellPSPrefix cacheStructure
                    <> maybe "" (("_" <>) .  scenarioCacheText) aScenarioM
                    <> maybe "" (("_" <>) .  scenarioCacheText) pScenarioM
                    <>  "_" <> prefTargetText
                    <> "_ACProbsAH.bin"
  cpahCacheKey <- BRCC.cacheFromDirE (csProjectCacheDirE cacheStructure) cacheSuffix
  BRCC.retrieveOrMakeFrame cpahCacheKey ahDeps $ \(tCPF, (pCP, pMPm), psD, prefTarget) -> do
    K.logLE K.Info "(Re)building AH adjusted all-cell probs."
    let probFrame =  fmap (\(ks, p) -> ks F.<+> FT.recordSingleton @ModelPr p) $ M.toList $ fmap MT.ciMid $ MC.unPSMap pCP
        turnoutFrame = fmap (F.rcast @(StateCatsPlus '[ModelT]) . FT.rename @"ModelPr" @"ModelT") tCPF
    tMP <- K.knitMaybe "runTurnoutPrefAH: Nothing in pref ModelParameters after allCellProbs run!" $ pMPm
    let (joined, missingPSPref, missingT) = FJ.leftJoin3WithMissing
                            @StateAndCats
                            (DP.unPSData psD) (F.toFrame probFrame) turnoutFrame
    when (not $ null missingPSPref) $ K.knitError $ "runPrefModelAH: missing keys in psData/prob-frame join: " <> show missingPSPref
    when (not $ null missingT) $ K.knitError $ "runPrefModelAH: missing keys in psData+prob-frame/turnout frame join: " <> show missingT
    let dmr = pc.pcModelConfig.mcDesignMatrixRow
        adjForDensity = MC2.adjustPredictionsForDensity (view modelPr) (over modelPr . const) tMP dmr
        densAdjProbFrame = fmap adjForDensity joined
        modeledVoters r = r ^. modelT * realToFrac (r ^. DT.popCount)
    ahProbs <- FL.foldM
               (TA.adjTurnoutFoldG @ModelPr @'[GT.StateAbbreviation] @_ @(AHrs ks [ModelPr, ModelT]) modeledVoters (view DP.prefDTarget) prefTarget)
               (fmap F.rcast densAdjProbFrame)
    case pScenarioM of
      Nothing -> pure ahProbs
      Just s -> pure $ fmap (applyScenario modelPr s) ahProbs

runPrefModelAH :: forall l ks r a b .
                  (K.KnitEffects r
                  , BRCC.CacheEffects r
                  , PSTypeC l ks '[ModelPr, ModelT]
                  , PSDataTypeTC ks
                  , PSDataTypePC ks
                  )
               => Int
               -> CacheStructure Text Text
               -> MC.ActionConfig a b  -- we need a turnout model for the AH adjustment
               -> Maybe (Scenario DP.PredictorsR)
               -> MC.PrefConfig b
               -> Maybe (Scenario DP.PredictorsR)
               -> PrefDTargetCategory r --DP.DShareTargetConfig r
               -> K.ActionWithCacheTime r (DP.PSData ks)
               -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval))
runPrefModelAH year cacheStructure ac aScenarioM pc pScenarioM prefDTargetCategory psData_C = K.wrapPrefix "ruPrefModelAH" $ do
  let mc = catFromPrefTargets prefDTargetCategory
  prefCPAH_C <- runPrefModelCPAH year cacheStructure ac aScenarioM pc pScenarioM prefDTargetCategory psData_C
  let psNum r = (realToFrac $ r ^. DT.popCount) * r ^. modelPr
      psDen r = realToFrac $ r ^. DT.popCount
      prefAHPS_C = fmap (FL.fold (psFold @l psNum psDen (view DT.popCount))) prefCPAH_C
  K.logLE K.Diagnostic "Running pref model for CIs, if necessary"
  prefPSForCI_C <- runBaseModel @l year (modelCacheStructure cacheStructure) (MC2.PrefOnly mc pc) psData_C
  let resMapDeps = (,) <$> prefAHPS_C <*> prefPSForCI_C
      (prefCacheDir, prefTargetText) = case prefDTargetCategory of
        RegDTargets -> ("Reg/CES", "PID")
        VoteDTargets dShareTargetConfig -> ("Pref/CES", DP.dShareTargetText dShareTargetConfig)
      cacheSuffix = prefCacheDir <> show year <> "_" <> MC.modelConfigText pc.pcModelConfig
                    <> csPSName cacheStructure
                    <> maybe "" (("_" <>) .  scenarioCacheText) aScenarioM
                    <> maybe "" (("_" <>) .  scenarioCacheText) pScenarioM
                    <> "_" <> prefTargetText <> "_resMap.bin"
  ahCacheKey <- BRCC.cacheFromDirE (csProjectCacheDirE cacheStructure) cacheSuffix
  BRCC.retrieveOrMakeD ahCacheKey resMapDeps $ \(ahps, (cisM, _)) -> do
    K.logLE K.Info "merging AH probs and CIs"
    let psProbMap = M.fromList $ fmap (\r -> (F.rcast @l r, r ^. modelPr)) $ FL.fold FL.list ahps
        whenMatched _ p ci = Right $ adjustCI p ci
        whenMissingPS l _ = Left $ "runPrefModelAH: key present in model CIs is missing from AHPS: " <> show l
        whenMissingCI l _ = Left $ "runPrefModelAH: key present in AHPS is missing from CIs: " <> show l
    MC.PSMap
      <$> (K.knitEither
            $ MM.mergeA (MM.traverseMissing whenMissingPS) (MM.traverseMissing whenMissingCI) (MM.zipWithAMatched whenMatched) psProbMap (MC.unPSMap cisM))

{-
runFullModel :: forall l r ks a b .
                (K.KnitEffects r
                , BRCC.CacheEffects r
                , V.RMap l
                , Ord (F.Record l)
                , FS.RecFlat l
                , Typeable l
                , Typeable (DP.PSDataR ks)
                , F.ElemOf (DP.PSDataR ks) GT.StateAbbreviation
                , DP.LPredictorsR F.⊆ DP.PSDataR ks
                , F.ElemOf (DP.PSDataR ks) DT.PopCount
                , DP.DCatsR F.⊆ DP.PSDataR ks
                , l F.⊆ DP.PSDataR ks
                , Show (F.Record l)
                )
             => Int
             -> CacheStructure () ()
             -> MC.TurnoutConfig a b
             -> MC.PrefConfig b
             -> K.ActionWithCacheTime r (DP.PSData ks)
             -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval, Maybe MC2.ModelParameters))
runFullModel year cacheStructure tc pc psData_C = K.wrapPrefix "runFullModel" $ do
  let config = MC2.TurnoutAndPref tc pc
      runConfig = MC.RunConfig False False (Just $ MC.psGroupTag @l)
  modelData_C <- cachedPreppedModelData cacheStructure
  MC2.runModel (csModelDirE cacheStructure) (MC.turnoutSurveyText tc.tcSurvey <> "F_" <> show year) (csPSName cacheStructure)
    runConfig config modelData_C psData_C
-}
type ToJoinPR ks = FT.Rename "ModelPr" "ModelP" (AH ks '[ModelPr, ModelT]) --(ks V.++ (DP.DCatsR V.++ [ModelPr, DT.PopCount]))
type JoinFR ks = FJ.JoinResult (ks V.++ DP.DCatsR) (DP.PSDataR ks) (ToJoinPR ks)

runFullModelAH :: forall l ks r a b .
                  (K.KnitEffects r
                  , BRCC.CacheEffects r
                  , PSTypeC l ks '[ModelPr, ModelT]
                  , PSDataTypeTC ks
                  , PSDataTypePC ks
                  , FJ.CanLeftJoinWithMissing  (ks V.++ DP.DCatsR) (DP.PSDataR ks) (ToJoinPR ks)
                  , l F.⊆ JoinFR ks
                  , JoinFR ks F.⊆ JoinFR ks
                  , FC.ElemsOf (JoinFR ks) [DT.PopCount, ModelT, ModelP]
                  , V.RMap (l V.++ PSResultR)
                  , FS.RecFlat (l V.++ PSResultR)
                  , Show (F.Record (ks V.++ DP.DCatsR))
                  )
               => Int
               -> CacheStructure Text Text
               -> MC.ActionConfig a b
               -> Maybe (Scenario DP.PredictorsR)
               -> MC.PrefConfig b
               -> Maybe (Scenario DP.PredictorsR)
               -> PrefDTargetCategory r
               -> K.ActionWithCacheTime r (DP.PSData ks)
               -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval))
runFullModelAH year cacheStructure ac aScenarioM pc pScenarioM prefDTargetCategory psData_C = K.wrapPrefix "runFullModelAH" $ do
  let mc = catFromPrefTargets prefDTargetCategory
--  let cachePrefixT = "model/election2/Turnout/" <> MC.turnoutSurveyText ts <> show year <> "_" <> MC.aggregationText sa <> "_" <> MC.alphasText am <> "/"
--  turnoutCPAH_C <- runTurnoutModelCPAH year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am "AllCells" psData_C
  prefCPAH_C <- runPrefModelCPAH year cacheStructure ac aScenarioM pc pScenarioM prefDTargetCategory psData_C
  let (fullCacheDir, prefTargetText) = case prefDTargetCategory of
        RegDTargets -> ("RFull/", "PID")
        VoteDTargets dShareTargetConfig -> ("Full/", DP.dShareTargetText dShareTargetConfig)
  let cacheMid =  fullCacheDir <> MC.actionSurveyText ac.acSurvey <> show year <> "_" <> MC.modelConfigText pc.pcModelConfig
      ahpsCacheSuffix = cacheMid
                        <> csAllCellPSPrefix cacheStructure
                        <> maybe "" (("_" <>) .  scenarioCacheText) aScenarioM
                        <> maybe "" (("_" <>) .  scenarioCacheText) pScenarioM
                        <>  "_" <> prefTargetText <> "_PS.bin"
  ahpsCacheKey <- BRCC.cacheFromDirE (csProjectCacheDirE cacheStructure) ahpsCacheSuffix
  let joinDeps = (,) <$> prefCPAH_C <*> psData_C
  fullAHPS_C <- BRCC.retrieveOrMakeFrame ahpsCacheKey joinDeps $ \(pref, psData) -> do
    K.logLE K.Info "Doing 3-way join..."
    let-- turnoutFrame = fmap (FT.rename @"ModelPr" @"ModelT") turnout
        prefFrame = fmap (FT.rename @"ModelPr" @"ModelP") pref
        (joined, missing) = FJ.leftJoinWithMissing
                            @(ks V.++ DP.DCatsR)
                            (DP.unPSData psData) prefFrame
    when (not $ null missing) $ K.knitError $ "runFullModelAH: Missing keys in psData/prefCPAH join: " <> show missing
    K.logLE K.Info "Doing post-stratification..."
    let ppl r = realToFrac $ r ^. DT.popCount
        t r = r ^. modelT
        p r = r ^. modelP
        psNum r = ppl r * t r * p r
        psDen r = ppl r * t r
    pure $ FL.fold (psFold @l psNum psDen (view DT.popCount)) joined
  K.logLE K.Diagnostic "Running full model for CIs, if necessary"
  fullPSForCI_C <- runBaseModel @l year (modelCacheStructure cacheStructure) (MC2.ActionAndPref mc ac pc) psData_C

  let cacheSuffix = cacheMid <> csPSName cacheStructure <>  "_"
                    <> maybe "" (("_" <>) .  scenarioCacheText) aScenarioM
                    <> maybe "" (("_" <>) .  scenarioCacheText) pScenarioM
                    <> prefTargetText <> "_resMap.bin"
      resMapDeps = (,) <$> fullAHPS_C <*> fullPSForCI_C
  cacheKey <- BRCC.cacheFromDirE (csProjectCacheDirE cacheStructure) cacheSuffix
  BRCC.retrieveOrMakeD cacheKey resMapDeps $ \(ahps, (cisM, _)) -> do
    K.logLE K.Info "merging AH probs and CIs"
    let psProbMap = M.fromList $ fmap (\r -> (F.rcast @l r, r ^. modelPr)) $ FL.fold FL.list ahps
        whenMatched _ p ci = Right $ adjustCI p ci
        whenMissingPS l _ = Left $ "runFullModelAH: key present in model CIs is missing from AHPS: " <> show l
        whenMissingCI l _ = Left $ "runFullModelAH: key present in AHPS is missing from CIs: " <> show l
    MC.PSMap
      <$> (K.knitEither
            $ MM.mergeA (MM.traverseMissing whenMissingPS) (MM.traverseMissing whenMissingCI) (MM.zipWithAMatched whenMatched) psProbMap (MC.unPSMap cisM))

allCellProbsPS :: Set Text -> Double -> DP.PSData (GT.StateAbbreviation ': DP.DCatsR)
allCellProbsPS states avgPWDensity =
  let catRec ::  DT.Age5 -> DT.Sex -> DT.Education4 -> DT.Race5 -> F.Record DP.DCatsR
      catRec a s e r = a F.&: s F.&: e F.&: r F.&: V.RNil
      densRec :: F.Record '[DT.PWPopPerSqMile]
      densRec = FT.recordSingleton avgPWDensity
      popRec :: F.Record '[DT.PopCount]
      popRec = FT.recordSingleton 1
      allCellRec :: Text -> DT.Age5 -> DT.Sex -> DT.Education4 -> DT.Race5
                 -> F.Record (DP.PSDataR (GT.StateAbbreviation ': DP.DCatsR))
      allCellRec st a s e r = (st F.&: catRec a s e r) F.<+> densRec F.<+> catRec a s e r F.<+> popRec
      allCellRecList = [allCellRec st a s e r | st <- Set.toList states
                                              , a <- Set.toList Keyed.elements
                                              , s <- Set.toList Keyed.elements
                                              , e <- Set.toList Keyed.elements
                                              , r <- Set.toList Keyed.elements
                                              ]
  in DP.PSData $ F.toFrame allCellRecList

modelCompBy :: forall ks r b . (K.KnitEffects r, BRCC.CacheEffects r, PSByC ks)
            => (Text -> MC.SurveyAggregation b -> MC.Alphas -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
            -> Text -> MC.SurveyAggregation b -> MC.Alphas -> K.Sem r (Text, F.FrameRec (ks V.++ '[DT.PopCount, ModelCI]))
modelCompBy runModel catLabel agg am = do
          comp <- psBy @ks (runModel catLabel agg am)
          pure (MC.aggregationText agg <> "_" <> MC.alphasText am, comp)

allModelsCompBy :: forall ks r b . (K.KnitEffects r, BRCC.CacheEffects r, PSByC ks)
                => (Text -> MC.SurveyAggregation b -> MC.Alphas -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
                -> Text -> [MC.SurveyAggregation b] -> [MC.Alphas] -> K.Sem r [(Text, F.FrameRec (ks V.++ '[DT.PopCount, ModelCI]))]
allModelsCompBy runModel catLabel aggs' alphaModels' =
  traverse (\(agg, am) -> modelCompBy @ks runModel catLabel agg am) [(agg, am) | agg <- aggs',  am <- alphaModels']

allModelsCompChart :: forall ks r b pbase . (K.KnitOne r, BRCC.CacheEffects r, PSByC ks, Keyed.FiniteSet (F.Record ks)
                                            , F.ElemOf (ks V.++ [DT.PopCount, ModelCI]) DT.PopCount
                                            , F.ElemOf (ks V.++ [DT.PopCount, ModelCI]) ModelCI
                                            , ks F.⊆ (ks V.++ [DT.PopCount, ModelCI])
                                        )
                   => BRHJ.JsonLocations pbase
                   -> (Text -> MC.SurveyAggregation b -> MC.Alphas -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
                   -> Text
                   -> Text
                   -> (F.Record ks -> Text)
                   -> [MC.SurveyAggregation b]
                   -> [MC.Alphas]
                   -> K.Sem r ()
allModelsCompChart jsonLocations runModel catLabel modelType catText aggs' alphaModels' = do
  allModels <- allModelsCompBy @ks runModel catLabel aggs' alphaModels'
  let cats = Set.toList $ Keyed.elements @(F.Record ks)
      _numCats = length cats
      numSources = length allModels
  catCompChart <- categoryChart @ks jsonLocations (modelType <> " Comparison By Category") (modelType <> "Comp")
                  (FV.fixedSizeVC 300 (30 * realToFrac numSources) 10) (Just cats) (Just $ fmap fst allModels)
                  catText allModels
  _ <- K.addHvega Nothing Nothing catCompChart
  pure ()

type PSByC ks = (Show (F.Record ks)
                 , Typeable ks
                 , V.RMap ks
                 , Ord (F.Record ks)
                 , ks F.⊆ DP.PSDataR '[GT.StateAbbreviation]
                 , ks F.⊆ DDP.ACSa5ByStateR
                 , ks F.⊆ (ks V.++ '[DT.PopCount])
                 , F.ElemOf (ks V.++ '[DT.PopCount]) DT.PopCount
                 , FSI.RecVec (ks V.++ '[DT.PopCount])
                 , FSI.RecVec (ks V.++ '[DT.PopCount, ModelCI])
                 , FS.RecFlat ks)

psBy :: forall ks r .
         (PSByC ks
         , K.KnitEffects r
         , BRCC.CacheEffects r
         )
      => K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval))
      -> K.Sem r (F.FrameRec (ks V.++ [DT.PopCount, ModelCI]))
psBy runModel = do
    (MC.PSMap psMap) <- K.ignoreCacheTimeM runModel
    let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22
    pcMap <- DDP.cachedACSa5ByState srcWindow cachedSrc 2022 >>= popCountByMap @ks
    let whenMatched :: F.Record ks -> MT.ConfidenceInterval -> Int -> Either Text (F.Record  (ks V.++ [DT.PopCount, ModelCI]))
        whenMatched k t p = pure $ k F.<+> (p F.&: t F.&: V.RNil :: F.Record [DT.PopCount, ModelCI])
        whenMissingPC k _ = Left $ "psBy: " <> show k <> " is missing from PopCount map."
        whenMissingT k _ = Left $ "psBy: " <> show k <> " is missing from ps map."
    mergedMap <- K.knitEither
                 $ MM.mergeA (MM.traverseMissing whenMissingPC) (MM.traverseMissing whenMissingT) (MM.zipWithAMatched whenMatched) psMap pcMap
    pure $ F.toFrame $ M.elems mergedMap


psByState ::  (K.KnitEffects r, BRCC.CacheEffects r)
          => (K.Sem r (K.ActionWithCacheTime r (MC.PSMap '[GT.StateAbbreviation] MT.ConfidenceInterval)))
          -> (F.FrameRec [GT.StateAbbreviation, DT.PopCount, ModelCI] -> K.Sem r (F.FrameRec ([GT.StateAbbreviation, DT.PopCount, ModelCI] V.++ bs)))
          -> K.Sem r (F.FrameRec ([GT.StateAbbreviation, DT.PopCount, ModelCI] V.++ bs))
psByState runModel addStateFields = psBy @'[GT.StateAbbreviation] runModel >>= addStateFields

popCountBy :: forall ks rs r .
              (--K.KnitEffects r
--              , BRCC.CacheEffects r
                ks F.⊆ rs, F.ElemOf rs DT.PopCount, Ord (F.Record ks)
              , FSI.RecVec (ks V.++ '[DT.PopCount])
              )
           => K.ActionWithCacheTime r (F.FrameRec rs)
           -> K.Sem r (F.FrameRec (ks V.++ '[DT.PopCount]))
popCountBy counts_C = do
  counts <- K.ignoreCacheTime counts_C
  let aggFld = FMR.concatFold
               $ FMR.mapReduceFold
               FMR.noUnpack
               (FMR.assignKeysAndData @ks @'[DT.PopCount])
               (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
  pure $ FL.fold aggFld counts

popCountByMap :: forall ks rs r .
                 (K.KnitEffects r
--                 , BRCC.CacheEffects r
                 , ks F.⊆ rs
                 , F.ElemOf rs DT.PopCount
                 , Ord (F.Record ks)
                 , ks F.⊆ (ks V.++ '[DT.PopCount])
                 , F.ElemOf (ks V.++ '[DT.PopCount])  DT.PopCount
                 , FSI.RecVec (ks V.++ '[DT.PopCount])
                 )
              => K.ActionWithCacheTime r (F.FrameRec rs)
              -> K.Sem r (Map (F.Record ks) Int)
popCountByMap = fmap (FL.fold (FL.premap keyValue FL.map)) . popCountBy @ks where
  keyValue r = (F.rcast @ks r, r ^. DT.popCount)




surveyDataBy :: forall ks rs a .
                (Ord (F.Record ks)
                , ks F.⊆ rs
                , DP.CountDataR F.⊆ rs
                , FSI.RecVec (ks V.++ '[ModelPr])
                )
             => Maybe (MC.SurveyAggregation a) -> F.FrameRec rs -> F.FrameRec (ks V.++ '[ModelPr])
surveyDataBy saM = FL.fold fld
  where
    safeDiv :: Double -> Double -> Double
    safeDiv x y = if (y /= 0) then x / y else 0
    uwInnerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    uwInnerFld =
      let sF = fmap realToFrac $ FL.premap (view DP.surveyed) FL.sum
          vF = fmap realToFrac $ FL.premap (view DP.voted) FL.sum
      in (\s v -> safeDiv v s F.&: V.RNil) <$> sF <*> vF
    mwInnerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    mwInnerFld =
      let sF = FL.premap (view DP.surveyedW) FL.sum
          vF = FL.premap (view DP.votedW) FL.sum
      in (\s v -> safeDiv v s F.&: V.RNil) <$> sF <*> vF
    wInnerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    wInnerFld =
      let swF = FL.premap (view DP.surveyWeight) FL.sum
          swvF = FL.premap (\r -> view DP.surveyWeight r * realToFrac (view DP.voted r) / realToFrac (view DP.surveyed r)) FL.sum
      in (\s v -> safeDiv v s F.&: V.RNil) <$> swF <*> swvF

    innerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    innerFld = case saM of
      Nothing -> mwInnerFld
      Just sa -> case sa of
        MC.UnweightedAggregation -> uwInnerFld
        MC.WeightedAggregation _ -> wInnerFld
    fld :: FL.Fold (F.Record rs) (F.FrameRec (ks V.++ '[ModelPr]))
    fld = FMR.concatFold
          $ FMR.mapReduceFold
          FMR.noUnpack
          (FMR.assignKeysAndData @ks @DP.CountDataR)
          (FMR.foldAndAddKey innerFld)

addBallotsCountedVAP :: (K.KnitEffects r, BRCC.CacheEffects r
                        , FJ.CanLeftJoinM '[GT.StateAbbreviation] rs BRDF.StateTurnoutCols
                        , F.ElemOf (rs V.++ (F.RDelete GT.StateAbbreviation BRDF.StateTurnoutCols)) GT.StateAbbreviation
                        , rs V.++ '[BRDF.BallotsCountedVAP, BRDF.VAP] F.⊆ (rs V.++ (F.RDelete GT.StateAbbreviation BRDF.StateTurnoutCols))
                        )
                     => F.FrameRec rs ->  K.Sem r (F.FrameRec (rs V.++ '[BRDF.BallotsCountedVAP, BRDF.VAP]))
addBallotsCountedVAP fr = do
  turnoutByState <- F.filterFrame ((== 2020) . view BRDF.year) <$> K.ignoreCacheTimeM BRDF.stateTurnoutLoader
  let (joined, missing) = FJ.leftJoinWithMissing @'[GT.StateAbbreviation] fr turnoutByState
  when (not $ null missing) $ K.knitError $ "addBallotysCOuntedVAP: missing keys in state turnout target join: " <> show missing
  pure $ fmap F.rcast joined

addBallotsCountedVEP :: (K.KnitEffects r, BRCC.CacheEffects r
                        , FJ.CanLeftJoinM '[GT.StateAbbreviation] rs BRDF.StateTurnoutCols
                        , F.ElemOf (rs V.++ (F.RDelete GT.StateAbbreviation BRDF.StateTurnoutCols)) GT.StateAbbreviation
                        , rs V.++ '[BRDF.BallotsCountedVEP, BRDF.VAP] F.⊆ (rs V.++ (F.RDelete GT.StateAbbreviation BRDF.StateTurnoutCols))
                        )
                     => F.FrameRec rs ->  K.Sem r (F.FrameRec (rs V.++ '[BRDF.BallotsCountedVEP, BRDF.VAP]))
addBallotsCountedVEP fr = do
  turnoutByState <- F.filterFrame ((== 2020) . view BRDF.year) <$> K.ignoreCacheTimeM BRDF.stateTurnoutLoader
  let (joined, missing) = FJ.leftJoinWithMissing @'[GT.StateAbbreviation] fr turnoutByState
  when (not $ null missing) $ K.knitError $ "addBallotysCOuntedVAP: missing keys in state turnout target join: " <> show missing
  pure $ fmap F.rcast joined



modelCIToModelPr :: (F.RDelete ModelCI rs V.++ '[ModelPr] F.⊆ ('[ModelPr] V.++ rs)
                    , F.ElemOf rs ModelCI)
                 => F.Record rs -> F.Record (F.RDelete ModelCI rs V.++ '[ModelPr])
modelCIToModelPr r = F.rcast $ FT.recordSingleton @ModelPr (MT.ciMid $ view modelCI r) F.<+> r

stateChart :: (K.KnitEffects r, F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs ModelPr)
           => BRHJ.JsonLocations pbase
           -> Text
           -> Text
           -> Text
           -> FV.ViewConfig
           -> (F.Record rs -> Int)
           -> Maybe (F.Record rs -> Double)
           -> [(Text, F.FrameRec rs)]
           -> K.Sem r GV.VegaLite
stateChart jsonLocations chartID title modelType vc vap tgtM tableRowsByModel = do
  let colData (t, r)
        = [("State", GV.Str $ r ^. GT.stateAbbreviation)
          , ("VAP", GV.Number $ realToFrac $ vap r)
          , (modelType, GV.Number $ 100 * r ^. modelPr)
          , ("Source", GV.Str t)
          ] ++ case tgtM of
                 Nothing -> []
                 Just tgt ->
                   [ ("Tgt " <> modelType, GV.Number $ 100 * tgt r)
                   , ("Model - Tgt", GV.Number $ 100 * (r ^. modelPr - tgt r))
                   ]

--      toData kltr = fmap ($ kltr) $ fmap colData [0..(n-1)]
      jsonRows = FL.fold (VJ.rowsToJSON colData [] Nothing) $ concat $ fmap (\(s, fr) -> fmap (s,) $ FL.fold FL.list fr) tableRowsByModel
  jsonFilePrefix <- K.getNextUnusedId $ ("statePSWithTargets_" <> chartID)
  jsonUrl <- BRHJ.addJSON jsonLocations jsonFilePrefix jsonRows

  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
  --
      encY = case tgtM of
        Nothing -> GV.position GV.Y [GV.PName modelType, GV.PmType GV.Quantitative]
        Just _ -> GV.position GV.Y [GV.PName "Model - Tgt", GV.PmType GV.Quantitative]
      encScatter = GV.encoding
        . GV.position GV.X [GV.PName "State", GV.PmType GV.Nominal]
        . encY
        . GV.color [GV.MName "Source", GV.MmType GV.Nominal]
        . GV.size [GV.MName "VAP", GV.MmType GV.Quantitative]
      markScatter = GV.mark GV.Circle [GV.MTooltip GV.TTEncoding]
      scatterSpec = GV.asSpec [encScatter [], markScatter]
      layers = GV.layer [scatterSpec]
  pure $ FV.configuredVegaLite vc [FV.title title
                                  , layers
                                  , vlData
                                  ]



categoryChart :: forall ks rs pbase r . (K.KnitEffects r, F.ElemOf rs DT.PopCount, F.ElemOf rs ModelCI, ks F.⊆ rs)
              => BRHJ.JsonLocations pbase
              -> Text
              -> Text
              -> FV.ViewConfig
              -> Maybe [F.Record ks]
              -> Maybe [Text]
              -> (F.Record ks -> Text)
              -> [(Text, F.FrameRec rs)] --[k, TurnoutCI, DT.PopCount])]
              -> K.Sem r GV.VegaLite
categoryChart jsonLocations title chartID vc catSortM sourceSortM catText tableRowsByModel = do
  let colData (t, r)
        = [("Category", GV.Str $ catText $ F.rcast r)
          , ("Ppl", GV.Number $ realToFrac  $ r ^. DT.popCount)
          , ("Lo", GV.Number $ MT.ciLower $ r ^. modelCI)
          , ("Mid", GV.Number $ MT.ciMid $ r ^. modelCI)
          , ("Hi", GV.Number $ MT.ciUpper $ r ^. modelCI)
          , ("Source", GV.Str t)
          ]

--      toData kltr = fmap ($ kltr) $ fmap colData [0..(n-1)]
      jsonRows = FL.fold (VJ.rowsToJSON colData [] Nothing) $ concat $ fmap (\(s, fr) -> fmap (s,) $ FL.fold FL.list fr) tableRowsByModel
  jsonFilePrefix <- K.getNextUnusedId $ ("statePSWithTargets_" <> chartID)
  jsonUrl <- BRHJ.addJSON jsonLocations jsonFilePrefix jsonRows

  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
  --
      xScale = GV.PScale [GV.SZero False]
      xSort = case sourceSortM of
        Nothing -> []
        Just so -> [GV.PSort [GV.CustomSort $ GV.Strings so]]
      facetSort = case catSortM of
        Nothing -> []
        Just so -> [GV.FSort [GV.CustomSort $ GV.Strings $ fmap catText so]]
      encMid = GV.encoding
        . GV.position GV.Y [GV.PName "Source", GV.PmType GV.Nominal]
        . GV.position GV.X ([GV.PName "Mid", GV.PmType GV.Quantitative, xScale] <> xSort)
--        . GV.color [GV.MName "Source", GV.MmType GV.Nominal]
--        . GV.size [GV.MName "Ppl", GV.MmType GV.Quantitative]
      markMid = GV.mark GV.Circle [GV.MTooltip GV.TTEncoding]
      midSpec = GV.asSpec [encMid [], markMid]
      encError = GV.encoding
        . GV.position GV.Y [GV.PName "Source", GV.PmType GV.Nominal]
        . GV.position GV.X ([GV.PName "Lo", GV.PmType GV.Quantitative, xScale] <> xSort)
        . GV.position GV.X2 ([GV.PName "Hi", GV.PmType GV.Quantitative, xScale] <> xSort)
  --      . GV.color [GV.MName "Source", GV.MmType GV.Nominal]
      markError = GV.mark GV.ErrorBar [GV.MTooltip GV.TTEncoding]
      errorSpec = GV.asSpec [encError [], markError]
      layers = GV.layer [midSpec, errorSpec]
  pure $ FV.configuredVegaLite vc [FV.title title
                                  , GV.facet [GV.RowBy ([GV.FName "Category", GV.FmType GV.Nominal] <> facetSort)]
                                  , GV.specification (GV.asSpec [layers])
                                  , vlData
                                  ]
