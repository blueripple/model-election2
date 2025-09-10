{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
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
import qualified BlueRipple.Data.CachingCore as BRCC
--import qualified BlueRipple.Data.LoadersCore as BRLC
import qualified BlueRipple.Data.Types.Geographic as GT
import qualified BlueRipple.Data.Types.Demographic as DT
import qualified BlueRipple.Data.Types.Election as ET
import qualified BlueRipple.Data.Types.Modeling as MT
import qualified BlueRipple.Data.ACS_PUMS as ACS
import qualified BlueRipple.Data.CES as CES
import qualified BlueRipple.Data.Keyed as Keyed
import qualified BlueRipple.Utilities.HvegaJsonData as BRHJ
import qualified BlueRipple.Model.TurnoutAdjustment as TA

import qualified Polysemy as P
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
FTH.declareColumn "ModelA" ''Double
FTH.declareColumn "ModelP" ''Double
FTH.declareColumn "WgtdX" ''Double
FTH.declareColumn "RatioResult" ''Double
FTH.declareColumn "SumResult" ''Double

data CacheStructure a b =
  CacheStructure
  {
    csModelDirE :: Either Text Text
  , csProjectCacheDirE :: Either Text Text
  , csPSName :: Text
  , csAllCellPSName :: a
  , csAllCellPSPrefix :: b -- used for AH adjusted
  }

timed :: K.KnitEffects r => Text -> K.Sem r a -> K.Sem r a
timed = K.logTiming (K.logLE K.Info)

modelCacheStructure :: CacheStructure a b -> CacheStructure () ()
modelCacheStructure (CacheStructure x y z _ _) = CacheStructure x y z () ()

allCellCacheStructure :: CacheStructure a b -> CacheStructure a ()
allCellCacheStructure (CacheStructure x y z xx _) = CacheStructure x y z xx ()

allCellCS :: CacheStructure Text () -> CacheStructure () ()
allCellCS (CacheStructure a b _ c _) = CacheStructure a b c () ()

cachedPreppedModelData :: (K.KnitEffects r, BRCC.CacheEffects r)
                       => CacheStructure () ()
                       -> MC2.Config a b
                       -> K.Sem r (K.ActionWithCacheTime r (DP.ModelData DP.CDKeyR))
cachedPreppedModelData cacheStructure config = K.wrapPrefix "cachedPreppedModelData" $ do
  let sp = case config of
        MC2.ActionOnly _  (MC.ActionConfig as _) -> case as of
          MC.CESSurvey sp' -> sp'
          MC.CPSSurvey -> DP.AllSurveyed DP.Both -- this is unused
        MC2.PrefOnly _ (MC.PrefConfig sp' _) -> sp'
        MC2.ActionAndPref _ _ (MC.PrefConfig sp' _) -> sp'
  cacheDirE' <- K.knitMaybe "Empty cacheDir given!" $ BRCC.insureFinalSlashE $ csProjectCacheDirE cacheStructure
  let appendCacheFileCES :: Text -> Text -> Text
      appendCacheFileCES t d = d <> t <> "_" <> DP.surveyPortionText sp <> ".bin"
      appendCacheFileCPS :: Text -> Text -> Text
      appendCacheFileCPS t d = d <> t <> ".bin"
      cpsModelCacheE = bimap (appendCacheFileCPS "CPSModelData") (appendCacheFileCPS "CPSModelData") cacheDirE'
      cesByStateModelCacheE = bimap (appendCacheFileCES "CESModelData") (appendCacheFileCES "CESByStateModelData") cacheDirE'
      cesByCDModelCacheE = bimap (appendCacheFileCES "CESModelData") (appendCacheFileCES "CESByCDModelData") cacheDirE'
  rawCESByCD_C <- DP.cesCountedDemPresVotesByCD False sp
  rawCESByState_C <- DP.cesCountedDemPresVotesByState False sp
  rawCPS_C <- DP.cpsCountedTurnoutByState
  DP.cachedPreppedModelDataCD cpsModelCacheE rawCPS_C cesByStateModelCacheE rawCESByState_C cesByCDModelCacheE rawCESByCD_C

runBaseModel ::  forall l r ks a b .
                   (K.KnitEffects r
                   , Typeable ks
                   , BRCC.CacheEffects r
                   , MC2.ElectionModelC l ks DP.CDKeyR
                   , FSI.RecVec (DP.PSDataR ks)
                   , F.ElemOf (DP.PSDataR ks) DT.PopCount
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
        MC2.PrefOnly cat (MC.PrefConfig sp _) -> case cat of
          MC.Reg -> "RP_" <> DP.surveyPortionText sp <> "_" <> show year
          MC.Vote -> "P_" <> DP.surveyPortionText sp <> "_" <> show year
        MC2.ActionAndPref cat ac _ -> case cat of
          MC.Reg -> MC.actionSurveyText ac.acSurvey <> "RF_" <> show year
          MC.Vote -> MC.actionSurveyText ac.acSurvey <> "F_" <> show year
  modelData_C <- cachedPreppedModelData cacheStructure config
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
                       , MC2.ElectionModelC StateAndCats ks DP.CDKeyR
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
  modelData <- timed "Loaded model data" $ K.ignoreCacheTimeM $ cachedPreppedModelData (modelCacheStructure cacheStructure) config
  (allStates, avgPWPopPerSqMile) <- case config of
        MC2.ActionOnly _ ac -> case ac.acSurvey of
          MC.CESSurvey _ -> pure $ FL.fold ((,) <$> FL.premap (view GT.stateAbbreviation) FL.set <*> FL.premap (view DT.pWPopPerSqMile) FL.mean) modelData.cesData
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

instance Semigroup (Scenario a) where
  (SimpleScenario t1 f1) <> (SimpleScenario t2 f2) = SimpleScenario (t1 <> "_" <> t2) (\r -> f2 r . f1 r)

instance Semigroup (Scenario a) => Monoid (Scenario a) where
  mempty = SimpleScenario "" (const id)
  mappend = (<>)

applyScenario :: ks F.⊆ rs => Lens' (F.Record rs) Double -> Scenario ks -> F.Record rs -> F.Record rs
applyScenario pLens (SimpleScenario _ f) r = over pLens (f (F.rcast r)) r

scenarioCacheText :: Scenario ks -> Text
scenarioCacheText (SimpleScenario t _) = t

type TotalReg = "TotalReg" F.:-> Int

stateActionTargets ::  (K.KnitEffects r, BRCC.CacheEffects r)
                   => Int -> MC.ModelCategory -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec [GT.StateAbbreviation, DP.TargetPop, DP.ActionTarget]))
stateActionTargets year cat = do
  let stFilter y r = r ^. BRDF.year == y && r ^. GT.stateAbbreviation /= "US"
  stateTurnout_C <- BRDF.stateTurnoutLoader
  case cat of
    MC.Reg -> do
      let stateRegCacheKey = "data/stateReg2022.bin"
          innerFoldR :: FL.Fold (F.Record [RDH.PartyDem, RDH.PartyRep, RDH.PartyOth]) (F.Record '[TotalReg])
          innerFoldR = (\d r o -> FT.recordSingleton @TotalReg $ d + r + o)
                       <$> FL.premap (view RDH.partyDem) FL.sum
                       <*> FL.premap (view RDH.partyRep) FL.sum
                       <*> FL.premap (view RDH.partyOth) FL.sum
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
            g :: (FC.ElemsOf rs [GT.StateAbbreviation, BRDF.VEP, TotalReg]) => F.Record rs  -> F.Record [GT.StateAbbreviation, DP.TargetPop, DP.ActionTarget]
            g r = r ^. GT.stateAbbreviation F.&: r ^. BRDF.vEP F.&: f r F.&: V.RNil --FT.recordSingleton @DP.ActionTarget $ f r
        pure $ fmap g joined --fmap (F.rcast . FT.mutate g) joined
    MC.Vote -> pure
               $ fmap (F.rcast . FT.replaceColumn @BRDF.BallotsCountedVEP @DP.ActionTarget id . FT.replaceColumn @BRDF.VEP @DP.TargetPop id)
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
runActionModelCPAH year cacheStructure mc ac scenarioM psData_C = K.wrapPrefix "runActionModelCPAH" $ do
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
  BRCC.retrieveOrMakeFrame cacheKey ahDeps $ \((tCP, tMPm), psD, stateActionTargets') -> do
    K.logLE K.Info "(Re)building AH adjusted all-cell probs."
    let probFrame =  fmap (\(ks, p) -> ks F.<+> FT.recordSingleton @ModelPr p) $ M.toList $ fmap MT.ciMid $ MC.unPSMap tCP
    tMP <- K.knitMaybe "runActionModelCPAH: Nothing in turnout ModelParameters after allCellProbs run!" $ tMPm
    let (!joined, missing) = FJ.leftJoinWithMissing
                             @StateAndCats
                             @(DP.PSDataR ks)
                             @(StateCatsPlus '[ModelPr])
                             (DP.unPSData psD) (F.toFrame probFrame)
        dmr = ac.acModelConfig.mcDesignMatrixRow
    when (not $ null missing) $ K.knitError $ "runActionModelCPAH: missing keys in psData/prob-frame join: " <> show missing
    let densAdjProbFrame = fmap (MC2.adjustPredictionsForDensity (view modelPr) (over modelPr . const) tMP dmr) joined
        densAdjProbFrame' = case mc of
          MC.Reg -> F.filterFrame ((/= "DC") . view GT.stateAbbreviation) densAdjProbFrame
          MC.Vote -> densAdjProbFrame
    ahProbs <- timed "Doing AH fold for Action"
               $ FL.foldM (TA.adjTurnoutFoldG @ModelPr @'[GT.StateAbbreviation] @_  @(AHrs ks '[ModelPr])
                           (realToFrac . view DT.popCount) (view DP.actionTarget) stateActionTargets') (fmap F.rcast densAdjProbFrame')
    case scenarioM of
      Nothing -> pure ahProbs
      Just s -> pure $ fmap (applyScenario modelPr s) ahProbs

type ActionModelC l ks =
  (
    PSTypeC l ks '[ModelPr]
  , PSDataTypeTC ks
--  , Show (F.Frame (F.Rec F.ElField (l V.++ PSResultR)))
  )

runActionModelAH :: forall l ks r a b .
                    (K.KnitEffects r
                    , BRCC.CacheEffects r
                    , ActionModelC l ks
                    , Typeable ks
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
        whenMatched _ p ci = pure $ adjustCI p ci
        whenMissingCI l p = if p == 0 then pure (MT.ConfidenceInterval 0 0 0)
                            else K.knitError
                                 $ "runActionModelAH: key present in Ps (with non-zero p) is missing from CIs: " <> show l
                                 <> "cisM=" <> show (MC.unPSMap cisM)
        whenMissingPS l _ = K.knitError
                             $ "runActionModelAH: key present in CIs is missing from Ps: " <> show l
                             <> "psProbMap=" <> show psProbMap
    MC.PSMap
      <$> MM.mergeA (MM.traverseMissing whenMissingCI) (MM.traverseMissing whenMissingPS) (MM.zipWithAMatched whenMatched) psProbMap (MC.unPSMap cisM)

type JoinPR ks = FJ.JoinResult3 StateAndCats (DP.PSDataR ks) (StateCatsPlus '[ModelPr]) (StateCatsPlus '[ModelA])

type PSDataTypePC ks = ( FJ.CanLeftJoinWithMissing3 StateAndCats (DP.PSDataR ks) (StateCatsPlus '[ModelPr]) (StateCatsPlus '[ModelA])
                       , FC.ElemsOf (JoinPR ks) [DT.PWPopPerSqMile, ModelPr, ModelA]
                       , StateCatsPlus [ModelPr, ModelA, DT.PopCount] F.⊆ JoinPR ks
                       , FC.ElemsOf (JoinPR ks) [GT.StateAbbreviation, ModelA]
                       , AHC ks '[ModelPr, ModelA]
                       , FC.ElemsOf (FT.Rename "ModelPr" "ModelA" (AH ks '[ModelPr])) (StateCatsPlus '[ModelA])
                       , (GT.StateAbbreviation ': AHrs ks [ModelPr, ModelA]) F.⊆ JoinPR ks
                       , DP.PredictorsR F.⊆ AH ks [ModelPr, ModelA]
                       )

-- For registration we use partisan id as the preference since some states don't have voters register for a particular party
-- And we use 2-party share, D / (D + R) rather than D / (D + R + O)
data PrefDTargetCategory (r :: P.EffectRow) where
  RegDTargets :: PrefDTargetCategory r
  VoteDTargets :: DP.DShareTargetConfig r -> PrefDTargetCategory r
  CESImpliedDVotes :: K.ActionWithCacheTime r (F.FrameRec (DP.StateKeyR V.++ DP.DCatsR V.++ DP.CountDataR V.++ DP.PrefDataR)) -> PrefDTargetCategory r
  CESImpliedDPID :: K.ActionWithCacheTime r (F.FrameRec (DP.StateKeyR V.++ DP.DCatsR V.++ DP.CountDataR V.++ DP.PrefDataR)) -> PrefDTargetCategory r

catFromPrefTargets :: PrefDTargetCategory r -> MC.ModelCategory
catFromPrefTargets RegDTargets = MC.Reg
catFromPrefTargets (VoteDTargets _) = MC.Vote
catFromPrefTargets (CESImpliedDVotes _) = MC.Vote
catFromPrefTargets (CESImpliedDPID _) = MC.Reg

statePrefDTargets :: (K.KnitEffects r, BRCC.CacheEffects r)
                  => PrefDTargetCategory r
                  -> CacheStructure Text Text
                  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec [GT.StateAbbreviation, DP.TargetPop, DP.PrefDTarget]))
statePrefDTargets cat cacheStructure = case cat of
  RegDTargets -> do
    let statePIDCacheKey = "data/statePID2022.bin"
        regF = FL.prefilter ((== CES.VR_Active) . F.rgetField @CES.VRegistrationC) FL.length
        innerFold :: FL.Fold (F.Record [CES.VRegistrationC, CES.PartisanId3]) (F.Record '[DP.PrefDTarget])
        innerFold = (\_a d r -> FT.recordSingleton @DP.PrefDTarget $ if d + r > 0 then realToFrac d / realToFrac (d + r) else 0.5)
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
    stateTurnout_C <- BRDF.stateTurnoutLoader
    let deps = (,) <$> cesData_C <*> stateTurnout_C
    BRCC.retrieveOrMakeFrame statePIDCacheKey deps $ \(ces, stateTurnout) -> do
      let regTargets = FL.fold outerFold ces
          (joined, missing) = FJ.leftJoinWithMissing @'[GT.StateAbbreviation] regTargets $ F.filterFrame ((== 2020) . view BRDF.year) stateTurnout
      when (not $ null missing) $ K.knitError $ "statePrefDTargets: missing keys in reg/turnout join: k=" <> show missing
      pure $ fmap (F.rcast . FT.replaceColumn @BRDF.VEP @DP.TargetPop id) joined
  VoteDTargets dShareTargetConfig -> do
    dVoteTargets_C <- DP.dShareTarget (csProjectCacheDirE cacheStructure) dShareTargetConfig
    pure $ fmap (F.rcast . FT.replaceColumn @ET.DemShare @DP.PrefDTarget id) <$> dVoteTargets_C
  CESImpliedDVotes ces_C -> do
    stateTurnout_C <- BRDF.stateTurnoutLoader
    let deps = (,) <$> ces_C <*> stateTurnout_C
        f (ces, stateTurnout) = do
          let wsdPS = FL.fold (psFold @'[GT.StateAbbreviation]
                               (\r -> realToFrac (r ^. DP.dVotesW))
                               (\r -> realToFrac (r ^. DP.votesInRaceW))
                               (const 1))
                      $ F.filterFrame ((> 0) . view DP.votesInRace) ces
--          let wsd = weightedSurveyData @'[GT.StateAbbreviation]
--                    (view DP.surveyWeight)
--                    (\r -> realToFrac (r ^. DP.dVotes) / realToFrac (r ^. DP.votesInRace))
--                    $ F.filterFrame ((> 0) . view DP.votesInRace) ces
              (joined, missing) = FJ.leftJoinWithMissing @'[GT.StateAbbreviation] wsdPS $ F.filterFrame ((== 2020) . view BRDF.year) stateTurnout
          when (not $ null missing) $ K.knitError $ "statePrefDTargets: Missing keys in weighted survey/state-turnout join: " <> show missing
          pure $ fmap (F.rcast @[GT.StateAbbreviation, DP.TargetPop, DP.PrefDTarget]
                        . FT.replaceColumn @ModelPr @DP.PrefDTarget id
                        . FT.replaceColumn @BRDF.VAP @DP.TargetPop id) joined
    pure $ K.wctBind f deps
  CESImpliedDPID _ces_C -> undefined


runPrefModelCPAH :: forall ks r a b .
                  (K.KnitEffects r
                  , BRCC.CacheEffects r
                  , PSDataTypeTC ks
                  , PSDataTypePC ks
                  , Typeable ks
                  )
                 => Int
                 -> CacheStructure Text Text
                 -> MC.ActionConfig a b  -- we need a turnout model for the AH adjustment
                 -> Maybe (Scenario DP.PredictorsR)
                 -> MC.PrefConfig b
                 -> Maybe (Scenario DP.PredictorsR)
                 -> PrefDTargetCategory r -- DP.DShareTargetConfig r
                 -> K.ActionWithCacheTime r (DP.PSData ks)
                 -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (AH ks '[ModelPr, ModelA])))
runPrefModelCPAH year cacheStructure ac aScenarioM pc pScenarioM prefDTargetCategory psData_C = K.wrapPrefix "runPrefModelCPAH" $ do
  let mc = catFromPrefTargets prefDTargetCategory
  actionCPAH_C <- runActionModelCPAH year cacheStructure mc ac aScenarioM psData_C
  prefCPs_C <- modelCPs year (allCellCacheStructure cacheStructure) (MC2.PrefOnly mc pc)
  prefDTargets_C <- statePrefDTargets prefDTargetCategory cacheStructure --DP.dShareTarget (csProjectCacheDirE cacheStructure) dShareTargetConfig
  let ahDeps = (,,,) <$> actionCPAH_C <*> prefCPs_C <*> psData_C <*> prefDTargets_C
      (prefCacheDir, prefTargetText) = case prefDTargetCategory of
        RegDTargets -> ("Reg/CES", "PID")
        VoteDTargets dShareTargetConfig -> ("Pref/CES", DP.dShareTargetText dShareTargetConfig)
        CESImpliedDVotes _ -> ("Pref/CES", "CES_Implied")
        CESImpliedDPID _ -> ("Reg/CES", "CES_Implied")
      cacheSuffix = prefCacheDir <> show year <> "_" <> MC.modelConfigText pc.pcModelConfig <> "/"
                    <> csAllCellPSPrefix cacheStructure
                    <> maybe "" (("_" <>) .  scenarioCacheText) aScenarioM
                    <> maybe "" (("_" <>) .  scenarioCacheText) pScenarioM
                    <>  "_" <> prefTargetText
                    <> "_ACProbsAH.bin"
  cpahCacheKey <- BRCC.cacheFromDirE (csProjectCacheDirE cacheStructure) cacheSuffix
  BRCC.retrieveOrMakeFrame cpahCacheKey ahDeps $ \(tCPF, (pCP, pMPm), psD, prefTarget) -> do
    K.logLE K.Info "(Re)building AH adjusted all-cell probs."
    let probFrame =  F.toFrame $ fmap (\(ks, p) -> ks F.<+> FT.recordSingleton @ModelPr p) $ M.toList $ fmap MT.ciMid $ MC.unPSMap pCP
        turnoutFrame = fmap (F.rcast @'[ModelA] . FT.rename @"ModelPr" @"ModelA") tCPF
    tMP <- K.knitMaybe "runTurnoutPrefAH: Nothing in pref ModelParameters after allCellProbs run!" $ pMPm
    K.logLE K.Info $ "3-way join for Pref: psData has " <> show (F.frameLength (DP.unPSData psD))
      <> " rows; probFrame has " <> show (F.frameLength probFrame)
      <> " rows; turnoutFrame has " <> show (F.frameLength turnoutFrame) <> " rows."
    let (joinedPref, missingPSPref) = FJ.leftJoinWithMissing
                                      @StateAndCats
                                      (DP.unPSData psD) probFrame
        joined = F.zipFrames joinedPref turnoutFrame
    when (not $ null missingPSPref) $ K.knitError $ "runPrefModelAH: missing keys in psData/prob-frame join: " <> show missingPSPref
--    when (not $ null missingA) $ K.knitError $ "runPrefModelAH: missing keys in psData+prob-frame/turnout frame join: " <> show missingA
    let dmr = pc.pcModelConfig.mcDesignMatrixRow
        adjForDensity = MC2.adjustPredictionsForDensity (view modelPr) (over modelPr . const) tMP dmr
        densAdjProbFrame = fmap adjForDensity joined
        modeledVoters r = r ^. modelA * realToFrac (r ^. DT.popCount)
    ahProbs <-  timed "Doing AH fold for Pref"
                $ FL.foldM
                (TA.adjTurnoutFoldG @ModelPr @'[GT.StateAbbreviation] @_ @(AHrs ks [ModelPr, ModelA]) modeledVoters (view DP.prefDTarget) prefTarget)
                (fmap F.rcast densAdjProbFrame)
    case pScenarioM of
      Nothing -> pure ahProbs
      Just s -> pure $ fmap (applyScenario modelPr s) ahProbs

type PrefModelC l ks =
  (
    PSTypeC l ks '[ModelPr, ModelA]
  , PSDataTypeTC ks
  , PSDataTypePC ks
  )

runPrefModelAH :: forall l ks r a b .
                  (K.KnitEffects r
                  , BRCC.CacheEffects r
                  , PrefModelC l ks
                  , Typeable ks
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
runPrefModelAH year cacheStructure ac aScenarioM pc pScenarioM prefDTargetCategory psData_C = K.wrapPrefix "runPrefModelAH" $ do
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
        CESImpliedDVotes _ -> ("Pref/CES", "CES_Implied")
        CESImpliedDPID _ -> ("Reg/CES", "CES_Implied")
      cacheSuffix = prefCacheDir <> show year <> "_" <> MC.modelConfigText pc.pcModelConfig
                    <> csPSName cacheStructure
                    <> maybe "" (("_" <>) .  scenarioCacheText) aScenarioM
                    <> maybe "" (("_" <>) .  scenarioCacheText) pScenarioM
                    <> "_" <> prefTargetText <> "_resMap.bin"
  ahCacheKey <- BRCC.cacheFromDirE (csProjectCacheDirE cacheStructure) cacheSuffix
  BRCC.retrieveOrMakeD ahCacheKey resMapDeps $ \(ahps, (cisM, _)) -> do
    K.logLE K.Info "merging AH probs and CIs"
    let psProbMap = M.fromList $ fmap (\r -> (F.rcast @l r, r ^. modelPr)) $ FL.fold FL.list ahps
        whenMatched _ p ci = pure $ adjustCI p ci
        whenMissingCI l p = if p == 0 then pure (MT.ConfidenceInterval 0 0 0)
                                           else K.knitError
                                                $ "runPrefModelAH: key present in model p's (with p /= 0) is missing from AHPS: " <> show l
                                                <> "cisMap=" <> show (MC.unPSMap cisM)
        whenMissingPS l _ = K.knitError
                            $ "runPrefModelAH: key present in CIs is missing from probs: " <> show l <> "psProbMap=" <> show psProbMap
    MC.PSMap
      <$> MM.mergeA (MM.traverseMissing whenMissingCI) (MM.traverseMissing whenMissingPS) (MM.zipWithAMatched whenMatched) psProbMap (MC.unPSMap cisM)

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
type ToJoinPR ks = FT.Rename "ModelPr" "ModelP" (AH ks '[ModelPr, ModelA]) --(ks V.++ (DP.DCatsR V.++ [ModelPr, DT.PopCount]))
type JoinFR ks = FJ.JoinResult (ks V.++ DP.DCatsR) (DP.PSDataR ks) (ToJoinPR ks)

type FullModelC l ks =
  (
    PSTypeC l ks '[ModelPr, ModelA]
  , PSDataTypeTC ks
  , PSDataTypePC ks
  , FJ.CanLeftJoinWithMissing  (ks V.++ DP.DCatsR) (DP.PSDataR ks) (ToJoinPR ks)
  , l F.⊆ JoinFR ks
  , JoinFR ks F.⊆ JoinFR ks
  , FC.ElemsOf (JoinFR ks) [DT.PopCount, ModelA, ModelP]
  , V.RMap (l V.++ PSResultR)
  , FS.RecFlat (l V.++ PSResultR)
  , Show (F.Record (ks V.++ DP.DCatsR))
  , Typeable ks
  )


runFullModelAH :: forall l ks r a b .
                  (K.KnitEffects r
                  , BRCC.CacheEffects r
                  , FullModelC l ks
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
        CESImpliedDVotes _ -> ("Pref/CES", "CES_Implied")
        CESImpliedDPID _ -> ("Reg/CES", "CES_Implied")
  let cacheMid =  fullCacheDir <> MC.actionSurveyText ac.acSurvey <> show year <> "_" <> MC.modelConfigText pc.pcModelConfig
      ahpsCacheSuffix = cacheMid
                        <> csPSName cacheStructure --csAllCellPSPrefix cacheStructure
                        <> maybe "" (("_" <>) .  scenarioCacheText) aScenarioM
                        <> maybe "" (("_" <>) .  scenarioCacheText) pScenarioM
                        <>  "_" <> prefTargetText <> "_PS.bin"
  ahpsCacheKey <- BRCC.cacheFromDirE (csProjectCacheDirE cacheStructure) ahpsCacheSuffix
  let joinDeps = (,) <$> prefCPAH_C <*> psData_C
  fullAHPS_C <- BRCC.retrieveOrMakeFrame ahpsCacheKey joinDeps $ \(pref, psData) -> do
    K.logLE K.Info "Doing psData/modeledPref join..."
    let-- turnoutFrame = fmap (FT.rename @"ModelPr" @"ModelA") turnout
        prefFrame = fmap (FT.rename @"ModelPr" @"ModelP") pref
        (joined, missing) = FJ.leftJoinWithMissing
                            @(ks V.++ DP.DCatsR)
                            (DP.unPSData psData) prefFrame
    when (not $ null missing) $ K.knitError $ "runFullModelAH: Missing keys in psData/prefCPAH join: " <> show missing
    K.logLE K.Info "Doing post-stratification..."
    let ppl r = realToFrac $ r ^. DT.popCount
        t r = r ^. modelA
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
        whenMatched _ p ci = pure $ adjustCI p ci
        whenMissingCI l p = if p == 0
                            then pure (MT.ConfidenceInterval 0 0 0)
                            else K.knitError
                                 $ "runFullModelAH: key present in p's (with p /= 0) is missing from model CIs:" <> show l
                                 <> "ciMap=" <> show (MC.unPSMap cisM)
        whenMissingPS l _ = K.knitError
                            $ "runFullModelAH: key present in CIs is missing from probs: " <> show l <> "psProbs=" <> show psProbMap
    MC.PSMap
      <$> MM.mergeA (MM.traverseMissing whenMissingCI) (MM.traverseMissing whenMissingPS) (MM.zipWithAMatched whenMatched) psProbMap (MC.unPSMap cisM)

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


modelCompBy :: forall ks ks' r b . (K.KnitEffects r, BRCC.CacheEffects r, PSByC ks)
            => (MC.SurveyAggregation b -> K.Sem r (K.ActionWithCacheTime r (DP.PSData ks')))
            -> (K.ActionWithCacheTime r (DP.PSData ks') -> Text -> MC.SurveyAggregation b -> MC.Alphas ->  K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
            -> Text -> MC.SurveyAggregation b -> MC.Alphas -> K.Sem r (Text, F.FrameRec (ks V.++ '[DT.PopCount, ModelCI]))
modelCompBy psByAgg runModel catLabel agg am = do
  K.logLE K.Info $ "Model Comp By for " <> catLabel
  psData_C <- psByAgg agg
  comp <- psBy @ks (runModel psData_C (catLabel <> MC.addAggregationText agg) agg am)
  pure (MC.aggregationText agg <> "_" <> MC.alphasText am, comp)

allModelsCompBy :: forall ks ks' r b . (K.KnitEffects r, BRCC.CacheEffects r, PSByC ks)
                => (MC.SurveyAggregation b -> K.Sem r (K.ActionWithCacheTime r (DP.PSData ks')))
                -> (K.ActionWithCacheTime r (DP.PSData ks') -> Text -> MC.SurveyAggregation b -> MC.Alphas ->  K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
                -> Text -> [MC.SurveyAggregation b] -> [MC.Alphas] -> K.Sem r [(Text, F.FrameRec (ks V.++ '[DT.PopCount, ModelCI]))]
allModelsCompBy psByAgg runModel catLabel aggs' alphaModels' =
  traverse (\(agg, am) -> modelCompBy @ks psByAgg runModel catLabel agg am) [(agg, am) | agg <- aggs',  am <- alphaModels']

forStatesChart ::  (F.RDelete ModelCI (ks V.++ '[DT.PopCount, ModelCI]) V.++ '[ModelPr] F.⊆ ('[ModelPr] V.++ (ks V.++ '[DT.PopCount, ModelCI]))
                   , F.ElemOf (ks V.++ '[DT.PopCount, ModelCI]) ModelCI)
               => Text -> [(Text, F.FrameRec (ks V.++ '[DT.PopCount, ModelCI]))]
               -> [(Text, F.FrameRec (F.RDelete ModelCI (ks V.++ '[DT.PopCount, ModelCI]) V.++ '[ModelPr]))]
forStatesChart t = fmap (first (t <>))  . fmap (second $ fmap modelCIToModelPr)

allModelsCompChart :: forall ks ks' r b pbase . (K.KnitOne r, BRCC.CacheEffects r, PSByC ks, Keyed.FiniteSet (F.Record ks)
                                            , F.ElemOf (ks V.++ [DT.PopCount, ModelCI]) DT.PopCount
                                            , F.ElemOf (ks V.++ [DT.PopCount, ModelCI]) ModelCI
                                            , ks F.⊆ (ks V.++ [DT.PopCount, ModelCI])
                                            , ks F.⊆ (ks V.++ '[ModelPr])
                                            , FSI.RecVec (ks V.++ '[ModelPr])
                                            , ks F.⊆ (DP.CDKeyR V.++ DP.DCatsR V.++ DP.CountDataR V.++ DP.PrefDataR)
                                            , F.ElemOf (ks V.++ '[ModelPr]) ModelPr
                                        )
                   => BRHJ.JsonLocations pbase
                   -> (forall c. SurveyDataBy ks (DP.CDKeyR V.++ DP.DCatsR V.++ DP.CountDataR V.++ DP.PrefDataR) c)
                   -> (MC.SurveyAggregation b -> K.Sem r (K.ActionWithCacheTime r (DP.PSData ks')))
                   -> (K.ActionWithCacheTime r (DP.PSData ks') -> Text -> MC.SurveyAggregation b -> MC.Alphas ->  K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
                   -> Text
                   -> Text
                   -> (F.Record ks -> Text)
                   -> [MC.SurveyAggregation b]
                   -> [MC.Alphas]
                   -> K.Sem r ()
allModelsCompChart jsonLocations surveyDataBy psByAgg runModel catLabel modelType catText aggs' alphaModels' = do
--  let weightingStyle = DP.CellWeights
  allModels <- allModelsCompBy @ks psByAgg runModel catLabel aggs' alphaModels'
  cesSurvey <- K.ignoreCacheTimeM $ DP.cesCountedDemPresVotesByCD False (DP.AllSurveyed DP.Both)
  cesSurveyVV <- K.ignoreCacheTimeM $ DP.cesCountedDemPresVotesByCD False (DP.Validated DP.Both)
  let
--      cesCompUW = surveyDataBy Nothing cesSurvey
--      cesCompUW_VV = surveyDataBy Nothing cesSurveyVV
      cesCompFW = surveyDataBy  (Just DP.FullWeights) cesSurvey
      cesCompFW_VV = surveyDataBy  (Just DP.FullWeights) cesSurveyVV
      cesCompCW = surveyDataBy  (Just DP.CellWeights) cesSurvey
      cesCompCW_VV = surveyDataBy  (Just DP.CellWeights) cesSurveyVV
      cesCompDEW = surveyDataBy  (Just DP.DesignEffectWeights) cesSurvey
      cesCompDEW_VV = surveyDataBy  (Just DP.DesignEffectWeights) cesSurveyVV

{-      cesCompPW = surveyDataBy  (Just $ MC.RoundedWeightedAggregation) cesSurveyPW
      cesCompPW_VV = surveyDataBy  (Just $ MC.RoundedWeightedAggregation) cesSurveyPWVV
      cesCompRPW = surveyDataBy (Just $ MC.WeightedAggregation MC.ContinuousBinomial) cesSurveyPW
      cesCompRPW_VV = surveyDataBy (Just $ MC.WeightedAggregation MC.ContinuousBinomial) cesSurveyPWVV
--      cesCompDW = surveyDataBy (Just $ MC.RoundedWeightedAggregation) cesSurveyDEW
--      cesCompRDW = surveyDataBy (Just $ MC.WeightedAggregation MC.ContinuousBinomial) cesSurveyDEW
-}
  let cats = Set.toList $ Keyed.elements @(F.Record ks)
      _numCats = length cats
      numSources = length allModels
  catCompChart <- categoryChart @ks jsonLocations (modelType <> " Comparison By Category") (modelType <> "Comp")
                  (FV.fixedSizeVC 300 (50 * realToFrac numSources) 10) (Just cats) (Just $ fmap fst allModels)
                  catText allModels (Just [--("UW Survey", cesCompUW),("UW VV Survey", cesCompUW_VV)
                                        ("FW Survey", cesCompFW), ("FW VV Survey", cesCompFW_VV)
                                        ,("CW Survey", cesCompCW), ("CW VV Survey", cesCompCW_VV)
                                        , ("DEW Survey", cesCompDEW), ("DEW VV Survey", cesCompDEW_VV)
{-                                          , ("PW Survey", cesCompPW), ("PW VV Survey", cesCompPW_VV)
                                          , ("W Survey", cesCompW), ("W VV Survey", cesCompW_VV)
-}
                                          ]
                                    )
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
        whenMissingT k p = pure $ k F.<+> (p F.&: MT.ConfidenceInterval 0 0.5 1 F.&: V.RNil :: F.Record [DT.PopCount, ModelCI])
        -- whenMissingT k _ = Left $ "psBy: " <> show k <> " is missing from probs map: (size=" <> show (M.size psMap) <> ")"
    mergedMap <- K.knitEither
                 $ MM.mergeA (MM.traverseMissing whenMissingPC) (MM.traverseMissing whenMissingT) (MM.zipWithAMatched whenMatched) psMap pcMap
    pure $ F.toFrame $ M.elems mergedMap


psByState ::  (K.KnitEffects r, BRCC.CacheEffects r)
          => (K.Sem r (K.ActionWithCacheTime r (MC.PSMap '[GT.StateAbbreviation] MT.ConfidenceInterval)))
          -> (F.FrameRec [GT.StateAbbreviation, DT.PopCount, ModelCI] -> K.Sem r (F.FrameRec ([GT.StateAbbreviation, DT.PopCount, ModelCI] V.++ bs)))
          -> K.Sem r (F.FrameRec ([GT.StateAbbreviation, DT.PopCount, ModelCI] V.++ bs))
psByState runModel addStateFields = psBy @'[GT.StateAbbreviation] runModel >>= addStateFields


popCountBy' :: forall ks rs .
              (--K.KnitEffects r
--              , BRCC.CacheEffects r
                ks F.⊆ rs, F.ElemOf rs DT.PopCount, Ord (F.Record ks)
              , FSI.RecVec (ks V.++ '[DT.PopCount])
              )
           => F.FrameRec rs
           -> F.FrameRec (ks V.++ '[DT.PopCount])
popCountBy' = FL.fold aggFld
  where aggFld = FMR.concatFold
                 $ FMR.mapReduceFold
                 FMR.noUnpack
                 (FMR.assignKeysAndData @ks @'[DT.PopCount])
                 (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)


popCountBy :: forall ks rs r .
              (--K.KnitEffects r
--              , BRCC.CacheEffects r
                ks F.⊆ rs, F.ElemOf rs DT.PopCount, Ord (F.Record ks)
              , FSI.RecVec (ks V.++ '[DT.PopCount])
              )
           => K.ActionWithCacheTime r (F.FrameRec rs)
           -> K.Sem r (F.FrameRec (ks V.++ '[DT.PopCount]))
popCountBy counts_C = popCountBy' @ks <$> K.ignoreCacheTime counts_C

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


type SurveyDataBy ks rs a =
                (Ord (F.Record ks)
                , ks F.⊆ rs
--                , DP.CountDataR F.⊆ rs
--                , DP.PrefDataR  F.⊆ rs
                , FSI.RecVec (ks V.++ '[ModelPr])
                )
             => Maybe DP.WeightingStyle -> F.FrameRec rs -> F.FrameRec (ks V.++ '[ModelPr])

{-
surveyDataBy :: forall ks rs a .
                (Ord (F.Record ks)
                , ks F.⊆ rs
                , DP.CountDataR F.⊆ rs
                , FSI.RecVec (ks V.++ '[ModelPr])
                )
             => Maybe (MC.SurveyAggregation a) -> F.FrameRec rs -> F.FrameRec (ks V.++ '[ModelPr])
-}
turnoutDataBy :: forall ks rs a. (DP.CountDataR F.⊆ rs) => SurveyDataBy ks rs a
turnoutDataBy wsM = FL.fold fld
  where
    safeDiv :: Double -> Double -> Double
    safeDiv x y = if (y /= 0) then x / y else 0
    uwInnerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    uwInnerFld =
      let sF = fmap realToFrac $ FL.premap (view DP.surveyed) FL.sum
          vF = fmap realToFrac $ FL.premap (view DP.voted) FL.sum
      in (\v s -> safeDiv v s F.&: V.RNil) <$> vF <*> sF
    wInnerFld :: DP.WeightingStyle -> FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    wInnerFld ws = (\(s, v) -> safeDiv v s F.&: V.RNil) <$> DP.weightedFld' ws (view DP.surveyed) (view DP.surveyWeight) (view DP.surveyedESS) (view DP.votedW)
    innerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    innerFld = maybe uwInnerFld wInnerFld wsM
    fld :: FL.Fold (F.Record rs) (F.FrameRec (ks V.++ '[ModelPr]))
    fld = FMR.concatFold
          $ FMR.mapReduceFold
          FMR.noUnpack
          (FMR.assignKeysAndData @ks @DP.CountDataR)
          (FMR.foldAndAddKey innerFld)

regDataBy :: forall ks rs a. (DP.CountDataR F.⊆ rs) => SurveyDataBy ks rs a
regDataBy wsM = FL.fold fld
  where
    safeDiv :: Double -> Double -> Double
    safeDiv x y = if (y /= 0) then x / y else 0
    uwInnerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    uwInnerFld =
      let sF = fmap realToFrac $ FL.premap (view DP.surveyed) FL.sum
          vF = fmap realToFrac $ FL.premap (view DP.registered) FL.sum
      in (\v s -> safeDiv v s F.&: V.RNil) <$> vF <*> sF
    wInnerFld :: DP.WeightingStyle -> FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    wInnerFld ws = (\(s, v) -> safeDiv v s F.&: V.RNil) <$> DP.weightedFld' ws (view DP.surveyed) (view DP.surveyWeight) (view DP.surveyedESS) (view DP.registeredW)
    innerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    innerFld = maybe uwInnerFld wInnerFld wsM
    fld :: FL.Fold (F.Record rs) (F.FrameRec (ks V.++ '[ModelPr]))
    fld = FMR.concatFold
          $ FMR.mapReduceFold
          FMR.noUnpack
          (FMR.assignKeysAndData @ks @DP.CountDataR)
          (FMR.foldAndAddKey innerFld)

prefDataBy :: forall ks rs a. (DP.CountDataR V.++ DP.PrefDataR F.⊆ rs, F.ElemOf rs DP.VotesInRaceW) => SurveyDataBy ks rs a
prefDataBy wsM = FL.fold fld
  where
    safeDiv :: Double -> Double -> Double
    safeDiv x y = if (y /= 0) then x / y else 0
    uwInnerFld :: FL.Fold  (F.Record (DP.CountDataR V.++ DP.PrefDataR)) (F.Record '[ModelPr])
    uwInnerFld =
      let sF = fmap realToFrac $ FL.premap (view DP.votesInRace) FL.sum
          vF = fmap realToFrac $ FL.premap (view DP.dVotes) FL.sum
      in (\v s -> safeDiv v s F.&: V.RNil) <$> vF <*> sF
    wInnerFld :: DP.WeightingStyle -> FL.Fold  (F.Record (DP.CountDataR V.++ DP.PrefDataR)) (F.Record '[ModelPr])
    wInnerFld ws = (\(s, v) -> safeDiv v s F.&: V.RNil) <$> DP.weightedFld' ws (view DP.votesInRace) (view DP.votesInRaceW) (view DP.votesInRaceESS) (view DP.dVotesW)
    innerFld :: FL.Fold (F.Record (DP.CountDataR V.++ DP.PrefDataR)) (F.Record '[ModelPr])
    innerFld = maybe uwInnerFld wInnerFld wsM
    fld :: FL.Fold (F.Record rs) (F.FrameRec (ks V.++ '[ModelPr]))
    fld = FMR.concatFold
          $ FMR.mapReduceFold
          (FMR.unpackFilterOnField @DP.VotesInRaceW (> 0))
          (FMR.assignKeysAndData @ks @(DP.CountDataR V.++ DP.PrefDataR))
          (FMR.foldAndAddKey innerFld)

weightedSurveyData :: forall ks rs .
                      (
                        FSI.RecVec (ks V.++ '[WgtdX])
                      , Ord (F.Record ks)
                      , ks F.⊆ rs
                      , rs F.⊆ rs
                      )
                   => (F.Record rs -> Double)
                   -> (F.Record rs -> Double)
                   -> F.FrameRec rs
                   -> F.FrameRec (ks V.++ '[WgtdX])
weightedSurveyData wgt qty  = FL.fold fld
  where
    sumWgtF = FL.premap wgt FL.sum
    g r = wgt r * qty r
    wgtdSumF = FL.premap g FL.sum
    safeDiv x y = if y /= 0 then x / y else 0
    innerFld = fmap (FT.recordSingleton @WgtdX) . safeDiv <$> wgtdSumF <*> sumWgtF
    fld = FMR.concatFold
          $ FMR.mapReduceFold
          FMR.noUnpack
          (FMR.assignKeysAndData @ks @rs)
          (FMR.foldAndAddKey innerFld)

surveyPSData :: forall ks rs . ((ks V.++ DP.DCatsR) F.⊆ rs
                               , rs F.⊆ rs
                               , DP.PSDataR ks F.⊆ (ks V.++ DP.DCatsR V.++ [DT.PopCount, DT.PWPopPerSqMile])
                               , FSI.RecVec (ks V.++ DP.DCatsR V.++ [DT.PopCount, DT.PWPopPerSqMile])
                               , Ord (F.Record (ks V.++ DP.DCatsR))
                               )
             => (F.Record rs -> Double)
             -> (F.Record rs -> Double)
             -> (F.Record rs -> Double)
             -> F.FrameRec rs
             -> DP.PSData ks
surveyPSData wgt ppl dens = DP.PSData . fmap F.rcast . FL.fold fld
  where
    toRec :: (Double, Double) -> F.Record [DT.PopCount, DT.PWPopPerSqMile]
    toRec (w, d) = round w F.&: d F.&: V.RNil
    innerFld = fmap toRec $ DT.densityAndPopFld'' DT.Geometric (const 1) wgt ppl dens
    fld = FMR.concatFold
          $ FMR.mapReduceFold
          FMR.noUnpack
          (FMR.assignKeysAndData @(ks V.++ DP.DCatsR) @rs)
          (FMR.foldAndAddKey innerFld)


type PSMapProductC ks = ((ks V.++ DP.DCatsR) F.⊆ DP.PSDataR ks
                        , V.ReifyConstraint Show F.ElField (ks V.++ DP.DCatsR)
                        , V.RecordToList (ks V.++ DP.DCatsR)
                        , Ord (F.Record (ks V.++ DP.DCatsR))
                        , FC.ElemsOf (DP.PSDataR ks) '[DT.PopCount]
                        , FSI.RecVec (DP.PSDataR ks)
                        , V.RMap (ks V.++ DP.DCatsR)
                        )

psMapProduct :: forall ks a r . (K.KnitEffects r, PSMapProductC ks)
                => (a -> Int -> Int) -> DP.PSData ks -> Map (F.Record (ks V.++ DP.DCatsR)) a ->  K.Sem r (DP.PSData ks)
psMapProduct merge (DP.PSData psF)  m = do
  let mergeRow r = do
        let k = F.rcast @(ks V.++ DP.DCatsR) r
        a <- K.knitMaybe ("psMapProduct: lookup failed for k=" <> show k) $ M.lookup k m
        pure $ over DT.popCount (merge a) r
  DP.PSData . F.toFrame <$> traverse mergeRow (FL.fold FL.list psF)


psProduct :: forall ks rs r . (PSMapProductC ks, K.KnitEffects r)
          => (F.Record rs -> F.Record (ks V.++ DP.DCatsR)) -> (F.Record rs -> Double) -> DP.PSData ks -> F.FrameRec rs -> K.Sem r (DP.PSData ks)
psProduct getKey getFrac psD fs = do
  let fMap = FL.fold (FL.premap (\r -> (getKey r, getFrac r)) FL.map) fs
      merge f pop = round $ f * realToFrac pop
  psMapProduct merge psD fMap

addActionTargets :: (K.KnitEffects r, BRCC.CacheEffects r
                    , FJ.CanLeftJoinM '[GT.StateAbbreviation] rs [GT.StateAbbreviation, DP.TargetPop, DP.ActionTarget]
                    , F.ElemOf (rs V.++ (F.RDelete GT.StateAbbreviation [GT.StateAbbreviation, DP.TargetPop, DP.ActionTarget])) GT.StateAbbreviation
                    , rs V.++ '[DP.TargetPop, DP.ActionTarget] F.⊆ (rs V.++ (F.RDelete GT.StateAbbreviation [GT.StateAbbreviation, DP.TargetPop, DP.ActionTarget]))
                    )
                 => F.FrameRec [GT.StateAbbreviation, DP.TargetPop, DP.ActionTarget]
                 -> F.FrameRec rs ->  K.Sem r (F.FrameRec (rs V.++ '[DP.TargetPop, DP.ActionTarget]))
addActionTargets tgts fr = do
  let (joined, missing) = FJ.leftJoinWithMissing @'[GT.StateAbbreviation] fr tgts
  when (not $ null missing) $ K.logLE K.Warning $ "addActionTargets: missing keys in state turnout target join: " <> show missing
  pure $ fmap F.rcast joined

addPrefTargets :: (K.KnitEffects r, BRCC.CacheEffects r
                    , FJ.CanLeftJoinM '[GT.StateAbbreviation] rs [GT.StateAbbreviation, DP.TargetPop, DP.PrefDTarget]
                    , F.ElemOf (rs V.++ (F.RDelete GT.StateAbbreviation [GT.StateAbbreviation, DP.TargetPop, DP.PrefDTarget])) GT.StateAbbreviation
                    , rs V.++ '[DP.TargetPop, DP.ActionTarget] F.⊆ (F.RDelete DP.PrefDTarget (rs V.++ [DP.TargetPop, DP.PrefDTarget]) V.++ '[DP.ActionTarget])
                    ,  (F.RDelete DP.PrefDTarget (rs V.++ [DP.TargetPop, DP.PrefDTarget]) V.++ '[DP.ActionTarget])  F.⊆
                      ((rs V.++ [DP.TargetPop, DP.PrefDTarget]) V.++ '[DP.ActionTarget])
                    , FC.ElemsOf (rs V.++ [DP.TargetPop, DP.PrefDTarget]) '[DP.PrefDTarget]
                    )
                 => F.FrameRec [GT.StateAbbreviation, DP.TargetPop, DP.PrefDTarget]
                 -> F.FrameRec rs ->  K.Sem r (F.FrameRec (rs V.++ '[DP.TargetPop, DP.ActionTarget]))
addPrefTargets tgts fr = do
  let (joined, missing) = FJ.leftJoinWithMissing @'[GT.StateAbbreviation] fr tgts
  when (not $ null missing) $ K.logLE K.Warning $ "addPrefTargets: missing keys in state turnout target join: " <> show missing
  pure $ fmap (F.rcast . FT.replaceColumn @DP.PrefDTarget @DP.ActionTarget id) joined



addBallotsCountedVAP :: (K.KnitEffects r, BRCC.CacheEffects r
                        , FJ.CanLeftJoinM '[GT.StateAbbreviation] rs BRDF.StateTurnoutCols
                        , F.ElemOf (rs V.++ (F.RDelete GT.StateAbbreviation BRDF.StateTurnoutCols)) GT.StateAbbreviation
                        , rs V.++ '[BRDF.BallotsCountedVAP, BRDF.VAP] F.⊆ (rs V.++ (F.RDelete GT.StateAbbreviation BRDF.StateTurnoutCols))
                        )
                     => F.FrameRec rs ->  K.Sem r (F.FrameRec (rs V.++ '[BRDF.BallotsCountedVAP, BRDF.VAP]))
addBallotsCountedVAP fr = do
  turnoutByState <- F.filterFrame ((== 2020) . view BRDF.year) <$> K.ignoreCacheTimeM BRDF.stateTurnoutLoader
  let (joined, missing) = FJ.leftJoinWithMissing @'[GT.StateAbbreviation] fr turnoutByState
  when (not $ null missing) $ K.logLE K.Warning $ "addBallotsCOuntedVAP: missing keys in state turnout target join: " <> show missing
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



oneCatRatioFld :: (F.Record rs -> Double) -> (F.Record rs -> Double) -> (F.Record rs -> Bool) -> FL.Fold (F.Record rs) Double
oneCatRatioFld n d test = FL.prefilter test ((/) <$> FL.premap n FL.sum <*> FL.premap d FL.sum)

ratioFld :: forall ks rs . (Ord (F.Record ks), ks F.⊆ rs, rs F.⊆ rs, FSI.RecVec (ks V.++ '[RatioResult]))
               => (F.Record rs -> Double) -> (F.Record rs -> Double) -> FL.Fold (F.Record rs) (F.FrameRec (ks V.++ '[RatioResult]))
ratioFld n d = FMR.concatFold
               $ FMR.mapReduceFold
               FMR.noUnpack
               (FMR.assignKeysAndData @ks @rs)
               (FMR.foldAndAddKey innerFld)
  where
    innerFld = fmap (FT.recordSingleton @RatioResult) $ ((/) <$> FL.premap n FL.sum <*> FL.premap d FL.sum)

sumFld :: forall ks rs . (Ord (F.Record ks), ks F.⊆ rs, rs F.⊆ rs, FSI.RecVec (ks V.++ '[SumResult]))
       => (F.Record rs -> Double) ->  FL.Fold (F.Record rs) (F.FrameRec (ks V.++ '[SumResult]))
sumFld toX =  FMR.concatFold
              $ FMR.mapReduceFold
              FMR.noUnpack
              (FMR.assignKeysAndData @ks @rs)
              (FMR.foldAndAddKey innerFld)
  where
    innerFld = fmap (FT.recordSingleton @SumResult) (FL.premap toX FL.sum)

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



categoryChart :: forall ks rs pbase r . (K.KnitEffects r, F.ElemOf rs DT.PopCount, F.ElemOf rs ModelCI, ks F.⊆ rs
                                        , ks F.⊆ (ks V.++ '[ModelPr])
--                                        , F.ElemOf (ks V.++ '[ModelPr]) DT.PopCount
                                        , F.ElemOf (ks V.++ '[ModelPr]) ModelPr
                                        )
              => BRHJ.JsonLocations pbase
              -> Text
              -> Text
              -> FV.ViewConfig
              -> Maybe [F.Record ks]
              -> Maybe [Text]
              -> (F.Record ks -> Text)
              -> [(Text, F.FrameRec rs)] --[k, TurnoutCI, DT.PopCount])]
              -> Maybe [(Text, F.FrameRec (ks V.++ '[ModelPr]))]
              -> K.Sem r GV.VegaLite
categoryChart jsonLocations title chartID vc catSortM sourceSortM catText tableRowsByModel surveyRowsM = do
  let colDataModel (t, r)
        = [("Category", GV.Str $ catText $ F.rcast r)
--          , ("Ppl", GV.Number $ realToFrac  $ r ^. DT.popCount)
          , ("Lo", GV.Number $ MT.ciLower $ r ^. modelCI)
          , ("Mid", GV.Number $ MT.ciMid $ r ^. modelCI)
          , ("Hi", GV.Number $ MT.ciUpper $ r ^. modelCI)
          , ("Source", GV.Str t)
          ]
      colDataSurvey (t, r) = [("Category", GV.Str $ catText $ F.rcast r)
                             --                        , ("Ppl", GV.Number $ realToFrac  $ r ^. DT.popCount)
                             , ("Lo", GV.Number $ r ^. modelPr)
                             , ("Mid", GV.Number $ r ^. modelPr)
                             , ("Hi", GV.Number $ r ^. modelPr)
                             , ("Source", GV.Str t)
                             ]

--      toData kltr = fmap ($ kltr) $ fmap colData [0..(n-1)]
      modelJsonRows = FL.fold (VJ.rowsToJSON colDataModel [] Nothing) $ concat $ fmap (\(s, fr) -> fmap (s,) $ FL.fold FL.list fr) tableRowsByModel
  jsonRows <- case surveyRowsM of
    Nothing -> pure modelJsonRows
    Just surveyRows -> do
      let surveyJsonRows = FL.fold (VJ.rowsToJSON colDataSurvey [] Nothing) $ concat $ fmap (\(s, fr) -> fmap (s,) $ FL.fold FL.list fr) surveyRows
      K.knitMaybe "row merge problem in categoryChart" $ VJ.mergeJSONRows Nothing modelJsonRows surveyJsonRows
--      jsonRows = surveyJsonRows <> tableJsonRows
  jsonFilePrefix <- K.getNextUnusedId $ ("cc_modelPS_" <> chartID)
  jsonUrl <- BRHJ.addJSON jsonLocations jsonFilePrefix jsonRows
  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
  --
  let xScale = GV.PScale [GV.SZero False]
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
--                                  , GV.dataFromSource "model" []
                                  ]
