{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# OPTIONS_GHC -O0 #-}

module BlueRipple.Model.Election2.DataPrep
  (
    module BlueRipple.Model.Election2.DataPrep
  )
where

import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichCensus as DEC

import qualified BlueRipple.Data.CES as CCES
import qualified BlueRipple.Data.CPS_VS as CPS
import qualified BlueRipple.Data.ACS_PUMS as ACS
import qualified BlueRipple.Data.Small.DataFrames as BR
import qualified BlueRipple.Data.Types.Demographic as DT
import qualified BlueRipple.Data.Types.Election as ET
import qualified BlueRipple.Data.Types.Geographic as GT
import qualified BlueRipple.Data.Small.Loaders as BR
import qualified BlueRipple.Data.Keyed as Keyed
import qualified BlueRipple.Data.Types.Modeling as MT

import qualified BlueRipple.Data.FramesUtils as BRF
import qualified BlueRipple.Data.CachingCore as BRCC
import qualified BlueRipple.Data.LoadersCore as BRLC
import Control.Lens (view, (^.))
import qualified Control.Foldl as FL
import qualified Control.Foldl.Statistics as FLS
import qualified Data.Csv as CSV
import qualified Data.Map.Strict as M
import qualified Data.List.NonEmpty as NE
import qualified Data.Text as T
import qualified Data.Vector as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Flat
import qualified Frames as F
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified Frames.Melt as F
import qualified Frames.Serialize as FS
import qualified Frames.Constraints as FC
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Streamly.InCore as FI
import qualified Frames.Streamly.TH as FS
import qualified Frames.Streamly.OrMissing as FS
import qualified Frames.Transform as FT
import qualified Knit.Effect.AtomicCache as K hiding (retrieveOrMake)
import qualified Knit.Report as K
import Prelude hiding (pred)

--import qualified Control.MapReduce as FMR

FS.declareColumn "SurveyWeight" ''Double

FS.declareColumn "Surveyed" ''Int
FS.declareColumn "Registered" ''Int
FS.declareColumn "Registered2p" ''Int
FS.declareColumn "Voted" ''Int

FS.declareColumn "SurveyedW" ''Double
FS.declareColumn "RegisteredW" ''Double
FS.declareColumn "Registered2pW" ''Double
FS.declareColumn "VotedW" ''Double

FS.declareColumn "VotesInRace" ''Int
FS.declareColumn "DVotes" ''Int
FS.declareColumn "RVotes" ''Int

FS.declareColumn "VotesInRaceW" ''Double
FS.declareColumn "DVotesW" ''Double
FS.declareColumn "RVotesW" ''Double

FS.declareColumn "DReg" ''Int
FS.declareColumn "RReg" ''Int
FS.declareColumn "DRegW" ''Double
FS.declareColumn "RRegW" ''Double

FS.declareColumn "TVotes" ''Int
FS.declareColumn "PresVotes" ''Int
FS.declareColumn "PresDVotes" ''Int
FS.declareColumn "PresRVotes" ''Int
FS.declareColumn "HouseVotes" ''Int
FS.declareColumn "HouseDVotes" ''Int
FS.declareColumn "HouseRVotes" ''Int

FS.declareColumn "TargetPop" ''Int -- number of people surveyed
FS.declareColumn "ActionTarget" ''Double --fraction of all surveyed people who acted (registered or voted)
FS.declareColumn "PrefDTarget" ''Double -- fraction, among people who acted,  with D preference

-- +1 for Dem incumbent, 0 for no incumbent, -1 for Rep incumbent
FS.declareColumn "Incumbency" ''Int
FS.declareColumn "HouseIncumbency" ''Int

FS.declareColumn "Frac18To24" ''Double
FS.declareColumn "Frac25To34" ''Double
FS.declareColumn "Frac35To44" ''Double
FS.declareColumn "Frac45To64" ''Double
FS.declareColumn "Frac65plus" ''Double
FS.declareColumn "Frac45AndOver" ''Double
FS.declareColumn "FracFemale" ''Double
FS.declareColumn "FracMale" ''Double
FS.declareColumn "FracNonHSGrad" ''Double
FS.declareColumn "FracHSGrad" ''Double
FS.declareColumn "FracSomeCollege" ''Double
FS.declareColumn "FracCollegeGrad" ''Double
FS.declareColumn "FracOther" ''Double
FS.declareColumn "FracBlack" ''Double
FS.declareColumn "FracHispanic" ''Double
FS.declareColumn "FracAAPI" ''Double
FS.declareColumn "FracWhite" ''Double
FS.declareColumn "FracGradOfWhite" ''Double
FS.declareColumn "RealPop" ''Double

type StateKeyR = [BR.Year, GT.StateAbbreviation]
type CDKeyR = StateKeyR V.++ '[GT.CongressionalDistrict]

type ElectionR = [Incumbency, ET.Unopposed, DVotes, RVotes, TVotes]

type CountDataR = [SurveyWeight, Surveyed, Registered,  Voted, SurveyedW, RegisteredW,  VotedW]
type DCatsR = [DT.Age5C, DT.SexC, DT.Education4C, DT.Race5C]
type LPredictorsR = '[DT.PWPopPerSqMile]
type PredictorsR = LPredictorsR V.++ DCatsR
type PrefPredictorsR = HouseIncumbency ': PredictorsR
type PrefDataR = [VotesInRace, DVotes, RVotes, VotesInRaceW, DVotesW, RVotesW, Registered2p, DReg, RReg, Registered2pW, DRegW, RRegW]

type CESByR k = k V.++  PrefPredictorsR V.++ CountDataR V.++ PrefDataR
type CESByCDR = CESByR CDKeyR

type ElectionDataR = [HouseIncumbency, HouseVotes, HouseDVotes, HouseRVotes, PresVotes, PresDVotes, PresRVotes]

newtype CESData = CESData { unCESData :: F.FrameRec CESByCDR } deriving stock Generic

instance Flat.Flat CESData where
  size (CESData c) n = Flat.size (FS.SFrame c) n
  encode (CESData c) = Flat.encode (FS.SFrame c)
  decode = (\c → CESData (FS.unSFrame c)) <$> Flat.decode

mapCESData :: (F.FrameRec CESByCDR -> F.FrameRec CESByCDR) -> CESData -> CESData
mapCESData f = CESData . f . unCESData

type CPSByStateR = StateKeyR V.++ PredictorsR V.++ CountDataR

newtype CPSData = CPSData (F.FrameRec CPSByStateR) deriving stock Generic

instance Flat.Flat CPSData where
  size (CPSData c) n = Flat.size (FS.SFrame c) n
  encode (CPSData c) = Flat.encode (FS.SFrame c)
  decode = (\c → CPSData (FS.unSFrame c)) <$> Flat.decode

type PSDataR k = k V.++ PredictorsR V.++ '[DT.PopCount]

newtype PSData k = PSData { unPSData :: F.FrameRec (PSDataR k) }

instance (FS.RecFlat (PSDataR k)
         , V.RMap (PSDataR k)
         , FI.RecVec (PSDataR k)
         ) => Flat.Flat (PSData k) where
  size (PSData c) = Flat.size (FS.SFrame c)
  encode (PSData c) = Flat.encode (FS.SFrame c)
  decode = (\c -> PSData (FS.unSFrame c)) <$> Flat.decode

acsByStatePS :: forall r . (K.KnitEffects r, BRCC.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (PSData '[BR.Year, GT.StateAbbreviation]))
acsByStatePS =
  let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22 @r
  in fmap (PSData . fmap F.rcast) <$> DDP.cachedACSa5ByState srcWindow cachedSrc 2022

data ModelData lk =
  ModelData
  {
    cpsData :: F.FrameRec CPSByStateR
  , cesData :: F.FrameRec (CESByR lk)
  }

cesDataVotePref :: (FI.RecVec (CESByR lk), F.ElemOf (CESByR lk) VotesInRace) => ModelData lk -> F.FrameRec (CESByR lk)
cesDataVotePref = F.filterFrame ((> 0) . view votesInRace) . cesData

cesDataRegPref :: (FI.RecVec (CESByR lk), F.ElemOf (CESByR lk) Registered2p) => ModelData lk -> F.FrameRec (CESByR lk)
cesDataRegPref = F.filterFrame ((> 0) . view registered2p) . cesData


instance (FS.RecFlat (CESByR lk), V.RMap (CESByR lk), FI.RecVec (CESByR lk)) => Flat.Flat (ModelData lk) where
  size (ModelData cps ces) n = Flat.size (FS.SFrame cps, FS.SFrame ces) n
  encode (ModelData cps ces)  = Flat.encode (FS.SFrame cps, FS.SFrame ces)
  decode = (\(cps', ces') -> ModelData (FS.unSFrame cps') (FS.unSFrame ces')) <$> Flat.decode

type DiagR lk = lk V.++ DCatsR V.++ [SurveyedW, VotedW, VotesInRaceW, DVotesW]

cachedPreppedModelDataCD :: (K.KnitEffects r, BRCC.CacheEffects r)
                         => Either Text Text
                         -> K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR))
                         -> Either Text Text
                         -> K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR V.++ PrefDataR))
                         -> Either Text Text
                         -> K.ActionWithCacheTime r (F.FrameRec (CDKeyR V.++ DCatsR V.++ CountDataR V.++ PrefDataR))
                         -> K.Sem r (K.ActionWithCacheTime r (ModelData CDKeyR))
cachedPreppedModelDataCD cpsCacheE cpsRaw_C _cesByStateCacheE _cesRawByState_C cesByCDCacheE cesRawByCD_C = K.wrapPrefix "cachedPreppedModelData" $ do
  cps_C <- cachedPreppedCPS cpsCacheE cpsRaw_C
--  K.ignoreCacheTime cps_C >>= pure . F.takeRows 100 >>= BRLC.logFrame
--  cesByState_C <- cachedPreppedCES2 cesByStateCacheE cesRawByState_C
  cesByCD_C <- cachedPreppedCES cesByCDCacheE cesRawByCD_C
--  K.ignoreCacheTime ces_C >>= pure . F.takeRows 1000  >>= BRLC.logFrame
  pure $ ModelData <$> cps_C <*> cesByCD_C

cachedPreppedModelDataState :: (K.KnitEffects r, BRCC.CacheEffects r)
                            => Either Text Text
                            -> K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR))
                            -> Either Text Text
                            -> K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR V.++ PrefDataR))
                            -> K.Sem r (K.ActionWithCacheTime r (ModelData StateKeyR))
cachedPreppedModelDataState cpsCacheE cpsRaw_C cesCacheE cesRaw_C = K.wrapPrefix "cachedPreppedModelData2" $ do
  cps_C <- cachedPreppedCPS cpsCacheE cpsRaw_C
--  K.ignoreCacheTime cps_C >>= pure . F.takeRows 100 >>= BRLC.logFrame
  ces_C <- cachedPreppedCES2 cesCacheE cesRaw_C
  pure $ ModelData <$> cps_C <*> ces_C

-- general

type SummaryR = [Frac18To24, Frac25To34, Frac35To44, Frac45AndOver, Frac45To64, Frac65plus
                ,FracFemale, FracMale
                ,FracNonHSGrad, FracHSGrad, FracSomeCollege, FracCollegeGrad
                ,FracOther, FracBlack, FracHispanic, FracAAPI, FracWhite
                , FracGradOfWhite
                , RealPop, DT.PWPopPerSqMile
                ]

type SummaryDataR = DT.PopCount ': DT.PWPopPerSqMile ': DEC.ASER


summarizeASER_Fld :: forall ks rs .
                     (ks F.⊆ rs, FC.ElemsOf rs SummaryDataR, Ord (F.Record ks), FI.RecVec (ks V.++ SummaryR))
                  => FL.Fold (F.Record rs) (F.FrameRec (ks V.++ SummaryR))
summarizeASER_Fld = FMR.concatFold
                $ FMR.mapReduceFold
                FMR.noUnpack
                (FMR.assignKeysAndData @ks @SummaryDataR)
                (FMR.foldAndAddKey innerFld)
  where
    innerFld :: FL.Fold (F.Record SummaryDataR) (F.Record SummaryR)
    innerFld =
      let wgtF = FL.premap (realToFrac . view DT.popCount) FL.sum
          fracWgt f = FL.prefilter f wgtF
          fracOfF f = (/) <$> fracWgt f <*> wgtF
          ageFF a = fracOfF $ (== a) . view DT.age5C
          overAnd45FF = fracOfF $ (>= DT.A5_45To64) . view DT.age5C
          sexFF s = fracOfF $ (== s) . view DT.sexC
          eduFF e = fracOfF $ (== e) . view DT.education4C
          raceFF r = fracOfF $ (== r) . view DT.race5C
          gradWhiteFF = fracOfF (\r -> r ^. DT.race5C == DT.R5_WhiteNonHispanic && r ^. DT.education4C == DT.E4_CollegeGrad)
          safeDFilter r = let d = r ^. DT.pWPopPerSqMile in d > 0 && d < 1e6
          densAndPopFld = FL.prefilter safeDFilter (DT.densityAndPopFld' DT.Geometric (const 1) (realToFrac . view DT.popCount) (view DT.pWPopPerSqMile))
      in
        (\a1 a2 a3 o a4 a5 s1 s2 e1 e2 e3 e4 r1 r2 r3 r4 r5 gw pd
          -> a1 F.&: a2 F.&: a3 F.&: o F.&: a4 F.&: a5
             F.&: s1 F.&: s2
             F.&: e1 F.&: e2 F.&: e3 F.&: e4
             F.&: r1 F.&: r2 F.&: r3 F.&: r4 F.&: r5
             F.&: gw
             F.&: pd
        )
        <$> ageFF DT.A5_18To24 <*> ageFF DT.A5_25To34 <*> ageFF DT.A5_35To44 <*> overAnd45FF
        <*> ageFF DT.A5_45To64 <*> ageFF DT.A5_65AndOver
        <*> sexFF DT.Female <*> sexFF DT.Male
        <*> eduFF DT.E4_NonHSGrad <*> eduFF DT.E4_HSGrad <*> eduFF DT.E4_SomeCollege <*> eduFF DT.E4_CollegeGrad
        <*> raceFF DT.R5_Other <*> raceFF DT.R5_Black <*> raceFF DT.R5_Hispanic <*> raceFF DT.R5_Asian <*> raceFF DT.R5_WhiteNonHispanic
        <*> gradWhiteFF
        <*> fmap (\(p, d) -> p F.&: d F.&: V.RNil) densAndPopFld


class (Real a, Fractional a, Floating a) => StdFoldable a
instance (Real a, Fractional a, Floating a) => StdFoldable a

summaryMeanStd :: FL.Fold (F.Record SummaryR) (F.Record SummaryR, F.Record SummaryR)
summaryMeanStd = (,) <$> meanFld <*> stdFld
  where
    meanFld = FF.foldAllConstrained @Fractional FL.mean
    stdFld = FF.foldAllConstrained @StdFoldable FL.std
--    lRecFld :: FL.Fold (F.Record SummaryR) (F.Record SummaryR) = fmap (\n )V.rpureConstrained @Num (const )
{-
summaryMeanStd :: FL.Fold (F.Record SummaryR) (F.Record SummaryR, F.Record SummaryR)
summaryMeanStd = (,) <$> meanFld <*> stdFld
  where
    toConst :: V.Snd x ~ Double => V.ElField x -> V.Const Double x
    toConst (V.Field x) = V.Const x
    toList :: F.Record SummaryR -> [Double]
    toList r = V.recordToList $ V.rmapf toConst r
    foldEachInList :: Int -> FL.Fold a b -> FL.Fold [a] [b]
    foldEachInList n (FL.Fold step1 begin1 extract1) = FL.Fold step begin extract
      where
        step bs as = zipWith step1 bs as
        begin = replicate n begin1
        extract = fmap extract1
    summaryFromList :: [Double] -> F.Record SummaryR
    summaryFromList [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r] =
      a F.&: b F.&: c F.&: d F.&: e F.&: f F.&: g F.&: h F.&: i F.&: j
      F.&: k F.&: l F.&: m F.&: n F.&: o F.&: p F.&: q F.&: r F.&: V.RNil
    meanFld :: FL.Fold (F.Record SummaryR) (F.Record SummaryR)
    meanFld = fmap summaryFromList $ FL.premap toList $ foldEachInList 18 FL.mean
    stdFld = fmap summaryFromList $ FL.premap toList $ foldEachInList 18 FL.std
-}

withZeros :: forall outerK ks .
             (
               (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) F.⊆ (outerK V.++ ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
             , FI.RecVec ((outerK V.++ ks) V.++ [DT.PopCount, DT.PWPopPerSqMile])
             , ks F.⊆ (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
             , FI.RecVec (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
             , Keyed.FiniteSet (F.Record ks)
             , F.ElemOf (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) DT.PWPopPerSqMile
             , F.ElemOf (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) DT.PopCount
             , Ord (F.Record outerK)
             , outerK F.⊆ (outerK V.++ ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
             , outerK V.++ (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) ~ ((outerK V.++ ks) V.++ [DT.PopCount, DT.PWPopPerSqMile])
             )
          => F.FrameRec (outerK V.++ ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
          -> F.FrameRec (outerK V.++ ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
withZeros frame = FL.fold (FMR.concatFold
                           $ FMR.mapReduceFold
                           FMR.noUnpack
                           (FMR.assignKeysAndData @outerK @(ks V.++ [DT.PopCount, DT.PWPopPerSqMile]))
                           (FMR.foldAndLabel
                            (fmap F.toFrame $ Keyed.addDefaultRec @ks zc)
                            (\k r -> fmap (k F.<+>) r)
                           )
                          )
                  frame
  where
       zc :: F.Record '[DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil

-- CPS
cpsAddDensity ::  (K.KnitEffects r)
              => F.FrameRec DDP.ACSa5ByStateR
              -> F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR)
              -> K.Sem r (F.FrameRec CPSByStateR)
cpsAddDensity acs cps = do
  K.logLE K.Info "Adding people-weighted pop density to CPS"
  let (joined, missing) = FJ.leftJoinWithMissing @(StateKeyR V.++ DCatsR) cps $ withZeros @StateKeyR @DCatsR $ fmap F.rcast acs
  when (not $ null missing) $ K.knitError $ "cpsAddDensity: Missing keys in CPS/ACS join: " <> show missing
  pure $ fmap F.rcast joined

cachedPreppedCPS :: forall r . (K.KnitEffects r, BRCC.CacheEffects r)
                 => Either Text Text
                 -> K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR))
                 -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CPSByStateR))
cachedPreppedCPS cacheE cps_C = do
  cacheKey <- case cacheE of
    Left ck -> BRCC.clearIfPresentD ck >> pure ck
    Right ck -> pure ck
  let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22 @r
  acs_C <- DDP.cachedACSa5ByState srcWindow cachedSrc 2020 -- so we get density from the same year as the CPS data
  let only2020 = F.filterFrame ((== 2020) . view BR.year)
  BRCC.retrieveOrMakeFrame cacheKey ((,) <$> acs_C <*> fmap only2020 cps_C) $ uncurry cpsAddDensity

cpsKeysToASER :: Bool -> F.Record '[DT.Age5C, DT.SexC, DT.EducationC, DT.InCollege, DT.RaceAlone4C, DT.HispC] -> F.Record DCatsR
cpsKeysToASER addInCollegeToGrads r =
  let  e4' = DT.educationToEducation4 $ r ^. DT.educationC
       ic r' = addInCollegeToGrads && r' ^. DT.inCollege
       e4 r' = if ic r' then DT.E4_CollegeGrad else e4'
       ra4 =  F.rgetField @DT.RaceAlone4C r
       h = F.rgetField @DT.HispC r
       ra5 =  DT.race5FromRaceAlone4AndHisp True ra4 h
  in (r ^. DT.age5C) F.&: (r ^. DT.sexC) F.&: e4 r F.&: ra5 F.&: V.RNil

designEffect :: FLS.LMVSK -> Double
designEffect lmvsk = 1 + x
  where
    x = if FLS.lmvskCount lmvsk < 2 || m2 == 0 || isNaN v then 0 else v / m2
    v = FLS.lmvskVariance lmvsk
    m2 = FLS.lmvskMean lmvsk * FLS.lmvskMean lmvsk

designEffectFld :: FL.Fold a FLS.LMVSK -> FL.Fold a Double
designEffectFld = fmap designEffect

effSampleSize :: FLS.LMVSK -> Double
effSampleSize lmvsk = n / deff where
  n = realToFrac (FLS.lmvskCount lmvsk)
  deff = designEffect lmvsk

effSampleSizeFld :: FL.Fold a FLS.LMVSK -> FL.Fold a Double
effSampleSizeFld = fmap effSampleSize

wgtdAverageFld :: (a -> Double) -> (a -> Double) -> FL.Fold a Double
wgtdAverageFld wgt f = g <$> wgtF <*> wgtdF
  where
    wgtF = FL.premap wgt FL.sum
    wgtdF = FL.premap (\a -> wgt a * f a) FL.sum
    g sumWgts sumWgtd = if (sumWgts == 0) then 0 else sumWgtd / sumWgts

wgtdAverageBoolFld :: (a -> Double) -> (a -> Bool) -> FL.Fold a Double
wgtdAverageBoolFld wgt f = g <$> wgtF <*> wgtdF
  where
    wgtF = FL.premap wgt FL.sum
    wgtdF = FL.prefilter f wgtF
    g sumWgts sumWgtd = if (sumWgts == 0) then 0 else sumWgtd / sumWgts

wgtdBoolFld :: (a -> Double) -> (a -> Bool) -> FL.Fold a Double
wgtdBoolFld wgt f = FL.prefilter f (FL.premap wgt FL.sum)

withEffSampleSize :: Double -> Int -> Int -> Double
withEffSampleSize ess n d = if d > 0 then ess * realToFrac n / realToFrac d else 0

cpsCountedTurnoutByState ∷ (K.KnitEffects r, BRCC.CacheEffects r) ⇒ K.Sem r (K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR)))
cpsCountedTurnoutByState = do
  let afterYear y r = F.rgetField @BR.Year r >= y
      possible r = CPS.cpsPossibleVoter $ F.rgetField @ET.VotedYNC r
      citizen r = F.rgetField @DT.CitizenC r == DT.Citizen
      includeRow r = afterYear 2012 r && possible r && citizen r
      vtd r = CPS.cpsVoted $ r ^. ET.votedYNC
      rgstd r = CPS.cpsRegistered $ r ^. ET.registeredYNC
      unpack r = if includeRow r then Just (cpsKeysToASER True (F.rcast r) F.<+> r) else Nothing
      innerFld :: FL.Fold (F.Record [CPS.CPSVoterPUMSWeight, ET.RegisteredYNC, ET.VotedYNC]) (F.Record CountDataR)
      innerFld =
        let surveyedFld = FL.length
            registeredFld = FL.prefilter rgstd FL.length
            votedFld = FL.prefilter vtd FL.length
            wgt = view CPS.cPSVoterPUMSWeight
            surveyWgtF = FL.premap wgt FL.sum
            lmvskFld = FL.premap wgt FLS.fastLMVSK
            essFld = effSampleSizeFld lmvskFld
        in (\aw s r v ess -> aw F.&: s F.&: r F.&: v F.&: ess
                             F.&: (ess * realToFrac r / realToFrac s) F.&: (ess * realToFrac v / realToFrac s) F.&: V.RNil)
           <$> surveyWgtF <*> surveyedFld <*> registeredFld <*> votedFld <*> essFld
      fld :: FL.Fold (F.Record CPS.CPSVoterPUMS) (F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR))
      fld = FMR.concatFold
            $ FMR.mapReduceFold
            (FMR.Unpack unpack)
            (FMR.assignKeysAndData @(StateKeyR V.++ DCatsR) @[CPS.CPSVoterPUMSWeight, ET.RegisteredYNC, ET.VotedYNC])
            (FMR.foldAndAddKey innerFld)
  cpsRaw_C ← CPS.cpsVoterPUMSLoader
  BRCC.retrieveOrMakeFrame "model/election2/cpsByStateRaw.bin" cpsRaw_C $ pure . FL.fold fld

-- CES
-- Add Density
cesAddDensity :: (K.KnitEffects r)
              => CCES.CESYear
              -> F.FrameRec DDP.ACSa5ByCDR
              -> F.FrameRec (CDKeyR V.++ DCatsR V.++ CountDataR V.++ PrefDataR)
              -> K.Sem r (F.FrameRec (CDKeyR V.++ PredictorsR V.++ CountDataR V.++ PrefDataR))
cesAddDensity cesYr acs ces = K.wrapPrefix "Election2.DataPrep" $ do
  K.logLE K.Info "Adding people-weighted pop density to CES"
  let fixSingleDistricts :: (F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs GT.CongressionalDistrict, Functor f)
                         =>   f (F.Record rs) -> f (F.Record rs)
      fixSingleDistricts = BR.fixSingleDistricts ("DC" : (BR.atLargeDistrictStates (CCES.cesYear cesYr))) 1
      (joined, missing) = FJ.leftJoinWithMissing @(CDKeyR V.++ DCatsR) (fixSingleDistricts ces)
                          $ withZeros @CDKeyR @DCatsR $ fmap F.rcast $ fixSingleDistricts acs
  when (not $ null missing) $ do
    BRLC.logFrame $ F.filterFrame ((== "DC") . view GT.stateAbbreviation) acs
    K.knitError $ "cesAddDensity: Missing keys in CES/ACS join: " <> show missing
  pure $ fmap F.rcast joined

cesAddDensity2 :: (K.KnitEffects r)
               => F.FrameRec DDP.ACSa5ByStateR
               -> F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR V.++ PrefDataR)
               -> K.Sem r (F.FrameRec (StateKeyR V.++ PredictorsR V.++ CountDataR V.++ PrefDataR))
cesAddDensity2 acs ces = K.wrapPrefix "Election2.DataPrep.cesAddDensity2" $ do
  K.logLE K.Info "Adding people-weighted pop density to CES"
  let (joined, missing) = FJ.leftJoinWithMissing @(StateKeyR V.++ DCatsR) ces
                          $ withZeros @StateKeyR @DCatsR $ fmap F.rcast acs
  when (not $ null missing) $ do
    BRLC.logFrame $ F.filterFrame ((== "DC") . view GT.stateAbbreviation) acs
    K.knitError $ "cesAddDensity: Missing keys in CES/ACS join: " <> show missing
  pure $ fmap F.rcast joined
-- add House Incumbency
cesAddHouseIncumbency :: (K.KnitEffects r)
                      => CCES.CESYear
                      -> F.FrameRec BR.HouseElectionColsI
                      -> F.FrameRec (CDKeyR V.++ PredictorsR V.++ CountDataR V.++ PrefDataR)
                      -> K.Sem r (F.FrameRec (CESByR CDKeyR))
cesAddHouseIncumbency cesYr houseElections ces = K.wrapPrefix "Election2.DataPrep" $ do
  K.logLE K.Info "Adding house incumbency to CES (+ density)"
  houseElectionsByContest <- K.knitEither $ FL.foldM (electionF @CDKeyR) $ fmap F.rcast houseElections
  let fixSingleDistricts = BR.fixSingleDistricts ("DC" : (BR.atLargeDistrictStates (CCES.cesYear cesYr))) 1
      (joined, missing) = FJ.leftJoinWithMissing @CDKeyR ces $ fixSingleDistricts houseElectionsByContest
  when (not $ null missing) $ K.knitError $ "cesAddHouseIncumbency: Missing keys in CES/Elections join: " <> show missing
  let g = FT.mutate $ \r -> FT.recordSingleton @HouseIncumbency (F.rgetField @Incumbency r)
  pure $ fmap (F.rcast . g) joined

-- just adds 0s since at the state level this doesn't really make sense. I guess we could add the average?
cesAddHouseIncumbency2 :: K.KnitEffects r
                       => F.FrameRec (StateKeyR V.++ PredictorsR V.++ CountDataR V.++ PrefDataR)
                       -> K.Sem r (F.FrameRec (CESByR StateKeyR))
cesAddHouseIncumbency2 ces = K.wrapPrefix "Election2.DataPrep" $ do
  K.logLE K.Info "Adding house incumbency to CES (+ density)"
  let g = FT.mutate $ \_ -> FT.recordSingleton @HouseIncumbency 0
  pure $ fmap (F.rcast . g) ces


cachedPreppedCES :: forall r . (K.KnitEffects r, BRCC.CacheEffects r)
                 => Either Text Text
                 -> K.ActionWithCacheTime r (F.FrameRec (CDKeyR V.++ DCatsR V.++ CountDataR V.++ PrefDataR))
                 -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CESByR CDKeyR)))
cachedPreppedCES cacheE ces_C = do
  cacheKey <- case cacheE of
    Left ck -> BRCC.clearIfPresentD ck >> pure ck
    Right ck -> pure ck
  let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22 @r
  acs_C <- fmap (F.filterFrame ((== DT.Citizen) . view DT.citizenC)) <$> DDP.cachedACSa5ByCD srcWindow cachedSrc 2020 Nothing -- so we get density from same year as survey
  houseElections_C <- fmap (F.filterFrame ((>= 2008) . view BR.year)) <$> BR.houseElectionsWithIncumbency
  let deps = (,,) <$> ces_C <*> acs_C <*> houseElections_C
  BRCC.retrieveOrMakeFrame cacheKey deps $ \(ces, acs, elex) -> do
    cesWD <- cesAddDensity CCES.CES2020 acs ces
    cesAddHouseIncumbency CCES.CES2020 elex cesWD

cachedPreppedCES2 :: forall r . (K.KnitEffects r, BRCC.CacheEffects r)
                  => Either Text Text
                  -> K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR V.++ PrefDataR))
                  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CESByR StateKeyR)))
cachedPreppedCES2 cacheE ces_C = do
  cacheKey <- case cacheE of
    Left ck -> BRCC.clearIfPresentD ck >> pure ck
    Right ck -> pure ck
  let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22 @r
  acs_C <- fmap (F.filterFrame ((== DT.Citizen) . view DT.citizenC)) <$> DDP.cachedACSa5ByState srcWindow cachedSrc 2020 -- so we get density from same year as survey
--  houseElections_C <- fmap (F.filterFrame ((>= 2008) . view BR.year)) <$> BR.houseElectionsWithIncumbency
  let deps = (,) <$> ces_C <*> acs_C -- <*> houseElections_C
  BRCC.retrieveOrMakeFrame cacheKey deps $ \(ces, acs) -> do
    cesWD <- cesAddDensity2 acs ces
    cesAddHouseIncumbency2 cesWD


-- an example for presidential 2020 vote.
cesCountedDemPresVotesByCD ∷ (K.KnitEffects r, BRCC.CacheEffects r)
                           ⇒ Bool
                           -> SurveyPortion
                           -> WeightingStyle
                           → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CDKeyR V.++ DCatsR V.++ CountDataR V.++ PrefDataR)))
cesCountedDemPresVotesByCD clearCaches sp weightingStyle = do
  ces2020_C ← CCES.ces20Loader
  let cacheKey = "model/election2/ces20ByCD_" <> surveyPortionText sp <> "_" <> weightingStyleText weightingStyle <> ".bin"
  when clearCaches $ BRCC.clearIfPresentD cacheKey
  BRCC.retrieveOrMakeFrame cacheKey ces2020_C $ \ces → cesMR @CDKeyR sp weightingStyle 2020 (F.rgetField @CCES.MPresVoteParty) ces


cesCountedDemPresVotesByState ∷ (K.KnitEffects r, BRCC.CacheEffects r)
                              ⇒ Bool
                              -> SurveyPortion
                              -> WeightingStyle
                              → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ DCatsR V.++ CountDataR V.++ PrefDataR)))
cesCountedDemPresVotesByState clearCaches sp weightingStyle = do
  ces2020_C ← CCES.ces20Loader
  let cacheKey = "model/election2/ces20ByState_" <> surveyPortionText sp <> "_" <>  weightingStyleText weightingStyle <> ".bin"
  when clearCaches $ BRCC.clearIfPresentD cacheKey
  BRCC.retrieveOrMakeFrame cacheKey ces2020_C $ \ces → cesMR @StateKeyR sp weightingStyle 2020 (F.rgetField @CCES.MPresVoteParty) ces


data WeightingStyle = FullWeights | DesignEffectWeights deriving (Show, Eq)

weightingStyleText :: WeightingStyle -> Text
weightingStyleText FullWeights = "fw"
weightingStyleText DesignEffectWeights = "de"

countCESVotesF :: (FC.ElemsOf rs [CCES.VRegistrationC, CCES.PartisanId3, CCES.PartisanId7, CCES.VTurnoutC
                                 , CCES.CESPreWeight, CCES.CESPostWeight, CCES.CESVVPreWeight, CCES.CESVVPostWeight])
               => SurveyPortion
               -> WeightingStyle
               -> (F.Record rs -> MT.MaybeData ET.PartyT)
               -> FL.Fold
                  (F.Record rs)
                  (F.Record (CountDataR V.++ PrefDataR))
countCESVotesF sp weightingStyle votePartyMD =
  let vote (MT.MaybeData x) = maybe False (const True) x
      safeDiv x y = if y /= 0 then x / y else 0
      voted = CCES.voted . view CCES.vTurnoutC
      vote2p (MT.MaybeData x) = maybe False (\y -> y == ET.Democratic || y == ET.Republican) x
      dVote (MT.MaybeData x) = maybe False (== ET.Democratic) x
      rVote (MT.MaybeData x) = maybe False (== ET.Republican) x
      reg' = CCES.registered . view CCES.vRegistrationC
      reg2p r = reg' r && (pidDem r || pidRep r) -- for 2-party pref of reg denominator
      reg2pD r = reg2p r && pidDem r
      reg2pR r = reg2p r && pidRep r
      wgt = case sp of
        AllSurveyed sw -> case sw of
          Pre -> view CCES.cESPreWeight
          Both -> view CCES.cESPostWeight
        Validated sw -> case sw of
          Pre -> FS.orMissing 0 id . view CCES.cESVVPreWeight
          Both -> FS.orMissing 0 id . view CCES.cESVVPostWeight
        VRegistered sw -> case sw of
          Pre -> FS.orMissing 0 id . view CCES.cESVVPreWeight
          Both -> FS.orMissing 0 id . view CCES.cESVVPostWeight
        VVoted sw -> case sw of
          Pre -> FS.orMissing 0 id . view CCES.cESVVPreWeight
          Both -> FS.orMissing 0 id . view CCES.cESVVPostWeight
      surveyedF = FL.length
      registeredF = FL.prefilter reg' FL.length
      registered2pF = FL.prefilter reg2p FL.length
      votedF = FL.prefilter voted surveyedF
      votesF = FL.prefilter (vote2p . votePartyMD) votedF
      dVotesF = FL.prefilter (dVote . votePartyMD) votedF
      rVotesF = FL.prefilter (rVote . votePartyMD) votedF
      dRegF = FL.prefilter pidDem registeredF
      rRegF = FL.prefilter pidRep registeredF
      surveyWgtF = FL.premap wgt FL.sum
      lmvskSurveyedF = FL.premap wgt FLS.fastLMVSK
      wRegF = case weightingStyle of
        FullWeights -> wgtdBoolFld wgt reg'
        DesignEffectWeights -> withEffSampleSize <$> effSampleSizeFld lmvskSurveyedF <*> registeredF <*> surveyedF
      wVotedF = case weightingStyle of
        FullWeights -> wgtdBoolFld wgt voted
        DesignEffectWeights -> withEffSampleSize <$> effSampleSizeFld lmvskSurveyedF <*> votedF <*> surveyedF
      lmvskVotesF = FL.prefilter (vote2p . votePartyMD) lmvskSurveyedF
      wdVotesF = case weightingStyle of
        FullWeights -> wgtdBoolFld wgt (dVote . votePartyMD)
        DesignEffectWeights -> withEffSampleSize <$> effSampleSizeFld lmvskVotesF <*> dVotesF <*> votesF
      wrVotesF = case weightingStyle of
        FullWeights -> wgtdBoolFld wgt (rVote . votePartyMD)
        DesignEffectWeights -> withEffSampleSize <$> effSampleSizeFld lmvskVotesF <*> rVotesF <*> votesF
      lmvskReg2pF = FL.prefilter reg2p lmvskSurveyedF
      wdRegF = case weightingStyle of
        FullWeights -> wgtdBoolFld wgt reg2pD
        DesignEffectWeights -> withEffSampleSize <$> effSampleSizeFld lmvskReg2pF <*> dRegF <*> registered2pF
      wrRegF = case weightingStyle of
        FullWeights -> wgtdBoolFld wgt reg2pR
        DesignEffectWeights -> withEffSampleSize <$> effSampleSizeFld lmvskReg2pF <*> rRegF <*> registered2pF
   in (\sw s r v eS wr wv vs dvs rvs eV wdv wrv r2p dR rR er2p wdR wrR →
          sw F.&: s F.&: r F.&: v
          F.&: eS F.&: wr  F.&: wv
          F.&: vs F.&: dvs F.&: rvs
          F.&: eV F.&: wdv F.&: wrv
          F.&: r2p F.&: dR F.&: rR F.&: er2p F.&: wdR F.&: wrR F.&: V.RNil)
      <$> surveyWgtF
      <*> surveyedF
      <*> registeredF
      <*> votedF
      <*> effSampleSizeFld lmvskSurveyedF
      <*> wRegF
      <*> wVotedF
      <*> votesF
      <*> dVotesF
      <*> rVotesF
      <*> effSampleSizeFld lmvskVotesF
      <*> wdVotesF
      <*> wrVotesF
      <*> registered2pF
      <*> dRegF
      <*> rRegF
      <*> effSampleSizeFld lmvskReg2pF
      <*> wdRegF
      <*> wrRegF


pidDem :: FC.ElemsOf rs [CCES.PartisanId3, CCES.PartisanId7] => F.Record rs -> Bool
pidDem r = case F.rgetField @CCES.PartisanId3 r of
  CCES.PI3_Democrat -> True
  CCES.PI3_Republican -> False
  _ -> case F.rgetField @CCES.PartisanId7 r of
    CCES.PI7_StrongDem -> True
    CCES.PI7_LeanDem -> True
    CCES.PI7_WeakDem -> True
    _ -> False


pidRep :: FC.ElemsOf rs [CCES.PartisanId3, CCES.PartisanId7] => F.Record rs -> Bool
pidRep r = case F.rgetField @CCES.PartisanId3 r of
  CCES.PI3_Republican -> True
  CCES.PI3_Democrat -> False
  _ -> case F.rgetField @CCES.PartisanId7 r of
    CCES.PI7_StrongRep -> True
    CCES.PI7_LeanRep -> True
    CCES.PI7_WeakRep -> True
    _ -> False


cesRecodeHispanic ∷ (F.ElemOf rs DT.HispC, F.ElemOf rs DT.Race5C) => F.Record rs -> F.Record rs
cesRecodeHispanic r =
  let h = F.rgetField @DT.HispC r
      f r5 = if h == DT.Hispanic then DT.R5_Hispanic else r5
   in FT.fieldEndo @DT.Race5C f r

cesAddEducation4 ∷ (F.ElemOf rs DT.EducationC) => F.Record rs -> F.Record (DT.Education4C ': rs)
cesAddEducation4 r =
  let e4 = DT.educationToEducation4 $ F.rgetField @DT.EducationC r
  in e4 F.&: r

data SurveyWave = Pre | Both deriving stock (Show, Eq)
surveyWaveText :: SurveyWave -> Text
surveyWaveText Pre = "Pre"
surveyWaveText Both = "Both"

data SurveyPortion = AllSurveyed SurveyWave | Validated SurveyWave | VRegistered SurveyWave | VVoted SurveyWave deriving stock (Show, Eq)

surveyPortionText :: SurveyPortion -> Text
surveyPortionText (AllSurveyed sw) = "all" <> surveyWaveText sw
surveyPortionText (Validated sw) = "vv" <> surveyWaveText sw
surveyPortionText (VRegistered sw) = "vreg" <> surveyWaveText sw
surveyPortionText (VVoted sw) = "vvote" <> surveyWaveText sw

-- using each year's common content
cesMR ∷ forall lk rs f m .
        (Foldable f, Functor f, Monad m
        , FC.ElemsOf rs [BR.Year, DT.EducationC, DT.HispC, DT.Race5C, CCES.VRegistrationC, CCES.PartisanId3, CCES.PartisanId7, CCES.VTurnoutC
                        , CCES.CESPreWeight, CCES.CESPostWeight, CCES.CESVVPreWeight, CCES.CESVVPostWeight]
--        , F.ElemOf rs CCES.CCESWeight
        , rs F.⊆ (DT.Education4C ': rs)
        , (lk V.++ DCatsR) V.++ (CountDataR V.++ PrefDataR) ~ (((lk V.++ DCatsR) V.++ CountDataR) V.++ PrefDataR)
        , Ord (F.Record (lk V.++ DCatsR))
        , (lk V.++ DCatsR) F.⊆ (DT.Education4C ': rs)
        , FI.RecVec (((lk V.++ DCatsR) V.++ CountDataR) V.++ PrefDataR)
        )
      ⇒ SurveyPortion -> WeightingStyle
      -> Int → (F.Record rs -> MT.MaybeData ET.PartyT) -> f (F.Record rs) → m (F.FrameRec (lk V.++ DCatsR V.++ CountDataR V.++ PrefDataR))
cesMR sp weightingStyle earliestYear votePartyMD =
  BRF.frameCompactMR
  unpack
  (FMR.assignKeysAndData @(lk V.++ DCatsR) @rs)
  (countCESVotesF sp weightingStyle votePartyMD)
  . fmap (cesAddEducation4 . cesRecodeHispanic)
  where
    unpack = case sp of
      AllSurveyed _ -> FMR.unpackFilterOnField @BR.Year (>= earliestYear)
      Validated _ -> FMR.unpackFilterRow (\r -> r ^. BR.year >= earliestYear && r ^. CCES.vRegistrationC /= CCES.VR_Missing)
      VRegistered _ -> FMR.unpackFilterRow (\r -> r ^. BR.year >= earliestYear && r ^. CCES.vRegistrationC == CCES.VR_Active)
      VVoted _ -> FMR.unpackFilterRow (\r -> r ^. BR.year >= earliestYear && r ^. CCES.vRegistrationC == CCES.VR_Active && r ^. CCES.vTurnoutC /= CCES.VT_Missing)

-- vote targets
data DRAOverride =
  DRAOverride
  {
    doStateAbbreviation :: Text
  , doType :: Text
  , doDemShare :: Double
  , doRepShare :: Double
  , doOtherShare :: Double
  } deriving stock (Show, Generic)

instance CSV.FromRecord DRAOverride

loadOverrides :: K.KnitEffects r => Text -> Text -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec [GT.StateAbbreviation, ET.DemShare]))
loadOverrides orFile orType = do
  let toRec :: DRAOverride -> F.Record [GT.StateAbbreviation, ET.DemShare]
      toRec x = doStateAbbreviation x F.&: doDemShare x / 100 F.&: V.RNil
      overrides fp = do
        lbs <- K.liftKnit @IO $ readFileLBS fp
        decoded <- K.knitEither $ first toText $ CSV.decodeWith CSV.defaultDecodeOptions CSV.HasHeader lbs
        pure $ F.toFrame $ fmap toRec $ filter ((== orType) . doType) $ Vec.toList decoded
  K.fileWithCacheTime orFile >>= pure . K.wctBind (overrides . toString)
--  K.fileWithCacheTime orFile >>= overrides . toString

data DShareTargetConfig r where
  ElexTargetConfig :: (FC.ElemsOf es [BR.Year, GT.StateAbbreviation, BR.Candidate, ET.Party, ET.Votes, ET.Incumbent, ET.TotalVotes], FI.RecVec es)
                   => Text
                   -> K.ActionWithCacheTime r (F.FrameRec [GT.StateAbbreviation, ET.DemShare])
                   -> Int
                   -> K.ActionWithCacheTime r (F.FrameRec es)
                   -> DShareTargetConfig r

dShareTargetText :: DShareTargetConfig r -> Text
dShareTargetText (ElexTargetConfig n _ year _) = n <> show year

dShareTarget :: (K.KnitEffects r, BRCC.CacheEffects r)
            => Either Text Text
            -> DShareTargetConfig r
            -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec [GT.StateAbbreviation, TargetPop, ET.DemShare]))
dShareTarget cacheDirE dvt@(ElexTargetConfig _ overrides_C year elex_C) = do
  cacheKey <- BRCC.cacheFromDirE cacheDirE $ dShareTargetText dvt <> ".bin"
  let overrideKey = view GT.stateAbbreviation
      overridePremap r = (overrideKey r, r ^. ET.demShare)
      safeDiv x y = if y > 0 then x / y else 0
      safeDivInt n' m = safeDiv (realToFrac n') (realToFrac m)
      deps = (,) <$> overrides_C <*> elex_C
      f (overrides, elex) = do
        flattenedElex <- K.knitEither $ FL.foldM (electionF @'[GT.StateAbbreviation]) $ fmap F.rcast $ F.filterFrame ((== year) . view BR.year) elex
        let overrideMap = FL.fold (FL.premap overridePremap FL.map) overrides
            process r = let sa = r ^. GT.stateAbbreviation
                            dv = r ^. dVotes
                            rv = r ^. rVotes
                            tv = dv + rv
                        in case M.lookup sa overrideMap of
                             Nothing -> sa F.&: tv F.&: safeDivInt dv tv F.&: V.RNil
                             Just x -> sa F.&: tv F.&: x F.&: V.RNil
        pure $ fmap process flattenedElex
  BRCC.retrieveOrMakeFrame cacheKey deps f


-- This is the thing to apply to loaded result data (with incumbents)
electionF
  ∷ ∀ ks
   . ( Ord (F.Record ks)
     , ks F.⊆ (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent])
     , F.ElemOf (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]) ET.Incumbent
     , F.ElemOf (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]) ET.Party
     , F.ElemOf (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]) ET.Votes
     , F.ElemOf (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]) BR.Candidate
     , FI.RecVec (ks V.++ ElectionR)
     )
  ⇒ FL.FoldM (Either T.Text) (F.Record (ks V.++ [BR.Candidate, ET.Party, ET.Votes, ET.Incumbent])) (F.FrameRec (ks V.++ ElectionR))
electionF =
  FMR.concatFoldM $
    FMR.mapReduceFoldM
      (FMR.generalizeUnpack FMR.noUnpack)
      (FMR.generalizeAssign $ FMR.assignKeysAndData @ks)
      (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ const $ fmap (pure @[]) flattenVotesF)

data IncParty = None | Inc (ET.PartyT, Text) | Multi (NonEmpty (ET.PartyT, Text))

updateIncParty ∷ IncParty → (ET.PartyT, Text) → IncParty
updateIncParty (Multi is) i = Multi (i NE.<| is)
updateIncParty (Inc i) i' = Multi (i :| [i'])
updateIncParty None i = Inc i

incPartyToInt ∷ IncParty → Int
incPartyToInt None = 0
incPartyToInt (Inc (ET.Democratic, _)) = 1
incPartyToInt (Inc (ET.Republican, _)) = negate 1
incPartyToInt (Inc _) = 0
incPartyToInt (Multi is) =
  let parties = fst <$> is
      sameParties = and (fmap (== head parties) (tail parties))
      party = head parties
      res party' = case party' of
        ET.Democratic -> 1
        ET.Republican -> negate 1
        _ -> 0
  in if sameParties then res party else 0

flattenVotesF ∷ FL.FoldM (Either T.Text) (F.Record [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes]) (F.Record ElectionR)
flattenVotesF = fmap (FL.fold flattenF) aggregatePartiesF
 where
  party = F.rgetField @ET.Party
  votes = F.rgetField @ET.Votes
  incumbentPartyF =
    fmap incPartyToInt
    $ FL.prefilter (F.rgetField @ET.Incumbent)
    $ FL.premap (\r → (F.rgetField @ET.Party r, F.rgetField @BR.Candidate r)) (FL.Fold updateIncParty None id)
  totalVotes = FL.premap votes FL.sum
  demVotesF = FL.prefilter (\r → party r == ET.Democratic) $ totalVotes
  repVotesF = FL.prefilter (\r → party r == ET.Republican) $ totalVotes
  unopposedF = (\x y → x == 0 || y == 0) <$> demVotesF <*> repVotesF
  flattenF = (\ii uo dv rv tv → ii F.&: uo F.&: dv F.&: rv F.&: tv F.&: V.RNil) <$> incumbentPartyF <*> unopposedF <*> demVotesF <*> repVotesF <*> totalVotes

aggregatePartiesF
  ∷ FL.FoldM
      (Either T.Text)
      (F.Record [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes])
      (F.FrameRec [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes])
aggregatePartiesF =
  let apF ∷ Text → FL.FoldM (Either T.Text) (F.Record [ET.Party, ET.Votes]) (F.Record [ET.Party, ET.Votes])
      apF c = FMR.postMapM ap (FL.generalize $ FL.premap (\r → (F.rgetField @ET.Party r, F.rgetField @ET.Votes r)) FL.map)
       where
        ap pvs =
          let demvM = M.lookup ET.Democratic pvs
              repvM = M.lookup ET.Republican pvs
              votes = FL.fold FL.sum $ M.elems pvs
              partyE = case (demvM, repvM) of
                (Nothing, Nothing) → Right ET.Other
                (Just _, Nothing) → Right ET.Democratic
                (Nothing, Just _) → Right ET.Republican
                (Just _, Just _) → Left $ c <> " has votes on both D and R lines!"
           in fmap (\p → p F.&: votes F.&: V.RNil) partyE
   in FMR.concatFoldM $
        FMR.mapReduceFoldM
          (FMR.generalizeUnpack FMR.noUnpack)
          (FMR.generalizeAssign $ FMR.assignKeysAndData @[BR.Candidate, ET.Incumbent] @[ET.Party, ET.Votes])
          (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ \r → fmap (pure @[]) (apF $ F.rgetField @BR.Candidate r))


{-
achenHurAdjDVotesViaState :: (K.KnitEffects r, BRCC.CacheEffects r)
                           => Text
                           -> K.ActionWithCacheTime r (F.FrameRec BR.PresidentialElectionCols)
                           -> K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ DCatsR V.++ '[DT.PopCount]))
                           -> K.ActionWithCacheTime r (F.FrameRec (CESByR StateKeyR))
                           -> K.ActionWithCacheTime r (F.FrameRec (CESByR CDKeyR))
                           -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CESByR CDKeyR)))
achenHurAdjDVotesViaState cacheKey turnout_C acsByState_C cesByState_C cesByCD_C = do
  let dVotesFraction pvRow = realToFrac (pvRow ^. ET.votes) / realToFrac (pvRow ^. ET.totalVotes)
      nd r = (r ^. dVotesW, r ^. votesInRaceW)
      wnd r = let (n, d) = nd r in (realToFrac $ r ^. DT.popCount, n, d)
      updateN = F.rputField @DVotesW
      deps = (,,,) <$> turnout_C <*> acsByState_C <*> cesByState_C <*> cesByCD_C
      fullCacheKey = "model/election2/" <> cacheKey <> ".bin"
  BRCC.retrieveOrMakeFrame fullCacheKey deps $ \(st, acs, cesByState, cesByCD) -> do
    K.logLE K.Info $ "achenHurAdjTurnoutViaState: cached result (" <> fullCacheKey <> ") missing or out of date. Running computation."
    let (joined, missing) = FJ.leftJoinWithMissing @(StateKeyR V.++ DCatsR) cesByState acs
    when (not $ null missing) $ K.knitError $ "achenHurStateTurnoutAdjustment: missing keys in ces/acs by state join" <> show missing
    deltaMap <- FL.foldM (BRT.wgtdSurveyDeltaFld @StateKeyR dVotesFraction wnd st) joined
    FL.foldM (BRT.adjSurveyWithDeltaMapFld @StateKeyR nd updateN deltaMap) cesByCD


achenHurAdjTurnoutViaState :: (K.KnitEffects r, BRCC.CacheEffects r)
                           => Text
                           -> K.ActionWithCacheTime r (F.FrameRec BR.StateTurnoutCols)
                           -> K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ DCatsR V.++ '[DT.PopCount]))
                           -> K.ActionWithCacheTime r (F.FrameRec (CESByR StateKeyR))
                           -> K.ActionWithCacheTime r (F.FrameRec (CESByR CDKeyR))
                           -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CESByR CDKeyR)))
achenHurAdjTurnoutViaState cacheKey turnout_C acsByState_C cesByState_C cesByCD_C = do
  let turnoutFraction tRow = tRow ^. BR.ballotsCountedVEP
      nd r = (r ^. votedW, r ^. surveyedW)
      wnd r = let (n, d) = nd r in (realToFrac $ r ^. DT.popCount, n, d)
      updateN = F.rputField @VotedW
      deps = (,,,) <$> turnout_C <*> acsByState_C <*> cesByState_C <*> cesByCD_C
      fullCacheKey = "model/election2/" <> cacheKey <> ".bin"
  BRCC.retrieveOrMakeFrame fullCacheKey deps $ \(st, acs, cesByState, cesByCD) -> do
    K.logLE K.Info $ "achenHurAdjTurnoutViaState: cached result (" <> fullCacheKey <> ") missing or out of date. Running computation."
    let (joined, missing) = FJ.leftJoinWithMissing @(StateKeyR V.++ DCatsR) cesByState acs
    when (not $ null missing) $ K.knitError $ "achenHurStateTurnoutAdjustment: missing keys in ces/acs by state join" <> show missing
    deltaMap <- FL.foldM (BRT.wgtdSurveyDeltaFld @StateKeyR turnoutFraction wnd st) joined
    FL.foldM (BRT.adjSurveyWithDeltaMapFld @StateKeyR nd updateN deltaMap) cesByCD

type AHJoined lk = CESByR lk V.++ F.RDeleteAll (lk V.++ DCatsR) (lk V.++ DCatsR V.++ '[DT.PopCount])
type AHRs lk = F.RDeleteAll [BR.Year, GT.StateAbbreviation] (CESByR lk) V.++ '[DT.PopCount]--PrefPredictorsR V.++ CountDataR V.++ PrefDataR V.++ '[DT.PopCount]

achenHurStateTurnoutAdj :: forall lk r .
                           (K.KnitEffects r, BRCC.CacheEffects r
                           , FJ.CanLeftJoinM (lk V.++ DCatsR) (CESByR lk) (lk V.++ DCatsR V.++ '[DT.PopCount])
                           , lk V.++ DCatsR F.⊆ AHJoined lk
                           , CESByR lk F.⊆ ([BR.Year, GT.StateAbbreviation] V.++ AHRs lk)
                           , AHRs lk F.⊆ ([BR.Year, GT.StateAbbreviation] V.++ AHRs lk)
                           , Show (F.Record (lk V.++ DCatsR))
                           , F.ElemOf (AHRs lk) DT.PopCount
                           , F.ElemOf (AHRs lk) VotedW
                           , F.ElemOf (AHRs lk) SurveyedW
                           , F.ElemOf (AHJoined lk) BR.Year
                           , F.ElemOf (AHJoined lk) GT.StateAbbreviation
                           , AHRs lk F.⊆ AHJoined lk
                           , FI.RecVec (AHRs lk)
                           , FS.RecFlat (CESByR lk)
                           )
                        => Text
                        -> K.ActionWithCacheTime r (F.FrameRec BR.StateTurnoutCols)
                        -> K.ActionWithCacheTime r (F.FrameRec (lk V.++ DCatsR V.++ '[DT.PopCount]))
                        -> K.ActionWithCacheTime r (F.FrameRec (CESByR lk))
                        -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CESByR lk)))
achenHurStateTurnoutAdj cacheKey turnout_C acsByCD_C cesByCD_C = do
  let turnoutFraction tRow = tRow ^. BR.ballotsCountedVEP
      wnd :: F.Record (AHRs lk) -> (Double, Double, Double)
      wnd r = (realToFrac $ r ^. DT.popCount, r ^. votedW, r ^. surveyedW)
      updateN = F.rputField @VotedW
      deps = (,,) <$> turnout_C <*> acsByCD_C <*> cesByCD_C
      fullCacheKey = "model/election2/" <> cacheKey <> ".bin"
  BRCC.retrieveOrMakeFrame fullCacheKey deps $ \(st, acs, ces) -> do
    K.logLE K.Info $ "achenHurStateTurnoutAdj: cached result (" <> fullCacheKey <> ") missing or out of date. Running computation."
    let (joined, missing) = FJ.leftJoinWithMissing @(lk V.++ DCatsR) ces acs
    when (not $ null missing) $ K.knitError $ "achenHurStateTurnoutAdjustment: missing keys in ces/acs join" <> show missing
    fmap F.rcast <$> (FL.foldM (BRT.adjWgtdSurveyFoldG @[BR.Year, GT.StateAbbreviation] turnoutFraction wnd updateN st) $ fmap F.rcast joined)


-- NB: we are updating dVotes but not rVotes
achenHurStatePresDVoteAdj :: forall lk r .
                             (K.KnitEffects r, BRCC.CacheEffects r
                             , FJ.CanLeftJoinM (lk V.++ DCatsR) (CESByR lk) (lk V.++ DCatsR V.++ '[DT.PopCount])
                             , lk V.++ DCatsR F.⊆ AHJoined lk --(CESByR lk V.++ F.RDeleteAll (lk V.++ DCatsR) (lk V.++ DCatsR V.++ '[DT.PopCount]))
                             , CESByR lk F.⊆ ([BR.Year, GT.StateAbbreviation] V.++ AHRs lk)
                             , AHRs lk F.⊆ ([BR.Year, GT.StateAbbreviation] V.++ AHRs lk)
                             , Show (F.Record (lk V.++ DCatsR))
                             , F.ElemOf (AHRs lk) DT.PopCount
                             , F.ElemOf (AHRs lk) DVotesW
                             , F.ElemOf (AHRs lk) VotesInRaceW
                             , F.ElemOf (AHJoined lk) BR.Year
                             , F.ElemOf (AHJoined lk) GT.StateAbbreviation
                             , AHRs lk F.⊆ AHJoined lk
                             , FI.RecVec (AHRs lk)
                             , FS.RecFlat (CESByR lk)
                             )
                          => Text
                          -> K.ActionWithCacheTime r (F.FrameRec BR.PresidentialElectionCols)
                          -> K.ActionWithCacheTime r (F.FrameRec (lk V.++ DCatsR V.++ '[DT.PopCount]))
                          -> K.ActionWithCacheTime r (F.FrameRec (CESByR lk))
                          -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CESByR lk)))
achenHurStatePresDVoteAdj cacheKey presVote_C acsByCD_C cesByCD_C = do
  let dVoteFraction pvRow = realToFrac (pvRow ^. ET.votes) / realToFrac (pvRow ^. ET.totalVotes)
      wnd :: F.Record (AHRs lk) -> (Double, Double, Double)
      wnd r = (realToFrac $ r ^. DT.popCount, r ^. dVotesW, r ^. votesInRaceW)
      updateN  = F.rputField @DVotesW
      deps = (,,) <$> presVote_C <*> acsByCD_C <*> cesByCD_C
      fullCacheKey = "model/election2/" <> cacheKey <> ".bin"
  BRCC.retrieveOrMakeFrame fullCacheKey deps $ \(pv, acs, ces) -> do
    K.logLE K.Info $ "achenHurStatePresDVoteAdj: cached result (" <> fullCacheKey <> ") missing or out of date. Running computation."
    let (joined, missing) = FJ.leftJoinWithMissing @(lk V.++ DCatsR) ces acs
    when (not $ null missing) $ K.knitError $ "achenHurStateTurnoutAdjustment: missing keys in ces/acs join" <> show missing
    fmap F.rcast <$> (FL.foldM (BRT.adjWgtdSurveyFoldG @[BR.Year, GT.StateAbbreviation] dVoteFraction wnd updateN pv) $ fmap F.rcast joined)
-}


{-
cesFold ∷ Int → FL.Fold (F.Record CCES.CESPR) (F.FrameRec CCESByCDR)
cesFold earliestYear =
  FMR.concatFold $
    FMR.mapReduceFold
      (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
      (FMR.assignKeysAndData @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C])
      (FMR.foldAndAddKey countCESVotesF)
-}



{-
prepCCESAndCPSEM
  ∷ (K.KnitEffects r, BRCC.CacheEffects r)
  ⇒ Bool
  → K.Sem r (K.ActionWithCacheTime r CCESAndCPSEM)
prepCCESAndCPSEM clearCache = do
  ccesAndPUMS_C ← prepCCESAndPums clearCache
  presElex_C ← prepPresidentialElectionData clearCache 2016
  senateElex_C ← prepSenateElectionData clearCache 2016
  houseElex_C ← prepHouseElectionData clearCache 2016
  --  K.logLE K.Diagnostic "Presidential Election Rows"
  --  K.ignoreCacheTime elex_C >>= BR.logFrame
  let cacheKey = "model/house/CCESAndCPSEM.bin"
      deps = (,,,) <$> ccesAndPUMS_C <*> presElex_C <*> senateElex_C <*> houseElex_C
  when clearCache $ BRCC.clearIfPresentD cacheKey
  BRCC.retrieveOrMakeD cacheKey deps $ \(ccesAndPums, pElex, sElex, hElex) → do
    let ccesEM = FL.fold fldAgeInCCES $ ccesRows ccesAndPums
        cpsVEM = FL.fold fldAgeInCPS $ cpsVRows ccesAndPums
        acsEM = FL.fold fixACSFld $ pumsRows ccesAndPums
    return $ CCESAndCPSEM ccesEM cpsVEM acsEM (pElex <> sElex) hElex

prepACS
  ∷ (K.KnitEffects r, BRCC.CacheEffects r)
  ⇒ Bool
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMSWithDensityEM))
prepACS clearCache = do
  ccesAndPUMS_C ← prepCCESAndPums clearCache
  let cacheKey = "model/house/ACS.bin"
  when clearCache $ BRCC.clearIfPresentD cacheKey
  BRCC.retrieveOrMakeFrame cacheKey ccesAndPUMS_C $ \ccesAndPums → return $ FL.fold fldAgeInACS $ pumsRows ccesAndPums

acsForYears ∷ [Int] → F.FrameRec PUMSWithDensityEM → F.FrameRec PUMSWithDensityEM
acsForYears years x =
  let f ∷ (FI.RecVec rs, F.ElemOf rs BR.Year) ⇒ F.FrameRec rs → F.FrameRec rs
      f = F.filterFrame ((`elem` years) . F.rgetField @BR.Year)
   in f x

pumsMR
  ∷ ∀ ks f
   . ( Foldable f
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ DemographicsR)
     , ks F.⊆ (ks V.++ PUMSDataR)
     , F.ElemOf (ks V.++ PUMSDataR) DT.AvgIncome
     , F.ElemOf (ks V.++ PUMSDataR) DT.PopCount
     , F.ElemOf (ks V.++ PUMSDataR) DT.CitizenC
     , F.ElemOf (ks V.++ PUMSDataR) DT.CollegeGradC
     , F.ElemOf (ks V.++ PUMSDataR) DT.HispC
     , F.ElemOf (ks V.++ PUMSDataR) DT.PWPopPerSqMile
     , F.ElemOf (ks V.++ PUMSDataR) DT.RaceAlone4C
     , F.ElemOf (ks V.++ PUMSDataR) DT.SexC
     , F.ElemOf (ks V.++ PUMSDataR) DT.SimpleAgeC
     )
  ⇒ f (F.Record (ks V.++ PUMSDataR))
  → (F.FrameRec (ks V.++ DemographicsR))
pumsMR =
  runIdentity
    . BRF.frameCompactMR
      FMR.noUnpack
      (FMR.assignKeysAndData @ks)
      pumsDataF

pumsDataF
  ∷ FL.Fold
      (F.Record PUMSDataR)
      (F.Record DemographicsR)
pumsDataF =
  let ppl = F.rgetField @DT.PopCount
      pplF = FL.premap ppl FL.sum
      intRatio x y = realToFrac x / realToFrac y
      fracF f = intRatio <$> FL.prefilter f pplF <*> pplF
      pplWgtdSumF f = FL.premap (\r → realToFrac (ppl r) * f r) FL.sum
      pplWgtdF f = (/) <$> pplWgtdSumF f <*> fmap realToFrac pplF
      race4A = F.rgetField @DT.RaceAlone4C
      hisp = F.rgetField @DT.HispC
      wnh r = race4A r == DT.RA4_White && hisp r == DT.NonHispanic
      wh r = race4A r == DT.RA4_White && hisp r == DT.Hispanic
      nwh r = race4A r /= DT.RA4_White && hisp r == DT.Hispanic
      black r = race4A r == DT.RA4_Black && hisp r == DT.NonHispanic
      asian r = race4A r == DT.RA4_Asian && hisp r == DT.NonHispanic
      other r = race4A r == DT.RA4_Other && hisp r == DT.NonHispanic
--      white r = race4A r == DT.RA4_White
      whiteNonHispanicGrad r = (wnh r) && (F.rgetField @DT.CollegeGradC r == DT.Grad)
   in FF.sequenceRecFold $
        FF.toFoldRecord (fracF ((== DT.Under) . F.rgetField @DT.SimpleAgeC))
          V.:& FF.toFoldRecord (fracF ((== DT.Female) . F.rgetField @DT.SexC))
          V.:& FF.toFoldRecord (fracF ((== DT.Grad) . F.rgetField @DT.CollegeGradC))
          V.:& FF.toFoldRecord (fracF wnh)
          V.:& FF.toFoldRecord (fracF wh)
          V.:& FF.toFoldRecord (fracF nwh)
          V.:& FF.toFoldRecord (fracF black)
          V.:& FF.toFoldRecord (fracF asian)
          V.:& FF.toFoldRecord (fracF other)
          V.:& FF.toFoldRecord (fracF whiteNonHispanicGrad)
          V.:& FF.toFoldRecord (pplWgtdF (F.rgetField @DT.AvgIncome))
          V.:& FF.toFoldRecord (pplWgtdF (F.rgetField @DT.PWPopPerSqMile))
          V.:& FF.toFoldRecord pplF
          V.:& V.RNil


type ElectionResultWithDemographicsR ks = ks V.++ '[ET.Office] V.++ ElectionR V.++ DemographicsR
type ElectionResultR ks = ks V.++ '[ET.Office] V.++ ElectionR V.++ '[DT.PopCount]

{-
addUnopposed :: (F.ElemOf rs DVotes, F.ElemOf rs RVotes) => F.Record rs -> F.Record (rs V.++ '[ET.Unopposed])
addUnopposed = FT.mutate (FT.recordSingleton @ET.Unopposed . unopposed) where
  unopposed r = F.rgetField @DVotes r == 0 || F.rgetField @RVotes r == 0
-}

makeStateElexDataFrame
  ∷ (K.KnitEffects r)
  ⇒ ET.OfficeT
  → Int
  → F.FrameRec (StateKeyR V.++ CensusPredictorR V.++ '[DT.PopCount])
  → F.FrameRec [BR.Year, GT.StateAbbreviation, BR.Candidate, ET.Party, ET.Votes, ET.Incumbent, BR.Special]
  → K.Sem r (F.FrameRec (ElectionResultR [BR.Year, GT.StateAbbreviation]))
makeStateElexDataFrame office earliestYear acsByState elex = do
  let addOffice rs = FT.recordSingleton @ET.Office office F.<+> rs
      length' = FL.fold FL.length
  let cvapFld =
        FMR.concatFold $
          FMR.mapReduceFold
            FMR.noUnpack
            (FMR.assignKeysAndData @[BR.Year, GT.StateAbbreviation] @'[DT.PopCount])
            (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
      cvapByState = FL.fold cvapFld acsByState
  flattenedElex ←
    K.knitEither $
      FL.foldM
        (electionF @[BR.Year, GT.StateAbbreviation, BR.Special])
        (fmap F.rcast $ F.filterFrame ((>= earliestYear) . F.rgetField @BR.Year) elex)
  let (elexWithCVAP, missing) = FJ.leftJoinWithMissing @[BR.Year, GT.StateAbbreviation] flattenedElex cvapByState
  when (not $ null missing) $ K.knitError $ "makeStateElexDataFrame: missing keys in elex/ACS join=" <> show missing
  when (length' elexWithCVAP /= length' flattenedElex) $ K.knitError "makeStateElexDataFrame: added rows in elex/ACS join"
  return $ fmap (F.rcast . addOffice) elexWithCVAP

addSpecial ∷ F.Record rs → F.Record (rs V.++ '[BR.Special])
addSpecial = FT.mutate (const $ FT.recordSingleton @BR.Special False)

prepPresidentialElectionData
  ∷ (K.KnitEffects r, BRCC.CacheEffects r)
  ⇒ Bool
  → Int
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (ElectionResultR '[BR.Year, GT.StateAbbreviation])))
prepPresidentialElectionData clearCache earliestYear = do
  let cacheKey = "model/house/presElexWithCVAP.bin"
  when clearCache $ BRCC.clearIfPresentD cacheKey
  presElex_C ← BR.presidentialElectionsWithIncumbency
  acs_C ← PUMS.pumsLoaderAdults
  acsByState_C ← cachedPumsByState acs_C
  let deps = (,) <$> acsByState_C <*> presElex_C
  BRCC.retrieveOrMakeFrame cacheKey deps $
    \(acsByState, pElex) → makeStateElexDataFrame ET.President earliestYear acsByState (fmap (F.rcast . addSpecial) $ pElex)

prepSenateElectionData
  ∷ (K.KnitEffects r, BRCC.CacheEffects r)
  ⇒ Bool
  → Int
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (ElectionResultR '[BR.Year, GT.StateAbbreviation])))
prepSenateElectionData clearCache earliestYear = do
  let cacheKey = "model/house/senateElexWithCVAP.bin"
  when clearCache $ BRCC.clearIfPresentD cacheKey
  senateElex_C ← BR.senateElectionsWithIncumbency
  acs_C ← PUMS.pumsLoaderAdults
  acsByState_C ← cachedPumsByState acs_C
  let deps = (,) <$> acsByState_C <*> senateElex_C
  BRCC.retrieveOrMakeFrame cacheKey deps $
    \(acsByState, senateElex) → makeStateElexDataFrame ET.Senate earliestYear acsByState (fmap F.rcast senateElex)

makeCDElexDataFrame
  ∷ (K.KnitEffects r)
  ⇒ ET.OfficeT
  → Int
  → F.FrameRec PUMSByCDR
  → F.FrameRec [BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]
  → K.Sem r (F.FrameRec (ElectionResultR [BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict]))
makeCDElexDataFrame office earliestYear acsByCD elex = do
  let addOffice rs = FT.recordSingleton @ET.Office office F.<+> rs
      lengthF = FL.fold FL.length
      fixDC_CD r =
        if (F.rgetField @GT.StateAbbreviation r == "DC")
          then FT.fieldEndo @GT.CongressionalDistrict (const 1) r
          else r
  let cvapFld =
        FMR.concatFold $
          FMR.mapReduceFold
            FMR.noUnpack
            (FMR.assignKeysAndData @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict] @'[DT.PopCount])
            (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
      cvapByCD = FL.fold cvapFld acsByCD
  flattenedElex ←
    K.knitEither $
      FL.foldM
        (electionF @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict])
        (fmap F.rcast $ F.filterFrame ((>= earliestYear) . F.rgetField @BR.Year) elex)
  let (elexWithCVAP, missing) =
        FJ.leftJoinWithMissing @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict]
          (fmap fixDC_CD flattenedElex)
          (fmap fixDC_CD cvapByCD)
  when (not $ null missing) $ K.knitError $ "makeCDElexDataFrame: missing keys in elex/ACS join=" <> show missing
  when (lengthF elexWithCVAP /= lengthF flattenedElex) $ K.knitError "makeCDElexDataFrame: added rows in elex/ACS join"
  return $ fmap (F.rcast . addOffice) elexWithCVAP

prepHouseElectionData
  ∷ (K.KnitEffects r, BRCC.CacheEffects r)
  ⇒ Bool
  → Int
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (ElectionResultR '[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict])))
prepHouseElectionData clearCache earliestYear = do
  let cacheKey = "model/house/houseElexWithCVAP.bin"
  when clearCache $ BRCC.clearIfPresentD cacheKey
  houseElex_C ← BR.houseElectionsWithIncumbency
  acs_C ← PUMS.pumsLoaderAdults
  cdByPUMA_C ← BR.allCDFromPUMA2012Loader
  acsByCD_C ← cachedPumsByCD acs_C cdByPUMA_C
  let deps = (,) <$> acsByCD_C <*> houseElex_C
  BRCC.retrieveOrMakeFrame cacheKey deps $
    \(acsByCD, houseElex) → makeCDElexDataFrame ET.House earliestYear acsByCD (fmap F.rcast houseElex)

-- TODO:  Use weights?  Design effect?
{-
countCCESVotesF :: FL.Fold (F.Record [CCES.Turnout, CCES.HouseVoteParty]) (F.Record [Surveyed, TVotes, DVotes])
countCCESVotesF =
  let surveyedF = FL.length
      votedF = FL.prefilter ((== CCES.T_Voted) . F.rgetField @CCES.Turnout) FL.length
      dVoteF = FL.prefilter ((== ET.Democratic) . F.rgetField @CCES.HouseVoteParty) votedF
  in (\s v d -> s F.&: v F.&: d F.&: V.RNil) <$> surveyedF <*> votedF <*> dVoteF

-- using cumulative
ccesMR :: (Foldable f, Monad m) => Int -> f (F.Record CCES.CCES_MRP) -> m (F.FrameRec CCESByCDR)
ccesMR earliestYear = BRF.frameCompactMRM
                     (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
                     (FMR.assignKeysAndData @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC])
                     countCCESVotesF

ccesCountedDemHouseVotesByCD :: (K.KnitEffects r, BRCC.CacheEffects r) => Bool -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CCESByCDR))
ccesCountedDemHouseVotesByCD clearCaches = do
  cces_C <- CCES.ccesDataLoader
  let cacheKey = "model/house/ccesByCD.bin"
  when clearCaches $  BRCC.clearIfPresentD cacheKey
  BRCC.retrieveOrMakeFrame cacheKey cces_C $ \cces -> do
--    BR.logFrame cces
    ccesMR 2012 cces
-}


--  BRCC.retrieveOrMakeFrame cacheKey ces2020_C $ return . FL.fold (cesFold 2020)

-- NB: StateKey includes year
cpsCountedTurnoutByState ∷ (K.KnitEffects r, BRCC.CacheEffects r) ⇒ K.Sem r (K.ActionWithCacheTime r (F.FrameRec CPSVByStateR))
cpsCountedTurnoutByState = do
  let afterYear y r = F.rgetField @BR.Year r >= y
      possible r = CPS.cpsPossibleVoter $ F.rgetField @ET.VotedYNC r
      citizen r = F.rgetField @DT.CitizenC r == DT.Citizen
      includeRow r = afterYear 2012 r && possible r && citizen r
      votedF r = CPS.cpsVoted $ F.rgetField @ET.VotedYNC r
      wgt r = F.rgetField @CPS.CPSVoterPUMSWeight r
      fld =
        BRCF.weightedCountFold @_ @CPS.CPSVoterPUMS
          (\r → F.rcast @StateKeyR r `V.rappend` CPS.cpsKeysToCASER4H True (F.rcast r))
          (F.rcast @[ET.VotedYNC, CPS.CPSVoterPUMSWeight])
          includeRow
          votedF
          wgt
  cpsRaw_C ← CPS.cpsVoterPUMSLoader -- NB: this is only useful for CD rollup since counties may appear in multiple CDs.
  BRCC.retrieveOrMakeFrame "model/house/cpsVByState.bin" cpsRaw_C $ return . FL.fold fld

pumsReKey
  ∷ F.Record '[DT.CitizenC, DT.Age5FC, DT.SexC, DT.EducationC, DT.InCollege, DT.RaceAlone4C, DT.HispC]
  → F.Record '[DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
pumsReKey r =
  let cg = DT.collegeGrad $ F.rgetField @DT.EducationC r
      ic = F.rgetField @DT.InCollege r
   in   F.rgetField @DT.CitizenC r
        F.&: DT.age5FToSimple (F.rgetField @DT.Age5FC r)
        F.&: F.rgetField @DT.SexC r
        F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
        F.&: F.rgetField @DT.RaceAlone4C r
        F.&: F.rgetField @DT.HispC r
        F.&: V.RNil

copy2019to2020 ∷ (FI.RecVec rs, F.ElemOf rs BR.Year) ⇒ F.FrameRec rs → F.FrameRec rs
copy2019to2020 rows = rows <> fmap changeYear2020 (F.filterFrame year2019 rows)
 where
  year2019 r = F.rgetField @BR.Year r == 2019
  changeYear2020 r = F.rputField @BR.Year 2020 r

type SenateRaceKeyR = [BR.Year, GT.StateAbbreviation, BR.Special, BR.Stage]

type ElexDataR = [ET.Office, BR.Stage, BR.Runoff, BR.Special, BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]

type HouseModelCensusTablesByCD =
  Census.CensusTables Census.LDLocationR Census.CensusDataR DT.Age5FC DT.SexC DT.CollegeGradC Census.RaceEthnicityC DT.CitizenC Census.EmploymentC

type HouseModelCensusTablesByState =
  Census.CensusTables '[BR.StateFips] Census.CensusDataR DT.Age5FC DT.SexC DT.CollegeGradC Census.RaceEthnicityC DT.CitizenC Census.EmploymentC

pumsByPUMA
  ∷ (F.Record PUMS.PUMS → Bool)
  → F.FrameRec PUMS.PUMS
  → F.FrameRec (PUMS.PUMACounts [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC])
pumsByPUMA keepIf = FL.fold (PUMS.pumsRollupF keepIf $ pumsReKey . F.rcast)

pumsByCD ∷ (K.KnitEffects r) ⇒ F.FrameRec PUMS.PUMS → F.FrameRec BR.DatedCDFromPUMA2012 → K.Sem r (F.FrameRec PUMSByCDR)
pumsByCD pums cdFromPUMA = fmap F.rcast <$> PUMS.pumsCDRollup (earliest earliestYear) (pumsReKey . F.rcast) cdFromPUMA pums
 where
  earliestYear = 2016
  earliest year = (>= year) . F.rgetField @BR.Year

cachedPumsByCD
  ∷ ∀ r
   . (K.KnitEffects r, BRCC.CacheEffects r)
  ⇒ K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
  → K.ActionWithCacheTime r (F.FrameRec BR.DatedCDFromPUMA2012)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMSByCDR))
cachedPumsByCD pums_C cdFromPUMA_C = do
  let pumsByCDDeps = (,) <$> pums_C <*> cdFromPUMA_C
  BRCC.retrieveOrMakeFrame "model/house/pumsByCD.bin" pumsByCDDeps $
    \(pums, cdFromPUMA) → pumsByCD pums cdFromPUMA

pumsByState ∷ F.FrameRec PUMS.PUMS → F.FrameRec PUMSByStateR
pumsByState pums = F.rcast <$> FL.fold (PUMS.pumsStateRollupF (pumsReKey . F.rcast)) filteredPums
 where
  earliestYear = 2016
  earliest year = (>= year) . F.rgetField @BR.Year
  filteredPums = F.filterFrame (earliest earliestYear) pums

cachedPumsByState
  ∷ ∀ r
   . (K.KnitEffects r, BRCC.CacheEffects r)
  ⇒ K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ CensusPredictorR V.++ '[DT.PopCount])))
cachedPumsByState pums_C = do
  let zeroCount ∷ F.Record '[DT.PopCount]
      zeroCount = 0 F.&: V.RNil
      addZeroF =
        FMR.concatFold $
          FMR.mapReduceFold
            FMR.noUnpack
            (FMR.assignKeysAndData @StateKeyR @(CensusPredictorR V.++ '[DT.PopCount]))
            ( FMR.makeRecsWithKey id $
                FMR.ReduceFold $
                  const $
                    BRK.addDefaultRec @CensusPredictorR zeroCount
            )
  BRCC.retrieveOrMakeFrame "model/house/pumsByState.bin" pums_C $
    pure . FL.fold addZeroF . pumsByState

-- caFilter = F.filterFrame (\r -> F.rgetField @BR.Year r == 2020 && F.rgetField @GT.StateAbbreviation r == "CA")

prepCCESAndPums ∷ ∀ r. (K.KnitEffects r, BRCC.CacheEffects r) ⇒ Bool → K.Sem r (K.ActionWithCacheTime r CCESAndPUMS)
prepCCESAndPums clearCache = do
  let testRun = False
      cacheKey x k = k <> if x then ".tbin" else ".bin"
  let earliestYear = 2016 -- set by ces for now
      earliest year = (>= year) . F.rgetField @BR.Year
      fixDC_CD r =
        if (F.rgetField @GT.StateAbbreviation r == "DC")
          then FT.fieldEndo @GT.CongressionalDistrict (const 1) r
          else r
--      fLength = FL.fold FL.length
--      lengthInYear y = fLength . F.filterFrame ((== y) . F.rgetField @BR.Year)
  pums_C ← PUMS.pumsLoaderAdults
--  pumsByState_C ← cachedPumsByState pums_C
  countedCCES_C ← fmap (BR.fixAtLargeDistricts 0) <$> cesCountedDemVotesByCD clearCache
  cpsVByState_C ← fmap (F.filterFrame $ earliest earliestYear) <$> cpsCountedTurnoutByState
  cdFromPUMA_C ← BR.allCDFromPUMA2012Loader
  pumsByCD_C ← cachedPumsByCD pums_C cdFromPUMA_C
  let deps = (,,) <$> countedCCES_C <*> cpsVByState_C <*> pumsByCD_C
      allCacheKey = cacheKey testRun "model/house/CCESAndPUMS"
  when clearCache $ BRCC.clearIfPresentD allCacheKey
  BRCC.retrieveOrMakeD allCacheKey deps $ \(ccesByCD, cpsVByState, acsByCD) → do
    -- get Density and avg income from PUMS and combine with election data for the district level data
    let acsCDFixed = fmap fixDC_CD acsByCD
        diInnerFold ∷ FL.Fold (F.Record [DT.PWPopPerSqMile, DT.AvgIncome, DT.PopCount]) (F.Record [DT.PopCount, DT.PWPopPerSqMile, DT.AvgIncome])
        diInnerFold =
          let ppl = F.rgetField @DT.PopCount -- Weight by voters. If voters/non-voters live in different places, we get voters experience.
          --              ppl r = cit r + F.rgetField @PUMS.NonCitizens r
              pplF = FL.premap ppl FL.sum
--              wgtdAMeanF w f = (/) <$> FL.premap (\r → w r * f r) FL.sum <*> FL.premap w FL.sum
--              wgtdGMeanF w f = fmap Numeric.exp $ (/) <$> FL.premap (\r → w r * Numeric.log (f r)) FL.sum <*> FL.premap w FL.sum
              pplWeightedAMeanF = wgtdAMeanF (realToFrac . ppl) -- (/) <$> FL.premap (\r -> realToFrac (cit r) * f r) FL.sum <*> fmap realToFrac citF
              pplWeightedGMeanF = wgtdGMeanF (realToFrac . ppl) -- (/) <$> FL.premap (\r -> realToFrac (cit r) * f r) FL.sum <*> fmap realToFrac citF
              --              pplWgtdAMeanF = wgtdAMeanF (realToFrac . ppl)
              --              pplF = FL.premap ppl FL.sum
              --              pplWeightedSumF f = (/) <$> FL.premap (\r -> realToFrac (ppl r) * f r) FL.sum <*> fmap realToFrac pplF
           in (\c d i → c F.&: d F.&: i F.&: V.RNil) <$> pplF <*> pplWeightedGMeanF (F.rgetField @DT.PWPopPerSqMile) <*> pplWeightedAMeanF (F.rgetField @DT.AvgIncome)
        diByCDFold ∷ FL.Fold (F.Record PUMSByCDR) (F.FrameRec DistrictDemDataR)
        diByCDFold =
          FMR.concatFold $
            FMR.mapReduceFold
              (FMR.filterUnpack $ \r -> F.rgetField @DT.CitizenC r == DT.Citizen)
              (FMR.assignKeysAndData @CDKeyR)
              (FMR.foldAndAddKey diInnerFold)
        diByStateFold ∷ FL.Fold (F.Record PUMSByCDR) (F.FrameRec StateDemDataR)
        diByStateFold =
          FMR.concatFold $
            FMR.mapReduceFold
              FMR.noUnpack
              (FMR.assignKeysAndData @StateKeyR)
              (FMR.foldAndAddKey diInnerFold)
        diByCD = FL.fold diByCDFold acsCDFixed
        diByState = FL.fold diByStateFold acsCDFixed
    ccesWD ← K.knitEither $ addPopDensByDistrict diByCD ccesByCD
    cpsVWD ← K.knitEither $ addPopDensByState diByState cpsVByState
    --    acsWD <- K.knitEither $ addPopDensByDistrict diByCD acsCDFixed
    return $ CCESAndPUMS (fmap F.rcast ccesWD) cpsVWD acsCDFixed diByCD -- (F.toFrame $ fmap F.rcast $ cats)

type CCESWithDensity = CCESByCDR V.++ '[DT.PWPopPerSqMile]
type CCESWithDensityEM = CCESByCDEMR V.++ '[DT.PWPopPerSqMile]

addPopDensByDistrict
  ∷ ∀ rs
   . ( FJ.CanLeftJoinM
        [BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict]
        rs
        [BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.PWPopPerSqMile]
--     , [BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict] F.⊆ [BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.PopPerSqMile]
     , F.ElemOf (rs V.++ '[DT.PWPopPerSqMile]) GT.CongressionalDistrict
     , F.ElemOf (rs V.++ '[DT.PWPopPerSqMile]) GT.StateAbbreviation
     , F.ElemOf (rs V.++ '[DT.PWPopPerSqMile]) BR.Year
     )
  ⇒ F.FrameRec DistrictDemDataR
  → F.FrameRec rs
  → Either Text (F.FrameRec (rs V.++ '[DT.PWPopPerSqMile]))
addPopDensByDistrict ddd rs = do
  let ddd' = F.rcast @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.PWPopPerSqMile] <$> ddd
      (joined, missing) =
        FJ.leftJoinWithMissing
          @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict]
          @rs
          @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.PWPopPerSqMile]
          rs
          ddd'
  when (not $ null missing) $ Left $ "missing keys in join of density data by district: " <> show missing
  Right joined

addPopDensByState
  ∷ ∀ rs
   . ( FJ.CanLeftJoinM
        [BR.Year, GT.StateAbbreviation]
        rs
        [BR.Year, GT.StateAbbreviation, DT.PWPopPerSqMile]
--     , [BR.Year, GT.StateAbbreviation] F.⊆ [BR.Year, GT.StateAbbreviation, DT.PopPerSqMile]
     , F.ElemOf (rs V.++ '[DT.PWPopPerSqMile]) GT.StateAbbreviation
     , F.ElemOf (rs V.++ '[DT.PWPopPerSqMile]) BR.Year
     )
  ⇒ F.FrameRec StateDemDataR
  → F.FrameRec rs
  → Either Text (F.FrameRec (rs V.++ '[DT.PWPopPerSqMile]))
addPopDensByState ddd rs = do
  let ddd' = F.rcast @[BR.Year, GT.StateAbbreviation, DT.PWPopPerSqMile] <$> ddd
      (joined, missing) =
        FJ.leftJoinWithMissing
          @[BR.Year, GT.StateAbbreviation]
          @rs
          @[BR.Year, GT.StateAbbreviation, DT.PWPopPerSqMile]
          rs
          ddd'
  when (not $ null missing) $ Left $ "missing keys in join of density data by state: " <> show missing
  Right joined

wgtdAMeanF ∷ (a → Double) → (a → Double) → FL.Fold a Double
wgtdAMeanF w f = (/) <$> FL.premap (\a → w a * f a) FL.sum <*> FL.premap w FL.sum

wgtdGMeanF ∷ (a → Double) → (a → Double) → FL.Fold a Double
wgtdGMeanF w f = Numeric.exp <$> wgtdAMeanF w (Numeric.log . f)

sumButLeaveDensity
  ∷ ∀ as
   . ( as F.⊆ (as V.++ '[DT.PWPopPerSqMile])
     , F.ElemOf (as V.++ '[DT.PWPopPerSqMile]) DT.PWPopPerSqMile
     , FF.ConstrainedFoldable Num as
     )
  ⇒ (F.Record (as V.++ '[DT.PWPopPerSqMile]) → Double)
  → FL.Fold (F.Record (as V.++ '[DT.PWPopPerSqMile])) (F.Record (as V.++ '[DT.PWPopPerSqMile]))
sumButLeaveDensity w =
  let sumF = FL.premap (F.rcast @as) $ FF.foldAllConstrained @Num FL.sum
      densF = fmap (FT.recordSingleton @DT.PWPopPerSqMile) $ wgtdGMeanF w (F.rgetField @DT.PWPopPerSqMile)
   in (F.<+>) <$> sumF <*> densF

fldAgeInCPS ∷ FL.Fold (F.Record CPSVWithDensity) (F.FrameRec CPSVWithDensityEM)
fldAgeInCPS =
  FMR.concatFold $
    FMR.mapReduceFold
      FMR.noUnpack
      (FMR.assignKeysAndData @(StateKeyR V.++ CensusPredictorEMR))
      (FMR.foldAndAddKey $ sumButLeaveDensity @BRCF.CountCols (realToFrac . F.rgetField @BRCF.Count))

fldAgeInACS ∷ FL.Fold (F.Record PUMSWithDensity) (F.FrameRec PUMSWithDensityEM)
fldAgeInACS =
  FMR.concatFold $
    FMR.mapReduceFold
      FMR.noUnpack
      (FMR.assignKeysAndData @(CDKeyR V.++ CensusPredictorEMR))
      (FMR.foldAndAddKey $ sumButLeaveDensity @'[DT.PopCount] (realToFrac . F.rgetField @DT.PopCount))

sumButLeaveDensityCCES ∷ FL.Fold (F.Record ((CCESVotingDataR V.++ '[DT.PWPopPerSqMile]))) (F.Record ((CCESVotingDataR V.++ '[DT.PWPopPerSqMile])))
sumButLeaveDensityCCES =
  let sumF f = FL.premap f FL.sum
      densF = wgtdGMeanF (realToFrac . F.rgetField @Surveyed) (F.rgetField @DT.PWPopPerSqMile)
   in (\f1 f2 f3 f4 f5 f6 f7 f8 f9 → f1 F.&: f2 F.&: f3 F.&: f4 F.&: f5 F.&: f6 F.&: f7 F.&: f8 F.&: f9 F.&: V.RNil)
        <$> sumF (F.rgetField @Surveyed)
        <*> sumF (F.rgetField @Voted)
        <*> sumF (F.rgetField @HouseVotes)
        <*> sumF (F.rgetField @HouseDVotes)
        <*> sumF (F.rgetField @HouseRVotes)
        <*> sumF (F.rgetField @PresVotes)
        <*> sumF (F.rgetField @PresDVotes)
        <*> sumF (F.rgetField @PresRVotes)
        <*> densF

-- NB : the polymorphic sumButLeaveDensity caused some sort of memory blowup compiling this one.
fldAgeInCCES ∷ FL.Fold (F.Record CCESWithDensity) (F.FrameRec CCESWithDensityEM)
fldAgeInCCES =
  FMR.concatFold $
    FMR.mapReduceFold
      FMR.noUnpack
      (FMR.assignKeysAndData @(CDKeyR V.++ CCESPredictorEMR))
      (FMR.foldAndAddKey sumButLeaveDensityCCES)

type PUMSWithDensity = PUMSByCDR -- V.++ '[DT.PWPopPerSqMile]
type PUMSWithDensityEM = PUMSByCDEMR V.++ '[DT.PWPopPerSqMile]
type ACSWithDensityEM = CDKeyR V.++ CCESPredictorEMR V.++ [DT.PWPopPerSqMile, DT.PopCount]

type CPSVWithDensity = CPSVByStateR V.++ '[DT.PWPopPerSqMile]
type CPSVWithDensityEM = CPSVByStateEMR V.++ '[DT.PWPopPerSqMile]

fixACSFld ∷ FL.Fold (F.Record PUMSWithDensity) (F.FrameRec ACSWithDensityEM)
fixACSFld =
  let --safeLog x = if x < 1e-12 then 0 else Numeric.log x
      density = F.rgetField @DT.PWPopPerSqMile
      ppl = F.rgetField @DT.PopCount
      pplFld = FL.premap ppl FL.sum
      pplWgtdDensityFld = wgtdGMeanF (realToFrac . ppl) density -- fmap Numeric.exp ((/) <$> FL.premap (\r -> realToFrac (cit r) * safeLog (density r)) FL.sum <*> fmap realToFrac citFld)
      dataFld ∷ FL.Fold (F.Record [DT.PWPopPerSqMile, DT.PopCount]) (F.Record [DT.PWPopPerSqMile, DT.PopCount])
      dataFld = (\d c → d F.&: c F.&: V.RNil) <$> pplWgtdDensityFld <*> pplFld
   in FMR.concatFold $
        FMR.mapReduceFold
          (FMR.simpleUnpack $ addRace5)
          (FMR.assignKeysAndData @(CDKeyR V.++ CCESPredictorEMR))
          (FMR.foldAndAddKey dataFld)

race5FromRace4AAndHisp ∷ (F.ElemOf rs DT.RaceAlone4C, F.ElemOf rs DT.HispC) ⇒ F.Record rs → DT.Race5
race5FromRace4AAndHisp r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
   in DT.race5FromRaceAlone4AndHisp True race4A hisp

addRace5
  ∷ (F.ElemOf rs DT.RaceAlone4C, F.ElemOf rs DT.HispC)
  ⇒ F.Record rs
  → F.Record (rs V.++ '[DT.Race5C])
addRace5 r = r V.<+> FT.recordSingleton @DT.Race5C (race5FromRace4AAndHisp r)

-- replaceRace

psFldCPS ∷ FL.Fold (F.Record [CVAP, Voters, DemVoters]) (F.Record [CVAP, Voters, DemVoters])
psFldCPS = FF.foldAllConstrained @Num FL.sum

cpsDiagnostics
  ∷ (K.KnitEffects r, BRCC.CacheEffects r)
  ⇒ Text
  → K.ActionWithCacheTime r (F.FrameRec CPSVByStateR)
  → K.Sem
      r
      ( K.ActionWithCacheTime
          r
          ( F.FrameRec [BR.Year, GT.StateAbbreviation, BRCF.Count, BRCF.Successes]
          , F.FrameRec [BR.Year, GT.StateAbbreviation, BRCF.Count, BRCF.Successes]
          )
      )
cpsDiagnostics _ cpsByState_C = K.wrapPrefix "cpDiagnostics" $ do
  let cpsCountsByYearAndStateFld =
        FMR.concatFold $
          FMR.mapReduceFold
            FMR.noUnpack
            (FMR.assignKeysAndData @'[BR.Year, GT.StateAbbreviation] @[BRCF.Count, BRCF.Successes])
            (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
      surveyedF = F.rgetField @BRCF.Count
      votedF = F.rgetField @BRCF.Successes
      cvap = F.rgetField @DT.PopCount
      ratio x y = realToFrac @_ @Double x / realToFrac @_ @Double y
      pT r = if surveyedF r == 0 then 0.6 else ratio (votedF r) (surveyedF r)
      compute rw rc =
        let voters = pT rw * realToFrac (cvap rc)
         in (cvap rc F.&: round voters F.&: V.RNil) ∷ F.Record [BRCF.Count, BRCF.Successes]
{-      addTurnout r =
        let cv = realToFrac (F.rgetField @CVAP r)
         in r
              F.<+> ( FT.recordSingleton @Turnout $
                        if cv < 1 then 0 else F.rgetField @Voters r / cv
                    )
-}
  let rawCK = "model/house/rawCPSByState.bin"
  rawCPS_C ← BRCC.retrieveOrMakeFrame rawCK cpsByState_C $ return . fmap F.rcast . FL.fold cpsCountsByYearAndStateFld
  acsByState_C ← PUMS.pumsLoaderAdults >>= cachedPumsByState
  let psCK = "model/house/psCPSByState.bin"
      psDeps = (,) <$> acsByState_C <*> cpsByState_C
  psCPS_C ← BRCC.retrieveOrMakeFrame psCK psDeps $ \(acsByState, cpsByState) → do
    let acsFixed = F.filterFrame (\r → F.rgetField @BR.Year r >= 2016) acsByState
        cpsFixed = F.filterFrame (\r → F.rgetField @BR.Year r >= 2016) cpsByState
        (psByState, _, rowDiff) =
          BRPS.joinAndPostStratify @'[BR.Year, GT.StateAbbreviation] @CensusPredictorR @[BRCF.Count, BRCF.Successes] @'[DT.PopCount]
            compute
            (FF.foldAllConstrained @Num FL.sum)
            (F.rcast <$> cpsFixed)
            (F.rcast <$> acsFixed)
    when (rowDiff /= 0) $ K.knitError $ "cpsDiagnostics: joinAndPostStratify join added/lost rows! (diff=)" <> show rowDiff
    pure psByState
  pure $ (,) <$> rawCPS_C <*> psCPS_C

-- CCES diagnostics
type Voters = "Voters" F.:-> Double
type DemVoters = "DemVoters" F.:-> Double
type Turnout = "Turnout" F.:-> Double

type CCESBucketR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C]

type PSVoted = "PSVoted" F.:-> Double

ccesDiagnostics
  ∷ (K.KnitEffects r, BRCC.CacheEffects r)
  ⇒ Bool
  → Text
  --                -> CCESVoteSource
  → K.ActionWithCacheTime r (F.FrameRec PUMSByCDR)
  → K.ActionWithCacheTime r (F.FrameRec CCESByCDR)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec [BR.Year, GT.StateAbbreviation, CVAP, Surveyed, Voted, PSVoted, PresVotes, PresDVotes, PresRVotes, HouseVotes, HouseDVotes, HouseRVotes]))
ccesDiagnostics clearCaches cacheSuffix acs_C cces_C = K.wrapPrefix "ccesDiagnostics" $ do
  K.logLE K.Info $ "computing CES diagnostics..."
  let surveyedF = F.rgetField @Surveyed
      votedF = F.rgetField @Voted
      ratio x y = realToFrac @_ @Double x / realToFrac @_ @Double y
      pT r = if surveyedF r == 0 then 0.6 else ratio (votedF r) (surveyedF r)
      presVotesF = F.rgetField @PresVotes
      presDVotesF = F.rgetField @PresDVotes
      presRVotesF = F.rgetField @PresRVotes
      houseVotesF = F.rgetField @HouseVotes
      houseDVotesF = F.rgetField @HouseDVotes
      houseRVotesF = F.rgetField @HouseRVotes
      pDP r = if presVotesF r == 0 then 0.5 else ratio (presDVotesF r) (presVotesF r)
      pRP r = if presVotesF r == 0 then 0.5 else ratio (presRVotesF r) (presVotesF r)
      pDH r = if houseVotesF r == 0 then 0.5 else ratio (houseDVotesF r) (houseVotesF r)
      pRH r = if houseVotesF r == 0 then 0.5 else ratio (houseRVotesF r) (houseVotesF r)
      cvap = F.rgetField @DT.PopCount
      addRace5F r = r F.<+> (FT.recordSingleton @DT.Race5C $ DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
      compute rw rc =
        let psVoted = pT rw * realToFrac (cvap rc)
            presVotesInRace = round $ realToFrac (cvap rc) * ratio (presVotesF rw) (surveyedF rw)
            houseVotesInRace = round $ realToFrac (cvap rc) * ratio (houseVotesF rw) (surveyedF rw)
            dVotesP = round $ realToFrac (cvap rc) * pDP rw
            rVotesP = round $ realToFrac (cvap rc) * pRP rw
            dVotesH = round $ realToFrac (cvap rc) * pDH rw
            rVotesH = round $ realToFrac (cvap rc) * pRH rw
         in (cvap rc F.&: surveyedF rw F.&: votedF rw F.&: psVoted F.&: presVotesInRace F.&: dVotesP F.&: rVotesP F.&: houseVotesInRace F.&: dVotesH F.&: rVotesH F.&: V.RNil) ∷ F.Record [CVAP, Surveyed, Voted, PSVoted, PresVotes, PresDVotes, PresRVotes, HouseVotes, HouseDVotes, HouseRVotes]
      deps = (,) <$> acs_C <*> cces_C
  let statesCK = "diagnostics/ccesPSByPumsStates" <> cacheSuffix <> ".bin"
  when clearCaches $ BRCC.clearIfPresentD statesCK
  BRCC.retrieveOrMakeFrame statesCK deps $ \(acs, cces) → do
    let acsFixFld =
          FMR.concatFold $
            FMR.mapReduceFold
              FMR.noUnpack
              (FMR.assignKeysAndData @([BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict] V.++ CCESBucketR) @'[DT.PopCount])
              (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
        acsFixed = FL.fold acsFixFld $ (fmap addRace5F $ F.filterFrame (\r → F.rgetField @BR.Year r >= 2016) acs)
        ccesZero ∷ F.Record [Surveyed, Voted, PresVotes, PresDVotes, PresRVotes, HouseVotes, HouseDVotes, HouseRVotes]
        ccesZero = 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: V.RNil
        addZeroFld =
          FMR.concatFold $
            FMR.mapReduceFold
              FMR.noUnpack
              (FMR.assignKeysAndData @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict])
              ( FMR.makeRecsWithKey id $
                  FMR.ReduceFold $
                    const $
                      BRK.addDefaultRec @CCESBucketR ccesZero
              )
        ccesWithZeros = FL.fold addZeroFld cces
    let (psByState, _, rowDiff) =
          BRPS.joinAndPostStratify @'[BR.Year, GT.StateAbbreviation] @(GT.CongressionalDistrict ': CCESBucketR) @[Surveyed, Voted, PresVotes, PresDVotes, PresRVotes, HouseVotes, HouseDVotes, HouseRVotes] @'[DT.PopCount]
            compute
            (FF.foldAllConstrained @Num FL.sum)
            (F.rcast <$> ccesWithZeros)
            (F.rcast <$> acsFixed) -- acs has all rows and we don't want to drop any for CVAP sum
            --    when (not $ null missing) $ K.knitError $ "ccesDiagnostics: Missing keys in cces/pums join: " <> show missing
    when (rowDiff /= 0) $ K.knitError $ "ccesDiagnostics: joinAndPostStratify join added/lost rows! (diff=)" <> show rowDiff
    pure psByState

type StateElectionR = ElectionResultR '[BR.Year, GT.StateAbbreviation]
type CDElectionR = ElectionResultR '[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict]

{-
-- many many people who identify as hispanic also identify as white. So we need to choose.
-- Better to model using both
mergeRace5AndHispanic r =
  let r5 = F.rgetField @DT.Race5C r
      h = F.rgetField @DT.HispC r
  in if (h == DT.Hispanic) then DT.R5_Hispanic else r5

--sldKey r = F.rgetField @GT.StateAbbreviation r <> "-" <> show (F.rgetField @ET.DistrictTypeC r) <> "-" <> show (F.rgetField @ET.DistrictNumber r)
sldKey :: (F.ElemOf rs GT.StateAbbreviation
          ,F.ElemOf rs ET.DistrictTypeC
          ,F.ElemOf rs ET.DistrictNumber)
       => F.Record rs -> SLDLocation
sldKey r = (F.rgetField @GT.StateAbbreviation r
           , F.rgetField @ET.DistrictTypeC r
           , F.rgetField @ET.DistrictNumber r
           )
districtKey r = F.rgetField @GT.StateAbbreviation r <> "-" <> show (F.rgetField @GT.CongressionalDistrict r)

wnh r = (F.rgetField @DT.RaceAlone4C r == DT.RA4_White) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGrad r = wnh r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
wnhCCES r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonHispanic) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGradCCES r = wnhCCES r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)

raceAlone4FromRace5 :: DT.Race5 -> DT.RaceAlone4
raceAlone4FromRace5 DT.R5_Other = DT.RA4_Other
raceAlone4FromRace5 DT.R5_Black = DT.RA4_Black
raceAlone4FromRace5 DT.R5_Hispanic = DT.RA4_Other
raceAlone4FromRace5 DT.R5_Asian = DT.RA4_Asian
raceAlone4FromRace5 DT.R5_WhiteNonHispanic = DT.RA4_White
-}
-}
