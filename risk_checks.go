// Жёсткие фильтры перед покупкой Pump («иксовые» монеты): соцсети, объём/активность, холдеры, прогресс кривой.
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// Пороги по умолчанию смягчены: свежие монеты часто без h1 в Dexscreener — смотрим m5/m15 и max(tx m5, tx h1).
// Переопределение через .env (см. комментарии в конце файла / .env.example).

func envIkemeBool(key string, def bool) bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if s == "" {
		return def
	}
	return s != "0" && s != "false" && s != "no" && s != "off"
}

func envIkemeFloat(key string, def float64) float64 {
	s := strings.TrimSpace(os.Getenv(key))
	if s == "" {
		return def
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return def
	}
	return v
}

func envIkemeInt(key string, def int) int {
	s := strings.TrimSpace(os.Getenv(key))
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

func ikemeEnabled() bool                         { return envIkemeBool("PUMP_IKEME_ENABLED", true) }
func ikemeRequireTelegram() bool                 { return envIkemeBool("PUMP_IKEME_REQUIRE_TELEGRAM", false) }
func ikemeDelayMinSec() int                      { return envIkemeInt("PUMP_IKEME_DELAY_SEC_MIN", 2) }
func ikemeDelayMaxSec() int                      { return envIkemeInt("PUMP_IKEME_DELAY_SEC_MAX", 5) }
func ikemeMinVolumeUSD() float64               { return envIkemeFloat("PUMP_IKEME_MIN_VOLUME_USD", 0) }
func ikemeMinTxEvents() int                    { return envIkemeInt("PUMP_IKEME_MIN_TX_EVENTS", 0) }
func ikemeMaxNonCurveHolderPct() float64       { return envIkemeFloat("PUMP_IKEME_MAX_HOLDER_PCT", 30) }
func ikemeMinBondingCurveProgressPct() float64 { return envIkemeFloat("PUMP_IKEME_MIN_CURVE_PROGRESS_PCT", 0) }
func ikemeSkipVelocityWhenDexEmpty() bool      { return envIkemeBool("PUMP_IKEME_SKIP_VELOCITY_WHEN_DEX_EMPTY", true) }

// pumpRiskRandomDelay — пауза перед повторной проверкой Dex (по умолчанию 2–5 с).
func pumpRiskRandomDelay() {
	minS, maxS := ikemeDelayMinSec(), ikemeDelayMaxSec()
	if minS < 0 {
		minS = 0
	}
	if maxS < minS {
		maxS = minS
	}
	n := minS
	if maxS > minS {
		n += rand.Intn(maxS - minS + 1)
	}
	time.Sleep(time.Duration(n) * time.Second)
}

// passesPumpSocialsTelegram — опционально: Telegram в Dexscreener (PUMP_IKEME_REQUIRE_TELEGRAM=false — пропуск без TG).
func passesPumpSocialsTelegram(body *dexscreenerTokenPairs, mint string) (bool, string) {
	if !ikemeRequireTelegram() {
		return true, ""
	}
	hasTG := false
	want := strings.ToLower(strings.TrimSpace(mint))
	for _, p := range body.Pairs {
		matches := strings.ToLower(strings.TrimSpace(p.BaseToken.Address)) == want ||
			strings.ToLower(strings.TrimSpace(p.QuoteToken.Address)) == want
		if !matches {
			continue
		}
		for _, s := range p.Info.Socials {
			typ := strings.ToLower(strings.TrimSpace(s.Type))
			u := strings.TrimSpace(s.URL)
			if u == "" {
				continue
			}
			if typ == "telegram" {
				hasTG = true
				break
			}
		}
		if hasTG {
			break
		}
	}
	if !hasTG {
		return false, "socials: missing non-empty telegram (Dexscreener info.socials)"
	}
	return true, ""
}

func maxVolumeWindow(m5, m15, h1, h6, h24 float64) float64 {
	m := 0.0
	for _, x := range []float64{m5, m15, h1, h6, h24} {
		if x > m {
			m = x
		}
	}
	return m
}

// dexPairVolumeAndTxActivity — лучшая пара по объёму; сделки = max(m5, h1) (у свежих монет h1 часто 0).
func dexPairVolumeAndTxActivity(body *dexscreenerTokenPairs, mint string) (volUSD float64, events int, pumpPair bool) {
	want := strings.ToLower(strings.TrimSpace(mint))
	bestVol := -1.0
	bestEv := 0
	isPump := false
	for i := range body.Pairs {
		pair := &body.Pairs[i]
		matches := strings.ToLower(strings.TrimSpace(pair.BaseToken.Address)) == want ||
			strings.ToLower(strings.TrimSpace(pair.QuoteToken.Address)) == want
		if !matches {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(pair.DexID), "pumpfun") {
			isPump = true
		}
		v := maxVolumeWindow(pair.Volume.M5, pair.Volume.M15, pair.Volume.H1, pair.Volume.H6, pair.Volume.H24)
		m5ev := pair.Txns.M5.Buys + pair.Txns.M5.Sells
		h1ev := pair.Txns.H1.Buys + pair.Txns.H1.Sells
		ev := m5ev
		if h1ev > ev {
			ev = h1ev
		}
		if v > bestVol {
			bestVol = v
			bestEv = ev
		}
	}
	if bestVol < 0 {
		return 0, 0, false
	}
	return bestVol, bestEv, isPump
}

// passesPumpVelocityAndVolumeAfterDelay — вызывать после pumpRiskRandomDelay и свежего fetchDexscreenerTokenPairs.
// У свежих монет Dex часто отдаёт volume=0 и tx=0 (лаг индексации) — при PUMP_IKEME_SKIP_VELOCITY_WHEN_DEX_EMPTY=true такой кейс пропускается.
func passesPumpVelocityAndVolumeAfterDelay(body *dexscreenerTokenPairs, mint string) (bool, string) {
	minVol := ikemeMinVolumeUSD()
	minEv := ikemeMinTxEvents()
	vol, ev, _ := dexPairVolumeAndTxActivity(body, mint)
	if ikemeSkipVelocityWhenDexEmpty() && vol <= 0 && ev <= 0 {
		log.Printf("[RISK] velocity: Dex volume/tx = 0 (часто лаг API) — пропуск порога; дальше проверки кривой/холдеров")
		return true, ""
	}
	if minVol > 0 && vol < minVol {
		return false, fmt.Sprintf("velocity: volume_usd_max_window=%.2f < %.0f", vol, minVol)
	}
	if minEv > 0 && ev < minEv {
		return false, fmt.Sprintf("velocity: tx_events_max(m5,h1)=%d < %d", ev, minEv)
	}
	return true, ""
}

// passesPumpBondingCurveProgress — прогресс кривой: (initial_real - real_token) / initial_real >= minPct.
// PUMP_IKEME_MIN_CURVE_PROGRESS_PCT <= 0 — проверка отключена (ранний вход ~0.2–0.4% иначе режется).
func passesPumpBondingCurveProgress(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (bool, string) {
	if ikemeMinBondingCurveProgressPct() <= 0 {
		return true, ""
	}
	gp, _, err := derivePumpGlobal()
	if err != nil {
		return false, fmt.Sprintf("curve progress: global PDA: %v", err)
	}
	gi, err := c.GetAccountInfoWithOpts(ctx, gp, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentProcessed})
	if err != nil || gi == nil || gi.Value == nil || gi.Value.Data == nil {
		return false, "curve progress: global account missing"
	}
	gData := gi.Value.Data.GetBinary()
	initialReal, err := parsePumpGlobalInitialRealTokenReserves(gData)
	if err != nil || initialReal == 0 {
		return false, "curve progress: cannot read initial_real_token_reserves"
	}
	bc, _, err := derivePumpBondingCurve(mint)
	if err != nil {
		return false, fmt.Sprintf("curve progress: bonding curve PDA: %v", err)
	}
	bi, err := c.GetAccountInfoWithOpts(ctx, bc, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentProcessed})
	if err != nil || bi == nil || bi.Value == nil || bi.Value.Data == nil {
		return false, "curve progress: bonding curve account missing"
	}
	_, _, realTok, _, _, _, _, err := parsePumpBondingCurveData(bi.Value.Data.GetBinary())
	if err != nil {
		return false, fmt.Sprintf("curve progress: parse curve: %v", err)
	}
	if realTok > initialReal {
		return true, ""
	}
	sold := initialReal - realTok
	pct := float64(sold) / float64(initialReal) * 100
	minPct := ikemeMinBondingCurveProgressPct()
	if pct < minPct {
		return false, fmt.Sprintf("curve progress: %.2f%% < %.2f%% (sold/initial_real)", pct, minPct)
	}
	return true, ""
}

// passesPumpTopHolderConcentration — кроме ATA bonding curve ни один из крупнейших кошельков не держит > maxPct сапплая.
func passesPumpTopHolderConcentration(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (bool, string) {
	sup, err := c.GetTokenSupply(ctx, mint, rpc.CommitmentProcessed)
	if err != nil || sup == nil || sup.Value == nil {
		return false, "holders: getTokenSupply failed"
	}
	supplyRaw, err := strconv.ParseUint(strings.TrimSpace(sup.Value.Amount), 10, 64)
	if err != nil || supplyRaw == 0 {
		return false, "holders: invalid supply"
	}
	tokProg, err := mintTokenProgram(ctx, c, mint)
	if err != nil {
		return false, fmt.Sprintf("holders: token program: %v", err)
	}
	bc, _, err := derivePumpBondingCurve(mint)
	if err != nil {
		return false, fmt.Sprintf("holders: %v", err)
	}
	assocBC, _, err := solana.FindProgramAddress(
		[][]byte{bc.Bytes(), tokProg.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return false, fmt.Sprintf("holders: assoc BC: %v", err)
	}
	largest, err := c.GetTokenLargestAccounts(ctx, mint, rpc.CommitmentProcessed)
	if err != nil || largest == nil || len(largest.Value) == 0 {
		return false, "holders: getTokenLargestAccounts empty"
	}
	maxPct := 0.0
	for _, x := range largest.Value {
		if x == nil {
			continue
		}
		if x.Address.Equals(assocBC) {
			continue
		}
		amt, err := strconv.ParseUint(strings.TrimSpace(x.Amount), 10, 64)
		if err != nil {
			continue
		}
		pct := float64(amt) / float64(supplyRaw) * 100
		if pct > maxPct {
			maxPct = pct
		}
	}
	maxH := ikemeMaxNonCurveHolderPct()
	if maxPct > maxH+1e-9 {
		return false, fmt.Sprintf("holders: largest non-curve wallet %.2f%% > %.0f%% supply", maxPct, maxH)
	}
	return true, ""
}

// passesPumpIkemeRisk — полный пайплайн после базового mint-security: соцсети → задержка → объём → кривая → холдеры.
// Отключить все проверки ikeme: PUMP_IKEME_ENABLED=false
func passesPumpIkemeRisk(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (bool, string) {
	if !ikemeEnabled() {
		return true, ""
	}
	mintStr := mint.String()
	body, err := fetchDexscreenerTokenPairs(ctx, mintStr)
	if err != nil {
		return false, "ikeme: dex fetch: " + err.Error()
	}
	if ok, why := passesPumpSocialsTelegram(&body, mintStr); !ok {
		return false, why
	}
	log.Printf("[RISK] Pump %s: socials OK, waiting %d–%d s before volume/curve checks…", mintStr, ikemeDelayMinSec(), ikemeDelayMaxSec())
	pumpRiskRandomDelay()
	body2, err := fetchDexscreenerTokenPairs(ctx, mintStr)
	if err != nil {
		return false, "ikeme: dex refetch: " + err.Error()
	}
	if ok, why := passesPumpVelocityAndVolumeAfterDelay(&body2, mintStr); !ok {
		return false, why
	}
	if ok, why := passesPumpBondingCurveProgress(ctx, c, mint); !ok {
		return false, why
	}
	if ok, why := passesPumpTopHolderConcentration(ctx, c, mint); !ok {
		return false, why
	}
	return true, ""
}
