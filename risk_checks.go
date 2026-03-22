// Жёсткие фильтры перед покупкой Pump («иксовые» монеты): соцсети, объём/активность, холдеры, прогресс кривой.
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// Настраиваемые пороги (при необходимости вынести в .env позже).
const (
	pumpRiskDelaySecsMin = 5
	pumpRiskDelaySecsMax = 10

	pumpMinVolumeUSD = 300.0
	// Dexscreener не отдаёт «уникальных трейдеров»; используем сумму сделок h1 как прокси активности.
	pumpMinTraderEventsH1 = 10

	pumpMaxNonCurveHolderPct = 10.0

	// Доля токенов, уже проданных с кривой: (initial_real - real) / initial_real (в %).
	pumpMinBondingCurveProgressPct = 4.0
)

// pumpRiskRandomDelay — 5–10 с перед повторной проверкой объёма (не снайпим в первую миллисекунду).
func pumpRiskRandomDelay() {
	n := pumpRiskDelaySecsMin + rand.Intn(pumpRiskDelaySecsMax-pumpRiskDelaySecsMin+1)
	time.Sleep(time.Duration(n) * time.Second)
}

// passesPumpSocialsTwitterTelegram — оба поля twitter и telegram должны быть заполнены в Dexscreener (как зеркало метаданных).
func passesPumpSocialsTwitterTelegram(body *dexscreenerTokenPairs, mint string) (bool, string) {
	hasTwitter, hasTG := false, false
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
			if typ == "twitter" || typ == "x" {
				hasTwitter = true
			}
			if typ == "telegram" {
				hasTG = true
			}
		}
	}
	if !hasTwitter {
		return false, "socials: missing non-empty twitter (Dexscreener info.socials)"
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

// h1TraderEvents — лучшая пара по объёму; сумма покупок и продаж за 1ч (прокси активности; уникальные кошельки в Dex API нет).
func h1TraderEvents(body *dexscreenerTokenPairs, mint string) (volUSD float64, events int, pumpPair bool) {
	want := strings.ToLower(strings.TrimSpace(mint))
	bestVol := 0.0
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
		ev := pair.Txns.H1.Buys + pair.Txns.H1.Sells
		if v > bestVol {
			bestVol = v
			bestEv = ev
		}
	}
	return bestVol, bestEv, isPump
}

// passesPumpVelocityAndVolumeAfterDelay — вызывать после pumpRiskRandomDelay и свежего fetchDexscreenerTokenPairs.
func passesPumpVelocityAndVolumeAfterDelay(body *dexscreenerTokenPairs, mint string) (bool, string) {
	vol, ev, _ := h1TraderEvents(body, mint)
	if vol < pumpMinVolumeUSD {
		return false, fmt.Sprintf("velocity: volume_usd_max_window=%.2f < %.0f", vol, pumpMinVolumeUSD)
	}
	if ev < pumpMinTraderEventsH1 {
		return false, fmt.Sprintf("velocity: h1_tx_events=%d < %d (proxy for on-chain activity)", ev, pumpMinTraderEventsH1)
	}
	return true, ""
}

// passesPumpBondingCurveProgress — прогресс кривой: (initial_real - real_token) / initial_real >= minPct.
func passesPumpBondingCurveProgress(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (bool, string) {
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
	if pct < pumpMinBondingCurveProgressPct {
		return false, fmt.Sprintf("curve progress: %.2f%% < %.1f%% (sold/initial_real)", pct, pumpMinBondingCurveProgressPct)
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
	if maxPct > pumpMaxNonCurveHolderPct+1e-9 {
		return false, fmt.Sprintf("holders: largest non-curve wallet %.2f%% > %.0f%% supply", maxPct, pumpMaxNonCurveHolderPct)
	}
	return true, ""
}

// passesPumpIkemeRisk — полный пайплайн после базового mint-security: соцсети → задержка → объём → кривая → холдеры.
func passesPumpIkemeRisk(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (bool, string) {
	mintStr := mint.String()
	body, err := fetchDexscreenerTokenPairs(ctx, mintStr)
	if err != nil {
		return false, "ikeme: dex fetch: " + err.Error()
	}
	if ok, why := passesPumpSocialsTwitterTelegram(&body, mintStr); !ok {
		return false, why
	}
	log.Printf("[RISK] Pump %s: socials OK, waiting %d–%d s before volume/curve checks…", mintStr, pumpRiskDelaySecsMin, pumpRiskDelaySecsMax)
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
