// Жёсткие фильтры перед покупкой Pump («иксовые» монеты): соцсети, объём/активность, холдеры, прогресс кривой.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
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
// Ранний вход: по умолчанию 0 с перед повторным Dex (раньше 2 с — цена уезжала на сотни %).
func ikemeDelayMinSec() int { return envIkemeInt("PUMP_IKEME_DELAY_SEC_MIN", 0) }
func ikemeDelayMaxSec() int { return envIkemeInt("PUMP_IKEME_DELAY_SEC_MAX", 0) }
func ikemeMinVolumeUSD() float64               { return envIkemeFloat("PUMP_IKEME_MIN_VOLUME_USD", 0) }
func ikemeMinTxEvents() int                    { return envIkemeInt("PUMP_IKEME_MIN_TX_EVENTS", 0) }
func ikemeMaxNonCurveHolderPct() float64       { return envIkemeFloat("PUMP_IKEME_MAX_HOLDER_PCT", 30) }
// 0 = не требовать мин. прогресс кривой (ранний вход; иначе 0.8 и т.д. режет свежие минты).
func ikemeMinBondingCurveProgressPct() float64 { return envIkemeFloat("PUMP_IKEME_MIN_CURVE_PROGRESS_PCT", 0) }
func ikemeSkipVelocityWhenDexEmpty() bool { return envIkemeBool("PUMP_IKEME_SKIP_VELOCITY_WHEN_DEX_EMPTY", true) }
// При наличии Twitter/Telegram в Dex — не требовать min volume/tx (ранний вход до «разгона» объёма).
func ikemeRelaxVelocityWhenSocialsPresent() bool { return envIkemeBool("PUMP_IKEME_RELAX_VELOCITY_WHEN_SOCIALS", true) }
// Требовать хотя бы Twitter или Telegram в Dexscreener (меньше «пустых» сливов; меньше сделок).
func ikemeRequireDexSocials() bool { return envIkemeBool("PUMP_IKEME_REQUIRE_DEX_SOCIALS", false) }
// Не пропускать монеты с volume=0 и tx=0 в Dex, если нет соцсетей (жёстче, чем SKIP_VELOCITY_WHEN_DEX_EMPTY).
func ikemeStrictEmptyDexVelocity() bool { return envIkemeBool("PUMP_IKEME_STRICT_EMPTY_DEX", false) }
// Макс. % уже проданного с кривой на момент входа; выше — считаем «уже накачали» (0 = проверка выкл.).
func ikemeMaxCurveSoldPct() float64 { return envIkemeFloat("PUMP_IKEME_MAX_CURVE_SOLD_PCT", 0) }

// Пауза перед getTokenLargestAccounts (мс). По умолчанию 0 — ранний вход; при лаге RPC поднять (напр. 500–1000).
func ikemeHoldersPreDelay() time.Duration {
	ms := envIkemeInt("PUMP_IKEME_HOLDERS_PRE_DELAY_MS", 0)
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// Пауза между попытками getTokenLargestAccounts (мс). По умолчанию 250 — быстрее, чем 1000.
func ikemeHoldersRetryPause() time.Duration {
	ms := envIkemeInt("PUMP_IKEME_HOLDERS_RETRY_PAUSE_MS", 250)
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// Если getProgramAccounts вернул «слишком много аккаунтов», пропускаем концентрацию холдеров при curve ≥ этого % (хайп / много мелких ATA).
// Дефолт 1.0 (не 5): у свежего минта кривая часто 1–3%% — при 5%% почти все сделки отсекались.
func ikemeHoldersHypePassCurvePct() float64 {
	return envIkemeFloat("PUMP_IKEME_HOLDERS_HYPE_PASS_CURVE_PCT", 1.0)
}

// Доп. нижняя планка: при Too many accounts и curve ≥ floor (но < hype) — тоже пропуск (ещё более ранние минты).
// 0 = выключить. Дефолт 0.3 — пропуск при curve ≥ 0.3%% если не прошли порог hype.
func ikemeHoldersHypeFloorCurvePct() float64 {
	v := envIkemeFloat("PUMP_IKEME_HOLDERS_HYPE_FLOOR_PCT", 0.3)
	if v < 0 {
		return 0
	}
	return v
}

// Доп. попытки getTokenLargestAccounts после первой (всего 1 + N). По умолчанию 1 (раньше 3 → до 4 RPC подряд).
func ikemeHoldersExtraRetries() int {
	n := envIkemeInt("PUMP_IKEME_HOLDERS_EXTRA_RETRIES", 1)
	if n < 0 {
		return 0
	}
	if n > 5 {
		return 5
	}
	return n
}

// pumpRiskRandomDelay — пауза перед повторной проверкой Dex (MIN–MAX из .env; 0 = без ожидания).
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

// passesPumpBondingCurveMaxSold — отсечь вход, когда кривая уже сильно сдвинута (часто фронтран / скам).
func passesPumpBondingCurveMaxSold(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (bool, string) {
	maxPct := ikemeMaxCurveSoldPct()
	if maxPct <= 0 || maxPct >= 100 {
		return true, ""
	}
	pct, err := ComputePumpBondingCurveSoldPct(ctx, c, mint)
	if err != nil {
		return false, fmt.Sprintf("max curve sold: %v", err)
	}
	if pct > maxPct {
		return false, fmt.Sprintf("curve уже %.2f%% продано > max %.2f%% (поздно / риск слива)", pct, maxPct)
	}
	return true, ""
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
// Если в Dex есть Twitter/Telegram — при PUMP_IKEME_RELAX_VELOCITY_WHEN_SOCIALS=true не ждём пороги min volume/tx (ранний вход).
func passesPumpVelocityAndVolumeAfterDelay(body *dexscreenerTokenPairs, mint string) (bool, string) {
	minVol := ikemeMinVolumeUSD()
	minEv := ikemeMinTxEvents()
	vol, ev, _ := dexPairVolumeAndTxActivity(body, mint)
	if vol <= 0 && ev <= 0 {
		log.Printf("[RISK] Skipping: No volume yet")
	}
	if ikemeStrictEmptyDexVelocity() && vol <= 0 && ev <= 0 && countTwitterTelegramLinks(body, mint) < 1 {
		return false, "strict empty dex: нет volume/tx и нет Twitter/Telegram (PUMP_IKEME_STRICT_EMPTY_DEX)"
	}
	if ikemeRelaxVelocityWhenSocialsPresent() && countTwitterTelegramLinks(body, mint) >= 1 {
		log.Printf("[RISK] velocity: есть Twitter/Telegram в Dex — пропуск порогов объёма/tx (не ждём крупный объём)")
		return true, ""
	}
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

// ComputePumpBondingCurveSoldPct — on-chain: доля проданного с кривой, % от initial_real_token_reserves (≈ прогресс bonding curve).
func ComputePumpBondingCurveSoldPct(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (float64, error) {
	gp, _, err := derivePumpGlobal()
	if err != nil {
		return 0, fmt.Errorf("global PDA: %w", err)
	}
	gi, err := c.GetAccountInfoWithOpts(ctx, gp, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentProcessed})
	if err != nil || gi == nil || gi.Value == nil || gi.Value.Data == nil {
		return 0, fmt.Errorf("global account missing")
	}
	gData := gi.Value.Data.GetBinary()
	initialReal, err := parsePumpGlobalInitialRealTokenReserves(gData)
	if err != nil || initialReal == 0 {
		return 0, fmt.Errorf("cannot read initial_real_token_reserves")
	}
	bc, _, err := derivePumpBondingCurve(mint)
	if err != nil {
		return 0, fmt.Errorf("bonding curve PDA: %w", err)
	}
	bi, err := c.GetAccountInfoWithOpts(ctx, bc, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentProcessed})
	if err != nil || bi == nil || bi.Value == nil || bi.Value.Data == nil {
		return 0, fmt.Errorf("bonding curve account missing")
	}
	_, _, realTok, _, _, _, _, err := parsePumpBondingCurveData(bi.Value.Data.GetBinary())
	if err != nil {
		return 0, fmt.Errorf("parse curve: %w", err)
	}
	if realTok > initialReal {
		return 100, nil
	}
	sold := initialReal - realTok
	return float64(sold) / float64(initialReal) * 100, nil
}

// passesPumpBondingCurveProgress — прогресс кривой: (initial_real - real_token) / initial_real >= minPct.
// По умолчанию min 0 (выкл.); задать в .env, например 0.8, чтобы резать совсем сырые кривые.
// PUMP_IKEME_MIN_CURVE_PROGRESS_PCT <= 0 — проверка отключена.
func passesPumpBondingCurveProgress(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (bool, string) {
	minPct := ikemeMinBondingCurveProgressPct()
	if minPct <= 0 {
		return true, ""
	}
	pct, err := ComputePumpBondingCurveSoldPct(ctx, c, mint)
	if err != nil {
		return false, fmt.Sprintf("curve progress: %v", err)
	}
	if pct < minPct {
		return false, fmt.Sprintf("curve progress: %.2f%% < %.2f%% (sold/initial_real)", pct, minPct)
	}
	return true, ""
}

// friendlyHolderRPCError — короткое сообщение в [SCAN] / REJECTED без простыни jsonrpc.
func friendlyHolderRPCError(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if strings.Contains(s, "not a Token mint") {
		return "getTokenLargestAccounts: not a Token mint (RPC; для Token-2022 см. fallback в логе)"
	}
	if len(s) > 200 {
		return s[:197] + "..."
	}
	return s
}

// isRPCTooManyAccountsErr — лимит RPC (Helius: getProgramAccounts слишком много ATA по одному минту).
func isRPCTooManyAccountsErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "Too many accounts") ||
		strings.Contains(s, "-32600") ||
		strings.Contains(s, "Large number of pubkeys")
}

// isRPCNotATokenMintErr — часть нод (в т.ч. Helius) отвечает -32602 «not a Token mint» на SPL Token-2022 минты (Pump.fun), хотя минт валиден.
func isRPCNotATokenMintErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "not a Token mint") || strings.Contains(s, "-32602")
}

// splTokenAccountAmountLE — базовое поле amount (u64 LE) в SPL Token / Token-2022 account (offset 64).
func splTokenAccountAmountLE(data []byte) (uint64, error) {
	if len(data) < 72 {
		return 0, fmt.Errorf("token account data len=%d", len(data))
	}
	return binary.LittleEndian.Uint64(data[64:72]), nil
}

// largestTokenAccountsViaProgramAccounts — обход для Token-2022: memcmp по mint @0, сортировка по amount, топ-20.
func largestTokenAccountsViaProgramAccounts(ctx context.Context, c *rpc.Client, mint, tokenProgram solana.PublicKey) (*rpc.GetTokenLargestAccountsResult, error) {
	filters := []rpc.RPCFilter{
		{Memcmp: &rpc.RPCFilterMemcmp{Offset: 0, Bytes: solana.Base58(mint.Bytes())}},
	}
	resp, err := c.GetProgramAccountsWithOpts(ctx, tokenProgram, &rpc.GetProgramAccountsOpts{
		Commitment: rpc.CommitmentProcessed,
		Filters:    filters,
	})
	if err != nil {
		return nil, err
	}
	type holder struct {
		addr solana.PublicKey
		amt  uint64
	}
	var holders []holder
	for _, keyed := range resp {
		if keyed == nil || keyed.Account == nil || keyed.Account.Data == nil {
			continue
		}
		raw := keyed.Account.Data.GetBinary()
		amt, err := splTokenAccountAmountLE(raw)
		if err != nil {
			continue
		}
		if amt == 0 {
			continue
		}
		holders = append(holders, holder{addr: keyed.Pubkey, amt: amt})
	}
	if len(holders) == 0 {
		return &rpc.GetTokenLargestAccountsResult{Value: nil}, nil
	}
	sort.Slice(holders, func(i, j int) bool { return holders[i].amt > holders[j].amt })
	if len(holders) > 20 {
		holders = holders[:20]
	}
	out := make([]*rpc.TokenLargestAccountsResult, 0, len(holders))
	for _, h := range holders {
		s := strconv.FormatUint(h.amt, 10)
		out = append(out, &rpc.TokenLargestAccountsResult{
			Address: h.addr,
			UiTokenAmount: rpc.UiTokenAmount{
				Amount:   s,
				Decimals: 0,
			},
		})
	}
	log.Printf("[RISK] holders: getProgramAccounts fallback — %d крупнейших ATA для минта (token program %s…)", len(out), tokenProgram.String()[:8])
	return &rpc.GetTokenLargestAccountsResult{Value: out}, nil
}

// getTokenLargestAccountsPump — getTokenLargestAccounts + ретраи; при -32602 «not a Token mint» — fallback для Token-2022 (Pump.fun).
func getTokenLargestAccountsPump(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (*rpc.GetTokenLargestAccountsResult, error) {
	extraRetries := ikemeHoldersExtraRetries()
	pause := ikemeHoldersRetryPause()
	var last *rpc.GetTokenLargestAccountsResult
	var lastErr error
	for attempt := 0; attempt <= extraRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(pause)
		}
		out, err := c.GetTokenLargestAccounts(ctx, mint, rpc.CommitmentProcessed)
		last, lastErr = out, err
		if err == nil && out != nil && len(out.Value) > 0 {
			if attempt > 0 {
				log.Printf("[RISK] holders: getTokenLargestAccounts успех после %d ретраев", attempt)
			}
			return out, nil
		}
		if isRPCNotATokenMintErr(err) {
			log.Printf("[RISK] holders: getTokenLargestAccounts — %v; переключаемся на getProgramAccounts (часто Token-2022 / Pump)", err)
			tokProg, e2 := mintTokenProgram(ctx, c, mint)
			if e2 != nil {
				return nil, fmt.Errorf("mint token program: %w", e2)
			}
			return largestTokenAccountsViaProgramAccounts(ctx, c, mint, tokProg)
		}
		if attempt < extraRetries {
			log.Printf("[RISK] holders: getTokenLargestAccounts пусто/ошибка (попытка %d/%d): %v — повтор через %v…", attempt+1, extraRetries+1, err, pause)
		}
	}
	// Пустой ответ без ошибки — иногда лаг RPC; пробуем тот же обход через program accounts
	if lastErr == nil && (last == nil || len(last.Value) == 0) {
		log.Printf("[RISK] holders: getTokenLargestAccounts пусто после ретраев — пробуем getProgramAccounts")
		tokProg, e2 := mintTokenProgram(ctx, c, mint)
		if e2 == nil {
			alt, e3 := largestTokenAccountsViaProgramAccounts(ctx, c, mint, tokProg)
			if e3 == nil && alt != nil && len(alt.Value) > 0 {
				return alt, nil
			}
			if e3 != nil {
				log.Printf("[RISK] holders: fallback getProgramAccounts: %v", e3)
				return nil, e3
			}
		}
	}
	return last, lastErr
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
	if d := ikemeHoldersPreDelay(); d > 0 {
		log.Printf("[RISK] holders: пауза %v перед getTokenLargestAccounts (индексация RPC / ATA)…", d)
		time.Sleep(d)
	}
	largest, err := getTokenLargestAccountsPump(ctx, c, mint)
	if err != nil {
		// Invalid param (-32602) и прочие ошибки — без обхода. Только «Too many accounts» (-32600): хайп, при высоком % кривой пропускаем концентрацию.
		if isRPCTooManyAccountsErr(err) {
			pct, e2 := ComputePumpBondingCurveSoldPct(ctx, c, mint)
			if e2 != nil {
				return false, fmt.Sprintf("holders: too many accounts (RPC) and curve: %v", e2)
			}
			minHype := ikemeHoldersHypePassCurvePct()
			if pct >= minHype {
				log.Printf("[RISK] holders: слишком много ATA по RPC (хайп) — пропуск проверки концентрации при curve %.2f%% ≥ %.2f%%", pct, minHype)
				return true, ""
			}
			floor := ikemeHoldersHypeFloorCurvePct()
			if floor > 0 && pct >= floor {
				log.Printf("[RISK] holders: слишком много ATA — пропуск по floor curve %.2f%% ≥ %.2f%% (ранний минт, PUMP_IKEME_HOLDERS_HYPE_FLOOR_PCT)", pct, floor)
				return true, ""
			}
			return false, fmt.Sprintf("holders: too many accounts (RPC), curve %.2f%% < floor %.2f%% / hype %.2f%%", pct, floor, minHype)
		}
		return false, "holders: " + friendlyHolderRPCError(err)
	}
	if largest == nil || len(largest.Value) == 0 {
		return false, "holders: getTokenLargestAccounts empty after retries"
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
// skipRandomDelay: не вызывать pumpRiskRandomDelay (для smart-follow «на хвосте»; у create оставлять false).
func passesPumpIkemeRisk(ctx context.Context, c *rpc.Client, mint solana.PublicKey, skipRandomDelay bool) (bool, string) {
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
	if ikemeRequireDexSocials() && countTwitterTelegramLinks(&body, mintStr) < 1 {
		return false, "ikeme: нужен Twitter или Telegram в Dex (PUMP_IKEME_REQUIRE_DEX_SOCIALS=true)"
	}
	if skipRandomDelay {
		log.Printf("[RISK] Pump %s: ikeme — пропуск PUMP_IKEME_DELAY_* (быстрый режим)", mintStr)
	} else if ikemeDelayMinSec() == 0 && ikemeDelayMaxSec() == 0 {
		log.Printf("[RISK] Pump %s: socials OK — без паузы перед volume/curve (ранний вход)", mintStr)
	} else if ikemeDelayMinSec() == ikemeDelayMaxSec() {
		log.Printf("[RISK] Pump %s: socials OK, waiting %d s before volume/curve checks…", mintStr, ikemeDelayMinSec())
	} else {
		log.Printf("[RISK] Pump %s: socials OK, waiting %d–%d s before volume/curve checks…", mintStr, ikemeDelayMinSec(), ikemeDelayMaxSec())
	}
	if !skipRandomDelay {
		pumpRiskRandomDelay()
	}
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
	if ok, why := passesPumpBondingCurveMaxSold(ctx, c, mint); !ok {
		return false, why
	}
	if ok, why := passesPumpTopHolderConcentration(ctx, c, mint); !ok {
		return false, why
	}
	return true, ""
}

// logIkemeDelayStartupWarning — если в .env задана большая пауза PUMP_IKEME_DELAY_* — поздний вход (минуты).
func logIkemeDelayStartupWarning() {
	if !ikemeEnabled() {
		return
	}
	minS, maxS := ikemeDelayMinSec(), ikemeDelayMaxSec()
	if maxS < 60 {
		return
	}
	log.Printf("[CONFIG] ВНИМАНИЕ: PUMP_IKEME_DELAY_SEC_MIN=%d MAX=%d — перед повторным Dex пауза до %d с. Для входа в первые секунды выставьте PUMP_IKEME_DELAY_SEC_MIN=0 и PUMP_IKEME_DELAY_SEC_MAX=0", minS, maxS, maxS)
}

// logIkemeQualityFiltersStartup — подсказка по «анти-слив» фильтрам.
func logIkemeQualityFiltersStartup() {
	if !ikemeEnabled() {
		return
	}
	if ikemeRequireDexSocials() || ikemeStrictEmptyDexVelocity() || ikemeMaxCurveSoldPct() > 0 {
		log.Printf("[CONFIG] Ikeme quality: REQUIRE_DEX_SOCIALS=%v STRICT_EMPTY_DEX=%v MAX_CURVE_SOLD_PCT=%.2f",
			ikemeRequireDexSocials(), ikemeStrictEmptyDexVelocity(), ikemeMaxCurveSoldPct())
		return
	}
	log.Printf("[CONFIG] Ikeme: режим «много сделок» (слабые фильтры). Меньше сливов: PUMP_IKEME_REQUIRE_DEX_SOCIALS=true, PUMP_IKEME_STRICT_EMPTY_DEX=true, PUMP_IKEME_MAX_CURVE_SOLD_PCT=10-15")
}
