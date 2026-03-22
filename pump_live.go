// Live Pump.fun: buy_exact_sol_in / sell по IDL pump-public-docs (mainnet).
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	"github.com/gagliardetto/solana-go/rpc"
)

// Отдельная программа комиссий Pump (IDL).
var pumpFeeProgramPK = solana.MustPublicKeyFromBase58("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")

// fee_config второй seed (32 байта) из IDL pump.json.
var pumpFeeConfigSeed32 = []byte{
	1, 86, 224, 246, 147, 102, 90, 207, 68, 219, 21, 104, 191, 23, 91, 170,
	81, 137, 203, 151, 245, 210, 255, 59, 101, 93, 43, 182, 253, 109, 24, 176,
}

// Дискриминатор Anchor: buy_exact_sol_in (IDL).
var pumpBuyExactSolInDisc = []byte{56, 252, 116, 8, 158, 223, 205, 95}

// Дискриминатор: sell.
var pumpSellDisc = []byte{51, 230, 133, 164, 1, 127, 131, 173}

// livePumpMu + livePumpOpenPositions — боевые позиции Pump (параллельно SIM).
var (
	livePumpMu            sync.Mutex
	livePumpOpenPositions = make(map[string]*livePumpPosition)

	// Анти-слив: лимит одновременных live BUY и минимальный интервал между ними (мелкий кошелёк / два create подряд).
	pumpLiveMaxOpenPositions int           = 1   // 0 = без лимита
	pumpLiveMinBetweenBuys   time.Duration = 90 * time.Second // 0 = выкл

	livePumpLastBuyAt time.Time
	livePumpBuyGateMu sync.Mutex
)

// initPumpLiveBuyGuardsFromEnv — PUMP_LIVE_MAX_OPEN_POSITIONS, PUMP_LIVE_MIN_SECONDS_BETWEEN_BUYS.
func initPumpLiveBuyGuardsFromEnv() {
	if s := strings.TrimSpace(os.Getenv("PUMP_LIVE_MAX_OPEN_POSITIONS")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 0 {
			pumpLiveMaxOpenPositions = v
		}
	}
	if s := strings.TrimSpace(os.Getenv("PUMP_LIVE_MIN_SECONDS_BETWEEN_BUYS")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 0 {
			pumpLiveMinBetweenBuys = time.Duration(v) * time.Second
		}
	}
	log.Printf("[PUMP LIVE] защита: max_open=%d (0=без лимита), min_between_buys=%v",
		pumpLiveMaxOpenPositions, pumpLiveMinBetweenBuys)
}

func countLivePumpOpenPositions() int {
	livePumpMu.Lock()
	defer livePumpMu.Unlock()
	n := 0
	for _, p := range livePumpOpenPositions {
		if p != nil && p.RemainingFraction > 0 {
			n++
		}
	}
	return n
}

// pumpLiveGuardAllowsNewBuy — перед live swapPumpFun: не открывать N позиций подряд и не чаще чем раз в N секунд.
func pumpLiveGuardAllowsNewBuy() (ok bool, reason string) {
	if pumpLiveMaxOpenPositions > 0 {
		n := countLivePumpOpenPositions()
		if n >= pumpLiveMaxOpenPositions {
			return false, fmt.Sprintf("уже %d/%d открытых live pump (PUMP_LIVE_MAX_OPEN_POSITIONS)", n, pumpLiveMaxOpenPositions)
		}
	}
	livePumpBuyGateMu.Lock()
	defer livePumpBuyGateMu.Unlock()
	if pumpLiveMinBetweenBuys > 0 && !livePumpLastBuyAt.IsZero() {
		elapsed := time.Since(livePumpLastBuyAt)
		if elapsed < pumpLiveMinBetweenBuys {
			wait := pumpLiveMinBetweenBuys - elapsed
			return false, fmt.Sprintf("кулдаун между BUY: ждите ещё ~%v (PUMP_LIVE_MIN_SECONDS_BETWEEN_BUYS)", wait.Round(time.Second))
		}
	}
	return true, ""
}

func recordLivePumpBuyCommitted() {
	livePumpBuyGateMu.Lock()
	livePumpLastBuyAt = time.Now()
	livePumpBuyGateMu.Unlock()
}

// livePumpPosition — зеркало simPosition для live Pump.fun (USD-цена с Jupiter/Dex).
type livePumpPosition struct {
	EntryUSD          float64
	OpenedAt          time.Time
	LastAPIPriceOKAt  time.Time
	LastKnownPriceUSD float64
	RemainingFraction float64
	DidMoonshotHalf   bool // legacy симуляции; live: не используется
	HighWaterMark     float64
	TrailingArmed     bool // после +20% PnL — трейлинг-стоп от пика
}

func derivePumpGlobal() (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("global")}, pumpFunProgram)
}

func derivePumpBondingCurve(mint solana.PublicKey) (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("bonding-curve"), mint.Bytes()}, pumpFunProgram)
}

// derivePumpBondingCurveV2 — обязательный аккаунт после апгрейда Pump (cashback / v2); последний в buy_exact_sol_in и sell.
func derivePumpBondingCurveV2(mint solana.PublicKey) (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("bonding-curve-v2"), mint.Bytes()}, pumpFunProgram)
}

func derivePumpEventAuthority() (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("__event_authority")}, pumpFunProgram)
}

func derivePumpFeeConfig() (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("fee_config"), pumpFeeConfigSeed32}, pumpFeeProgramPK)
}

func derivePumpGlobalVolumeAccumulator() (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("global_volume_accumulator")}, pumpFunProgram)
}

func derivePumpUserVolumeAccumulator(user solana.PublicKey) (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("user_volume_accumulator"), user.Bytes()}, pumpFunProgram)
}

func derivePumpCreatorVault(creator solana.PublicKey) (solana.PublicKey, uint8, error) {
	return solana.FindProgramAddress([][]byte{[]byte("creator-vault"), creator.Bytes()}, pumpFunProgram)
}

// parsePumpBondingCurveData — после 8-байтного дискриминатора BondingCurve.
// IDL pump.json: virtual_* x5, complete, creator, is_mayhem_mode, is_cashback_coin — creator сразу после complete (смещение 49).
// Важно: creator_vault PDA = seeds ["creator-vault", bonding_curve.creator], а не «dev» из create-транзакции:
// в create user и creator могут различаться (free coin creation и т.п.).
func parsePumpBondingCurveData(data []byte) (virtualToken, virtualSol, realToken, realSol, tokenTotal uint64, complete bool, creator solana.PublicKey, err error) {
	if len(data) < 8+8*5+1+32 {
		return 0, 0, 0, 0, 0, false, solana.PublicKey{}, fmt.Errorf("bonding curve data too short")
	}
	off := 8
	virtualToken = binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	virtualSol = binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	realToken = binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	realSol = binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	tokenTotal = binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	complete = data[off] != 0
	off++
	copy(creator[:], data[off:off+32])
	off += 32
	// is_mayhem_mode, is_cashback_coin (bool, bool) — при наличии байт, для расчётов не нужны
	_ = off
	return virtualToken, virtualSol, realToken, realSol, tokenTotal, complete, creator, nil
}

// parsePumpGlobalFees — fee_recipient + fee_basis_points + creator_fee_basis_points (Anchor Global layout).
func parsePumpGlobalFees(data []byte) (feeRecipient solana.PublicKey, feeBps, creatorFeeBps uint64, err error) {
	if len(data) < 170 {
		return solana.PublicKey{}, 0, 0, fmt.Errorf("global data too short")
	}
	off := 8
	off++ // initialized
	off += 32 // authority
	copy(feeRecipient[:], data[off:off+32])
	off += 32
	off += 8 * 4 // initial_virtual_token, initial_virtual_sol, initial_real_token, token_total_supply
	feeBps = binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	off += 32 // withdraw_authority
	off++       // enable_migrate
	off += 8  // pool_migration_fee
	creatorFeeBps = binary.LittleEndian.Uint64(data[off : off+8])
	return feeRecipient, feeBps, creatorFeeBps, nil
}

// parsePumpGlobalInitialRealTokenReserves — initial_real_token_reserves из Global (IDL: после fee_recipient идут 4×u64).
func parsePumpGlobalInitialRealTokenReserves(data []byte) (uint64, error) {
	if len(data) < 97 {
		return 0, fmt.Errorf("global data too short for initial_real")
	}
	// off 8: init; +32 auth; +32 fee_recipient; +8 virt tok; +8 virt sol; +8 initial_real
	return binary.LittleEndian.Uint64(data[89:97]), nil
}

// pumpQuoteExpectedTokensBuyExactSolIn — формула из IDL buy_exact_sol_in.
func pumpQuoteExpectedTokensBuyExactSolIn(spendableSolIn, vSol, vToken, protocolFeeBps, creatorFeeBps uint64) uint64 {
	totalFeeBps := protocolFeeBps + creatorFeeBps
	if totalFeeBps > 10000 {
		totalFeeBps = 10000
	}
	sp := new(big.Int).SetUint64(spendableSolIn)
	netSol := new(big.Int).Mul(sp, big.NewInt(10000))
	netSol.Div(netSol, big.NewInt(int64(10000+totalFeeBps)))

	fees1 := ceilDivBig(new(big.Int).Mul(netSol, big.NewInt(int64(protocolFeeBps))), big.NewInt(10000))
	fees2 := ceilDivBig(new(big.Int).Mul(netSol, big.NewInt(int64(creatorFeeBps))), big.NewInt(10000))
	fees := new(big.Int).Add(fees1, fees2)

	sum := new(big.Int).Add(netSol, fees)
	if sum.Cmp(sp) > 0 {
		adj := new(big.Int).Sub(sum, sp)
		netSol.Sub(netSol, adj)
	}
	if netSol.Sign() <= 0 || netSol.Cmp(big.NewInt(1)) <= 0 {
		return 0
	}
	ns := netSol.Uint64()
	if ns <= 1 {
		return 0
	}
	num := new(big.Int).SetUint64(ns - 1)
	num.Mul(num, new(big.Int).SetUint64(vToken))
	den := new(big.Int).SetUint64(vSol)
	den.Add(den, big.NewInt(int64(ns)))
	den.Sub(den, big.NewInt(1))
	if den.Sign() <= 0 {
		return 0
	}
	num.Div(num, den)
	if !num.IsUint64() {
		return 0
	}
	return num.Uint64()
}

const (
	// pumpMinOutExtraHaircutBps — доп. снижение min_out, если не используется PUMP_MIN_OUT_ONE.
	pumpMinOutExtraHaircutBps uint64 = 2000 // 20%
	// pumpSpendableBufferBps — 0: не увеличивать spendable (иначе котировка и min_out завышаются и чаще 6024 Overflow).
	pumpSpendableBufferBps uint64 = 0
)

// pumpSpendableWithBuffer — spendable_sol_in с небольшим запасом (ceil через big.Int).
func pumpSpendableWithBuffer(baseLamports uint64, bufferBps uint64) uint64 {
	if baseLamports == 0 || bufferBps == 0 {
		return baseLamports
	}
	delta := new(big.Int).SetUint64(baseLamports)
	delta.Mul(delta, big.NewInt(int64(bufferBps)))
	delta.Div(delta, big.NewInt(10000))
	out := new(big.Int).SetUint64(baseLamports)
	out.Add(out, delta)
	if !out.IsUint64() {
		return ^uint64(0)
	}
	return out.Uint64()
}

// pumpExtraHaircutMinOut — ещё −extraBps% к уже рассчитанному min_out.
func pumpExtraHaircutMinOut(minOut uint64, extraBps uint64) uint64 {
	if minOut == 0 || extraBps >= 10000 {
		return minOut
	}
	a := new(big.Int).SetUint64(minOut)
	a.Mul(a, big.NewInt(int64(10000-extraBps)))
	a.Div(a, big.NewInt(10000))
	if !a.IsUint64() {
		return 1
	}
	x := a.Uint64()
	if x == 0 {
		return 1
	}
	return x
}

// pumpEnvForceMinOutOne — по умолчанию true: min_tokens_out=1 (макс. допустимое проскальзывание), обходит 6024 Overflow из-за завышенного min.
// Отключить: PUMP_MIN_OUT_ONE=false
func pumpEnvForceMinOutOne() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("PUMP_MIN_OUT_ONE")))
	if s == "false" || s == "0" || s == "no" {
		return false
	}
	return true
}

// pumpComputeMinTokensOut — min_tokens для BuyExactSolIn по формуле IDL + запас под pfee GetFees vs Global.
// Все величины — сырые атомы (как u64 в контракте), decimals минта учитываются только на уровне mint (обычно 6).
func pumpComputeMinTokensOut(spendable, vSol, vToken, realToken, feeBps, creatorBps, slipBps uint64) (expectedOut, minOut uint64) {
	// Точная формула из IDL (fee из Global); pfee может отличаться — добавляем к slippage небольшой буфер.
	expectedOut = pumpQuoteExpectedTokensBuyExactSolIn(spendable, vSol, vToken, feeBps, creatorBps)
	if expectedOut == 0 {
		return 0, 0
	}
	if realToken > 0 && expectedOut > realToken {
		expectedOut = realToken
	}
	// Запас под расхождение pfee GetFees и Global (чем выше, тем ниже min_out).
	const pfeeSlipCushionBps uint64 = 2000
	totalSlip := slipBps + pfeeSlipCushionBps
	if totalSlip >= 9900 {
		totalSlip = 9899
	}
	minOut = applySlippage(expectedOut, totalSlip)
	if minOut == 0 && expectedOut > 0 {
		minOut = 1
	}
	// Нельзя требовать больше, чем лежит в real_token_reserves (частый триггер 6024 Overflow на стеке).
	if realToken > 0 && minOut > realToken {
		minOut = realToken
	}
	if minOut > expectedOut {
		minOut = expectedOut
	}
	return expectedOut, minOut
}

// pumpValidateBuyQuote — не отправляем транзакцию при нулевых/абсурдных значениях.
func pumpValidateBuyQuote(expectedOut, minOut, realToken uint64, mintDecimals uint8) error {
	if expectedOut == 0 || minOut == 0 {
		return fmt.Errorf("quote invalid: expected=%d min_out=%d", expectedOut, minOut)
	}
	if minOut > expectedOut {
		return fmt.Errorf("min_out>expected: %d > %d", minOut, expectedOut)
	}
	if realToken == 0 {
		return fmt.Errorf("real_token_reserves=0 (empty curve)")
	}
	if minOut > realToken {
		return fmt.Errorf("min_out>real_token_reserves: %d > %d", minOut, realToken)
	}
	// Защита от переполнения в промежуточных u64 на клиенте/RPC
	const maxAtoms = uint64(1 << 62)
	if expectedOut > maxAtoms || minOut > maxAtoms || realToken > maxAtoms {
		return fmt.Errorf("amounts too large (sanity cap)")
	}
	if mintDecimals > 9 {
		return fmt.Errorf("mint decimals=%d invalid", mintDecimals)
	}
	return nil
}

func mintDecimalsFromMintData(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (uint8, error) {
	acc, err := c.GetAccountInfoWithOpts(ctx, mint, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentProcessed})
	if err != nil || acc == nil || acc.Value == nil {
		return 0, fmt.Errorf("get mint account")
	}
	data := acc.Value.Data.GetBinary()
	if len(data) < 45 {
		return 0, fmt.Errorf("mint data too short")
	}
	// SPL Mint layout: decimals @ offset 44 (после mint_authority + supply)
	return data[44], nil
}

// ensurePumpUserATA — ATA пользователя с тем же token_program, что и минт (SPL vs Token-2022).
func ensurePumpUserATA(ctx context.Context, rpcClient *rpc.Client, preIxs *[]solana.Instruction, owner, mint, tokenProgram solana.PublicKey) (solana.PublicKey, error) {
	ata, _, err := solana.FindProgramAddress(
		[][]byte{owner.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.PublicKey{}, err
	}
	info, err := rpcClient.GetAccountInfoWithOpts(ctx, ata, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentProcessed})
	if err == nil && info != nil && info.Value != nil {
		return ata, nil
	}
	ix := solana.NewInstruction(
		solana.SPLAssociatedTokenAccountProgramID,
		[]*solana.AccountMeta{
			{PublicKey: owner, IsSigner: true, IsWritable: true},
			{PublicKey: ata, IsSigner: false, IsWritable: true},
			{PublicKey: owner, IsSigner: false, IsWritable: false},
			{PublicKey: mint, IsSigner: false, IsWritable: false},
			{PublicKey: solana.SystemProgramID, IsSigner: false, IsWritable: false},
			{PublicKey: tokenProgram, IsSigner: false, IsWritable: false},
		},
		[]byte{},
	)
	*preIxs = append(*preIxs, ix)
	return ata, nil
}

func ceilDivBig(a, b *big.Int) *big.Int {
	if b.Sign() == 0 {
		return big.NewInt(0)
	}
	num := new(big.Int).Add(a, new(big.Int).Sub(b, big.NewInt(1)))
	return num.Div(num, b)
}

func encodePumpBuyExactSolInData(spendableSolIn, minTokensOut uint64) []byte {
	// track_volume: OptionBool None = 0
	buf := make([]byte, 8+8+8+1)
	copy(buf[0:8], pumpBuyExactSolInDisc)
	binary.LittleEndian.PutUint64(buf[8:16], spendableSolIn)
	binary.LittleEndian.PutUint64(buf[16:24], minTokensOut)
	buf[24] = 0
	return buf
}

func encodePumpSellData(tokenAmount, minSolOut uint64) []byte {
	buf := make([]byte, 8+8+8)
	copy(buf[0:8], pumpSellDisc)
	binary.LittleEndian.PutUint64(buf[8:16], tokenAmount)
	binary.LittleEndian.PutUint64(buf[16:24], minSolOut)
	return buf
}

func mintTokenProgram(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (solana.PublicKey, error) {
	acc, err := c.GetAccountInfoWithOpts(ctx, mint, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentProcessed})
	if err != nil || acc == nil || acc.Value == nil {
		return solana.PublicKey{}, fmt.Errorf("get mint account")
	}
	return acc.Value.Owner, nil
}

// swapPumpFun покупка на bonding curve: spendable_lamports = BUY_LAMPORTS, min_tokens из slippageBps.
func swapPumpFun(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, mint solana.PublicKey, spendableLamports uint64) (solana.Signature, error) {
	owner := wallet.PublicKey()

	tokenProgram, err := mintTokenProgram(ctx, rpcClient, mint)
	if err != nil {
		return solana.Signature{}, err
	}
	mintDecimals, err := mintDecimalsFromMintData(ctx, rpcClient, mint)
	if err != nil {
		return solana.Signature{}, err
	}

	bondingCurve, _, err := derivePumpBondingCurve(mint)
	if err != nil {
		return solana.Signature{}, err
	}

	globalPK, _, err := derivePumpGlobal()
	if err != nil {
		return solana.Signature{}, err
	}
	gInfo, err := rpcClient.GetAccountInfoWithOpts(ctx, globalPK, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentProcessed})
	if err != nil || gInfo == nil || gInfo.Value == nil || gInfo.Value.Data == nil {
		return solana.Signature{}, fmt.Errorf("global account missing")
	}
	gData := gInfo.Value.Data.GetBinary()
	feeRecipient, feeBps, creatorFeeBps, err := parsePumpGlobalFees(gData)
	if err != nil {
		return solana.Signature{}, err
	}

	// Бюджет SOL в инструкции buy_exact_sol_in (первый аргумент) с небольшим запасом под движение кривой.
	spendableBudget := pumpSpendableWithBuffer(spendableLamports, pumpSpendableBufferBps)

	// После покупки на кошельке должно остаться ≥0.01 SOL под газ и приоритетные сборы (не сливать весь SOL в кривую).
	const minSolReserveAfterPumpBuyLamports uint64 = 10_000_000
	estThisTxFees := priorityFeeLamports + 25_000 // приоритет из .env + базовая комиссия/запас
	minBalanceRequired := spendableBudget + minSolReserveAfterPumpBuyLamports + estThisTxFees

	natBal, err := rpcClient.GetBalance(ctx, owner, rpc.CommitmentProcessed)
	if err != nil {
		return solana.Signature{}, fmt.Errorf("getBalance: %w", err)
	}
	if natBal == nil {
		return solana.Signature{}, fmt.Errorf("getBalance: empty response")
	}
	if natBal.Value < minBalanceRequired {
		return solana.Signature{}, fmt.Errorf(
			"insufficient SOL: after buy need ≥%.4f SOL reserve for fees + this tx (~%d lamports priority); have %d lamports, need ≥%d (spendable_budget %d)",
			float64(minSolReserveAfterPumpBuyLamports)/1e9,
			priorityFeeLamports,
			natBal.Value,
			minBalanceRequired,
			spendableBudget,
		)
	}

	// Самый свежий снимок Bonding Curve сразу перед котировкой и сборкой tx (без «кеша» между RPC-вызовами).
	bcInfo, err := rpcClient.GetAccountInfoWithOpts(ctx, bondingCurve, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentProcessed})
	if err != nil || bcInfo == nil || bcInfo.Value == nil || bcInfo.Value.Data == nil {
		return solana.Signature{}, fmt.Errorf("bonding curve account missing")
	}
	bcData := bcInfo.Value.Data.GetBinary()
	vTok, vSol, realToken, _, _, complete, creator, err := parsePumpBondingCurveData(bcData)
	if err != nil {
		return solana.Signature{}, err
	}
	if complete {
		return solana.Signature{}, fmt.Errorf("bonding curve complete (migrated)")
	}

	expectedOut, minOut := pumpComputeMinTokensOut(
		spendableBudget, vSol, vTok, realToken, feeBps, creatorFeeBps, slippageBps,
	)
	minOut = pumpExtraHaircutMinOut(minOut, pumpMinOutExtraHaircutBps)
	forceMinOne := pumpEnvForceMinOutOne()
	if forceMinOne {
		minOut = 1
	}
	if err := pumpValidateBuyQuote(expectedOut, minOut, realToken, mintDecimals); err != nil {
		return solana.Signature{}, fmt.Errorf("pump buy quote: %w", err)
	}
	log.Printf("[PUMP] quote: spendable_budget_lamports=%d (base=%d +%d bps) expected_atoms=%d min_out_atoms=%d (extra_haircut=%d bps, min_out_one=%v) real_token_reserves=%d mint_decimals=%d slip_bps=%d",
		spendableBudget, spendableLamports, pumpSpendableBufferBps, expectedOut, minOut, pumpMinOutExtraHaircutBps, forceMinOne, realToken, mintDecimals, slippageBps)

	assocBonding, _, err := solana.FindProgramAddress(
		[][]byte{
			bondingCurve.Bytes(),
			tokenProgram.Bytes(),
			mint.Bytes(),
		},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.Signature{}, err
	}

	assocUser, _, err := solana.FindProgramAddress(
		[][]byte{
			owner.Bytes(),
			tokenProgram.Bytes(),
			mint.Bytes(),
		},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.Signature{}, err
	}

	creatorVault, _, err := derivePumpCreatorVault(creator)
	if err != nil {
		return solana.Signature{}, err
	}
	eventAuth, _, err := derivePumpEventAuthority()
	if err != nil {
		return solana.Signature{}, err
	}
	gVol, _, err := derivePumpGlobalVolumeAccumulator()
	if err != nil {
		return solana.Signature{}, err
	}
	uVol, _, err := derivePumpUserVolumeAccumulator(owner)
	if err != nil {
		return solana.Signature{}, err
	}
	feeCfg, _, err := derivePumpFeeConfig()
	if err != nil {
		return solana.Signature{}, err
	}
	bondingCurveV2, _, err := derivePumpBondingCurveV2(mint)
	if err != nil {
		return solana.Signature{}, err
	}

	data := encodePumpBuyExactSolInData(spendableBudget, minOut)

	metas := []*solana.AccountMeta{
		{PublicKey: globalPK, IsSigner: false, IsWritable: false},
		{PublicKey: feeRecipient, IsSigner: false, IsWritable: true},
		{PublicKey: mint, IsSigner: false, IsWritable: false},
		{PublicKey: bondingCurve, IsSigner: false, IsWritable: true},
		{PublicKey: assocBonding, IsSigner: false, IsWritable: true},
		{PublicKey: assocUser, IsSigner: false, IsWritable: true},
		{PublicKey: owner, IsSigner: true, IsWritable: true},
		{PublicKey: solana.SystemProgramID, IsSigner: false, IsWritable: false},
		{PublicKey: tokenProgram, IsSigner: false, IsWritable: false},
		{PublicKey: creatorVault, IsSigner: false, IsWritable: true},
		{PublicKey: eventAuth, IsSigner: false, IsWritable: false},
		{PublicKey: pumpFunProgram, IsSigner: false, IsWritable: false},
		{PublicKey: gVol, IsSigner: false, IsWritable: false},
		{PublicKey: uVol, IsSigner: false, IsWritable: true},
		{PublicKey: feeCfg, IsSigner: false, IsWritable: false},
		{PublicKey: pumpFeeProgramPK, IsSigner: false, IsWritable: false},
		{PublicKey: bondingCurveV2, IsSigner: false, IsWritable: false},
	}

	buyIx := solana.NewInstruction(pumpFunProgram, metas, data)

	var preIxs []solana.Instruction
	if _, err := ensurePumpUserATA(ctx, rpcClient, &preIxs, owner, mint, tokenProgram); err != nil {
		return solana.Signature{}, fmt.Errorf("user ATA: %w", err)
	}

	cuLimitIx, err := computebudget.NewSetComputeUnitLimitInstruction(ComputeUnitLimit).ValidateAndBuild()
	if err != nil {
		return solana.Signature{}, err
	}
	microPerCU := effectiveMicroLamportsPerCU()
	cuPriceIx, err := computebudget.NewSetComputeUnitPriceInstruction(microPerCU).ValidateAndBuild()
	if err != nil {
		return solana.Signature{}, err
	}

	recent, err := rpcClient.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return solana.Signature{}, err
	}

	all := append([]solana.Instruction{}, cuLimitIx, cuPriceIx)
	all = append(all, preIxs...)
	all = append(all, buyIx)

	tx, err := solana.NewTransaction(all, recent.Value.Blockhash, solana.TransactionPayer(owner))
	if err != nil {
		return solana.Signature{}, err
	}
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(owner) {
			return &wallet
		}
		return nil
	})
	if err != nil {
		return solana.Signature{}, err
	}

	return sendSwapTransaction(ctx, rpcClient, tx)
}

// registerLivePumpBuy — после успешной покупки: цена входа в USD и мониторинг.
// livePumpHasOpenPosition — уже есть открытая live-позиция по этому mint (анти-дубль create + smart-follow).
func livePumpHasOpenPosition(mint string) bool {
	livePumpMu.Lock()
	defer livePumpMu.Unlock()
	p, ok := livePumpOpenPositions[mint]
	return ok && p != nil && p.RemainingFraction > 0
}

// pumpBondingCurveCreator — creator из аккаунта bonding curve (для risk-check при follow, не из create-tx).
func pumpBondingCurveCreator(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (solana.PublicKey, error) {
	bondingCurve, _, err := derivePumpBondingCurve(mint)
	if err != nil {
		return solana.PublicKey{}, err
	}
	bcInfo, err := c.GetAccountInfoWithOpts(ctx, bondingCurve, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentProcessed})
	if err != nil || bcInfo == nil || bcInfo.Value == nil || bcInfo.Value.Data == nil {
		return solana.PublicKey{}, fmt.Errorf("bonding curve account missing")
	}
	bcData := bcInfo.Value.Data.GetBinary()
	_, _, _, _, _, _, creator, err := parsePumpBondingCurveData(bcData)
	if err != nil {
		return solana.PublicKey{}, err
	}
	return creator, nil
}

func registerLivePumpBuy(mint string, entryUSD float64) {
	livePumpMu.Lock()
	defer livePumpMu.Unlock()
	now := time.Now()
	livePumpOpenPositions[mint] = &livePumpPosition{
		EntryUSD:          entryUSD,
		OpenedAt:          now,
		LastAPIPriceOKAt:  now,
		LastKnownPriceUSD: entryUSD,
		RemainingFraction: 1.0,
		DidMoonshotHalf:   false,
		HighWaterMark:     0,
		TrailingArmed:     false,
	}
}

func deleteLivePump(mint string) {
	livePumpMu.Lock()
	delete(livePumpOpenPositions, mint)
	livePumpMu.Unlock()
}

func solscanTxURL(sig string) string {
	return "https://solscan.io/tx/" + sig
}

// startLivePumpExitTracker — те же правила, что monitorPosition, с реальной продажей pump sell.
func startLivePumpExitTracker(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, mintStr string) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[LIVE PUMP] monitor panic recovered: %v", r)
			}
		}()
		t := time.NewTicker(pricePollInterval)
		defer t.Stop()
		if done := monitorLivePumpOnce(ctx, rpcClient, wallet, mintStr); done {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if monitorLivePumpOnce(ctx, rpcClient, wallet, mintStr) {
					return
				}
			}
		}
	}()
}

func monitorLivePumpOnce(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, mintStr string) bool {
	pctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	price, err := fetchTokenPriceUSDFromAPIs(pctx, mintStr)

	livePumpMu.Lock()
	pos, ok := livePumpOpenPositions[mintStr]
	if !ok || pos == nil || pos.RemainingFraction <= 0 {
		livePumpMu.Unlock()
		return true
	}
	entryUSD := pos.EntryUSD
	rem := pos.RemainingFraction

	if err == nil && price > 0 {
		pos.LastAPIPriceOKAt = time.Now()
		pos.LastKnownPriceUSD = price
		if price > pos.HighWaterMark {
			pos.HighWaterMark = price
		}
	}
	hwm := pos.HighWaterMark
	trailingArmed := pos.TrailingArmed
	livePumpMu.Unlock()

	mintPK, errPK := solana.PublicKeyFromBase58(mintStr)
	if errPK != nil {
		log.Printf("[LIVE PUMP] bad mint: %v", errPK)
		return true
	}

	if err != nil || price <= 0 {
		return false
	}

	pnl := positionPnLPercent(entryUSD, price)

	if pnl >= livePumpTakeProfitPnLPct && rem > 0 {
		log.Printf("[LIVE PUMP] TAKE_PROFIT +%.0f%% target %s pnl=%.2f%%", livePumpTakeProfitPnLPct, mintStr, pnl)
		if _, e := swapPumpFunSellAll(pctx, rpcClient, wallet, mintPK, slippageBps); e != nil {
			log.Printf("[LIVE PUMP] TP sell failed: %v", e)
		}
		deleteLivePump(mintStr)
		return true
	}

	livePumpMu.Lock()
	if p2 := livePumpOpenPositions[mintStr]; p2 != nil && !p2.TrailingArmed && pnl >= livePumpTrailingArmPnLPct && pnl < livePumpTakeProfitPnLPct {
		p2.TrailingArmed = true
		if price > p2.HighWaterMark {
			p2.HighWaterMark = price
		}
	}
	if p2 := livePumpOpenPositions[mintStr]; p2 != nil {
		hwm = p2.HighWaterMark
		trailingArmed = p2.TrailingArmed
	}
	livePumpMu.Unlock()

	if !trailingArmed {
		if price <= entryUSD*targetStopLossMultiplier {
			log.Printf("[LIVE PUMP] STOP_LOSS %s", mintStr)
			if _, e := swapPumpFunSellAll(pctx, rpcClient, wallet, mintPK, slippageBps); e != nil {
				log.Printf("[LIVE PUMP] SL sell failed: %v", e)
			}
			deleteLivePump(mintStr)
			return true
		}
		return false
	}

	trailFloor := hwm * (1.0 - livePumpTrailingDrawdownFromHWM)
	if price <= trailFloor {
		log.Printf("[LIVE PUMP] TRAILING_STOP %s (floor vs price)", mintStr)
		if _, e := swapPumpFunSellAll(pctx, rpcClient, wallet, mintPK, slippageBps); e != nil {
			log.Printf("[LIVE PUMP] trailing sell failed: %v", e)
		}
		deleteLivePump(mintStr)
		return true
	}
	if price <= entryUSD*targetStopLossMultiplier {
		log.Printf("[LIVE PUMP] STOP_LOSS (hard) %s", mintStr)
		if _, e := swapPumpFunSellAll(pctx, rpcClient, wallet, mintPK, slippageBps); e != nil {
			log.Printf("[LIVE PUMP] SL sell failed: %v", e)
		}
		deleteLivePump(mintStr)
		return true
	}
	return false
}

func swapPumpFunSellAll(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, mint solana.PublicKey, slipBps uint64) (solana.Signature, error) {
	owner := wallet.PublicKey()
	tokenProgram, err := mintTokenProgram(ctx, rpcClient, mint)
	if err != nil {
		return solana.Signature{}, err
	}
	ata, _, err := solana.FindProgramAddress(
		[][]byte{owner.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.Signature{}, err
	}
	bal, err := rpcClient.GetTokenAccountBalance(ctx, ata, rpc.CommitmentProcessed)
	if err != nil || bal == nil || bal.Value == nil {
		return solana.Signature{}, fmt.Errorf("token balance: %w", err)
	}
	raw, err := strconv.ParseUint(bal.Value.Amount, 10, 64)
	if err != nil || raw == 0 {
		return solana.Signature{}, fmt.Errorf("zero token balance")
	}
	return swapPumpFunSellAmount(ctx, rpcClient, wallet, mint, raw, slipBps)
}

func swapPumpFunSellFraction(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, mint solana.PublicKey, fraction float64, slipBps uint64) (solana.Signature, error) {
	if fraction <= 0 {
		return solana.Signature{}, fmt.Errorf("fraction <= 0")
	}
	owner := wallet.PublicKey()
	tokenProgram, err := mintTokenProgram(ctx, rpcClient, mint)
	if err != nil {
		return solana.Signature{}, err
	}
	ata, _, err := solana.FindProgramAddress(
		[][]byte{owner.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.Signature{}, err
	}
	bal, err := rpcClient.GetTokenAccountBalance(ctx, ata, rpc.CommitmentProcessed)
	if err != nil || bal == nil || bal.Value == nil {
		return solana.Signature{}, fmt.Errorf("token balance: %w", err)
	}
	raw, err := strconv.ParseUint(bal.Value.Amount, 10, 64)
	if err != nil || raw == 0 {
		return solana.Signature{}, fmt.Errorf("zero token balance")
	}
	amt := uint64(float64(raw) * fraction)
	if amt == 0 {
		amt = 1
	}
	if amt > raw {
		amt = raw
	}
	return swapPumpFunSellAmount(ctx, rpcClient, wallet, mint, amt, slipBps)
}

// pumpQuoteMinSolForSell — грубая оценка min SOL из constant product (без детальных fee tiers).
func pumpQuoteMinSolForSell(tokenAmount, vSol, vToken uint64, slipBps uint64) uint64 {
	if tokenAmount == 0 || vToken <= tokenAmount {
		return 0
	}
	num := new(big.Int).SetUint64(tokenAmount)
	num.Mul(num, new(big.Int).SetUint64(vSol))
	den := new(big.Int).SetUint64(vToken)
	den.Sub(den, new(big.Int).SetUint64(tokenAmount))
	if den.Sign() <= 0 {
		return 0
	}
	num.Div(num, den)
	if !num.IsUint64() {
		return 0
	}
	out := num.Uint64()
	// min после slippage (меньше ожидаемого SOL)
	return applySlippage(out, slipBps)
}

func swapPumpFunSellAmount(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, mint solana.PublicKey, tokenAmount uint64, slipBps uint64) (solana.Signature, error) {
	owner := wallet.PublicKey()

	bondingCurve, _, err := derivePumpBondingCurve(mint)
	if err != nil {
		return solana.Signature{}, err
	}
	bcInfo, err := rpcClient.GetAccountInfoWithOpts(ctx, bondingCurve, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentProcessed})
	if err != nil || bcInfo == nil || bcInfo.Value == nil {
		return solana.Signature{}, fmt.Errorf("bonding curve")
	}
	bcData := bcInfo.Value.Data.GetBinary()
	vTok, vSol, _, _, _, complete, creator, err := parsePumpBondingCurveData(bcData)
	if err != nil {
		return solana.Signature{}, err
	}
	if complete {
		return solana.Signature{}, fmt.Errorf("curve complete — use DEX, not pump sell")
	}

	minSol := pumpQuoteMinSolForSell(tokenAmount, vSol, vTok, slipBps)
	if minSol == 0 {
		minSol = 1
	}

	globalPK, _, _ := derivePumpGlobal()
	gInfo, err := rpcClient.GetAccountInfoWithOpts(ctx, globalPK, &rpc.GetAccountInfoOpts{Encoding: solana.EncodingBase64, Commitment: rpc.CommitmentProcessed})
	if err != nil || gInfo == nil || gInfo.Value == nil {
		return solana.Signature{}, fmt.Errorf("global")
	}
	feeRecipient, _, _, err := parsePumpGlobalFees(gInfo.Value.Data.GetBinary())
	if err != nil {
		return solana.Signature{}, err
	}

	tokenProgram, err := mintTokenProgram(ctx, rpcClient, mint)
	if err != nil {
		return solana.Signature{}, err
	}

	assocBonding, _, err := solana.FindProgramAddress(
		[][]byte{bondingCurve.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.Signature{}, err
	}

	assocUser, _, err := solana.FindProgramAddress(
		[][]byte{owner.Bytes(), tokenProgram.Bytes(), mint.Bytes()},
		solana.SPLAssociatedTokenAccountProgramID,
	)
	if err != nil {
		return solana.Signature{}, err
	}

	creatorVault, _, err := derivePumpCreatorVault(creator)
	if err != nil {
		return solana.Signature{}, err
	}
	eventAuth, _, err := derivePumpEventAuthority()
	if err != nil {
		return solana.Signature{}, err
	}
	feeCfg, _, err := derivePumpFeeConfig()
	if err != nil {
		return solana.Signature{}, err
	}
	bondingCurveV2, _, err := derivePumpBondingCurveV2(mint)
	if err != nil {
		return solana.Signature{}, err
	}

	data := encodePumpSellData(tokenAmount, minSol)

	metas := []*solana.AccountMeta{
		{PublicKey: globalPK, IsSigner: false, IsWritable: false},
		{PublicKey: feeRecipient, IsSigner: false, IsWritable: true},
		{PublicKey: mint, IsSigner: false, IsWritable: false},
		{PublicKey: bondingCurve, IsSigner: false, IsWritable: true},
		{PublicKey: assocBonding, IsSigner: false, IsWritable: true},
		{PublicKey: assocUser, IsSigner: false, IsWritable: true},
		{PublicKey: owner, IsSigner: true, IsWritable: true},
		{PublicKey: solana.SystemProgramID, IsSigner: false, IsWritable: false},
		{PublicKey: creatorVault, IsSigner: false, IsWritable: true},
		{PublicKey: tokenProgram, IsSigner: false, IsWritable: false},
		{PublicKey: eventAuth, IsSigner: false, IsWritable: false},
		{PublicKey: pumpFunProgram, IsSigner: false, IsWritable: false},
		{PublicKey: feeCfg, IsSigner: false, IsWritable: false},
		{PublicKey: pumpFeeProgramPK, IsSigner: false, IsWritable: false},
		{PublicKey: bondingCurveV2, IsSigner: false, IsWritable: false},
	}

	sellIx := solana.NewInstruction(pumpFunProgram, metas, data)

	cuLimitIx, err := computebudget.NewSetComputeUnitLimitInstruction(ComputeUnitLimit).ValidateAndBuild()
	if err != nil {
		return solana.Signature{}, err
	}
	microPerCU := effectiveMicroLamportsPerCU()
	cuPriceIx, err := computebudget.NewSetComputeUnitPriceInstruction(microPerCU).ValidateAndBuild()
	if err != nil {
		return solana.Signature{}, err
	}

	recent, err := rpcClient.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return solana.Signature{}, err
	}

	all := []solana.Instruction{cuLimitIx, cuPriceIx, sellIx}
	tx, err := solana.NewTransaction(all, recent.Value.Blockhash, solana.TransactionPayer(owner))
	if err != nil {
		return solana.Signature{}, err
	}
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(owner) {
			return &wallet
		}
		return nil
	})
	if err != nil {
		return solana.Signature{}, err
	}
	return sendSwapTransaction(ctx, rpcClient, tx)
}
