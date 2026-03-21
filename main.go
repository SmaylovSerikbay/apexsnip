// Solana «sniper» (образовательный каркас): WebSocket по нескольким программам.
//
// Raydium AMM V4: Initialize2 → rug-check → симуляция / swap_base_in + Jupiter TP/SL.
// Pump.fun: Anchor create → mint из инструкции → симуляция BUY (без Raydium V4 пула).
// Raydium CPMM (CP-Swap): Anchor initialize → выбор mint → симуляция BUY.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gagliardetto/solana-go"
	associatedtokenaccount "github.com/gagliardetto/solana-go/programs/associated-token-account"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	system "github.com/gagliardetto/solana-go/programs/system"
	spltoken "github.com/gagliardetto/solana-go/programs/token"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/ws"
	"github.com/joho/godotenv"
	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waptypes "go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

// --- Конфигурация поведения ---

// IS_SIMULATION: true — не подписывать и не слать транзакции в сеть (только логи).
// false — реальный swap (нужны SOL/WSOL, ATA, рабочий RPC и приватный ключ).
const IS_SIMULATION = true

// Одночасовой эксперимент: виртуальный бюджет USD (только при IS_SIMULATION).
const (
	simStartingBalanceUSD = 10.0
	simBetUSD             = 1.0  // каждая симуляция BUY списывает ровно $1 + комиссия
	simTradeFeeBuyUSD     = 0.015 // ~priority fee, диапазон $0.01–0.02
	simTradeFeeSellUSD    = 0.015
	simExperimentDuration   = 60 * time.Minute
	simPortfolioLogEvery  = 5 * time.Minute
)

// BUY_LAMPORTS — размер «покупки» в лампортах (0.05 SOL по умолчанию).
const BUY_LAMPORTS uint64 = 50_000_000

// SlippageBps — 15% = 1500 bps (используется при расчёте minimum_amount_out).
const SlippageBps uint64 = 1500

// PriorityFeeLamports — целевой приоритетный «чай» ~0.001 SOL (lamports), в комбинации с MinMicroLamportsPerCU ниже.
const PriorityFeeLamports uint64 = 1_000_000

// MinMicroLamportsPerCU — нижняя граница цены за CU (рекомендация валидаторов / конкуренция в мемпуле).
const MinMicroLamportsPerCU uint64 = 100_000

// ComputeUnitLimit — Raydium + Serum CPI + ATA + опционально wrap SOL (Transfer+SyncNative).
const ComputeUnitLimit uint32 = 600_000

// Jupiter price endpoints (основной + запасной).
const (
	jupiterPricePrimary  = "https://lite-api.jup.ag/price/v3?ids="
	jupiterPriceFallback = "https://api.jup.ag/price/v2?ids="
	pricePollInterval    = 2 * time.Second
	takeProfitMultiplier = 1.50
	stopLossMultiplier   = 0.80
)

// snipesLogFile — журнал сделок (BUY/SELL), без шума в консоли.
const (
	snipesLogFile = "snipes.log"
	bannerSep     = "========================================"

	defaultRefSOLUSD       = 200.0
	defaultMinLiquidityUSD = 3000.0
)

// Raydium AMM V4 (Liquidity Pool), как в ТЗ.
var raydiumAMMProgram = solana.MustPublicKeyFromBase58("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8")

// pumpFunProgram / raydiumCPMMProgram задаются в main() из .env или дефолтов (см. pump-public-docs / Raydium docs).
var (
	pumpFunProgram     solana.PublicKey
	raydiumCPMMProgram solana.PublicKey
)

var (
	wsolMint = solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112")
	usdcMint = solana.MustPublicKeyFromBase58("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
	usdtMint = solana.MustPublicKeyFromBase58("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB")
)

// initialize2AnchorDisc — 8 байт Anchor-дискриминатора (если пул создаётся через Anchor-обёртку).
var initialize2AnchorDisc = anchorDiscriminator("global:initialize2")

// Дискриминаторы Anchor (sha256("global:<name>")[:8]) для Pump и Raydium CP-Swap (IDL raydium_cp_swap).
var (
	pumpCreateDisc       = anchorDiscriminator("global:create")
	cpmmInitializeAnchor = anchorDiscriminator("global:initialize")
)

// raydiumInstructionInitialize2 — нативный тег enum Raydium AMM для Initialize2 (см. raydium-sdk: instruction 1).
const raydiumInstructionInitialize2 uint8 = 1

// raydiumSwapBaseIn — swap с фиксированным входом (Raydium SDK: instruction 9).
const raydiumSwapBaseIn uint8 = 9

// Offsets LiquidityStateV4 — см. raydium-io/raydium-sdk liquidity/layout.ts LIQUIDITY_STATE_LAYOUT_V4.
// 32 поля по u64 = 256 байт; затем u128×4 + u64×2 = 80 байт → итого 336 байт до первого Pubkey.
// baseVault @336, quoteVault @368, baseMint @400, quoteMint @432, lpMint @464,
// openOrders @496, marketId @528, marketProgramId @560, targetOrders @592.
const liqV4BaseVaultOffset = 336

// Offsets Serum / OpenBook Market v3 (raydium-sdk serum/layout.ts).
const (
	mktOffsetBaseVault        = 117
	mktOffsetQuoteVault       = 165
	mktOffsetEventQueue       = 253
	mktOffsetBids             = 285
	mktOffsetAsks             = 317
	mktOffsetVaultSignerNonce = 45
)

// Источник подписки logsSubscribeMentions (мультиплекс на одном WSS).
const (
	listenKindV4 = iota
	listenKindPump
	listenKindCPMM
)

// --- Глобальное состояние для SwapToken(mint, amount) ---

var (
	poolRegistry   sync.Map // string (mint base58) -> *RaydiumPoolKeys
	seenSignatures sync.Map // дедупликация сигнатур
	snipeLogMu     sync.Mutex

	// WhatsApp (whatsmeow)
	waMu         sync.Mutex
	waClient     *whatsmeow.Client
	waRecipient  waptypes.JID
	waConfigured bool

	// Диагностика WebSocket (logsSubscribeMentions): видно, приходят ли вообще уведомления.
	wsNotifOK    atomic.Uint64 // сообщения с успешной транзакцией (Value.Err == nil)
	wsRayLogLine atomic.Uint64 // найден непустой фрагмент после «ray_log»
	wsSwapLike     atomic.Uint64 // в логах есть swap_base_in/out (типичная активность, не новый пул)
	wsHybridPass   atomic.Uint64 // V4: Init-ray_log или initialize2 в логах
	wsPassedPump   atomic.Uint64 // Pump: лог «Instruction: create» + дальше разбор tx
	wsPassedCPMM   atomic.Uint64 // CPMM: лог «Instruction: initialize» + разбор tx

	// Виртуальный портфель (IS_SIMULATION): старт $10, ставка $1 + комиссии на сделку.
	simMu              sync.Mutex
	simCashUSD         = simStartingBalanceUSD
	simOpenEntryUSD    = make(map[string]float64) // mint -> цена входа Jupiter (USD)
	simClosedTrades    atomic.Uint64               // завершённые сделки (SELL)
	simTakeProfitHits  atomic.Uint64
	simStopLossHits    atomic.Uint64
	simFinalReportOnce sync.Once
)

const (
	whatsappSessionDB  = "file:session.db?_pragma=foreign_keys(1)"
	whatsappSQLDialect = "sqlite"
)

// RaydiumPoolKeys — всё, чтобы собрать swap_base_in v4 (как в raydium-sdk makeSwapFixedInInstruction).
type RaydiumPoolKeys struct {
	AMM              solana.PublicKey
	Authority        solana.PublicKey
	OpenOrders       solana.PublicKey
	TargetOrders     solana.PublicKey
	BaseVault        solana.PublicKey
	QuoteVault       solana.PublicKey
	BaseMint         solana.PublicKey
	QuoteMint        solana.PublicKey
	MarketProgramID  solana.PublicKey
	MarketID         solana.PublicKey
	MarketBids       solana.PublicKey
	MarketAsks       solana.PublicKey
	MarketEventQueue solana.PublicKey
	MarketBaseVault  solana.PublicKey
	MarketQuoteVault solana.PublicKey
	MarketAuthority  solana.PublicKey
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	_ = godotenv.Load()

	if err := InitWhatsApp(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "WhatsApp: init failed, notifications disabled: %v\n", err)
	} else if waConfigured {
		SendWA("🚀 Sniper initialized. QR scan successful.")
	}

	rpcURL := os.Getenv("RPC_URL")
	wssURL := os.Getenv("WSS_URL")
	privKey := strings.TrimSpace(os.Getenv("PRIVATE_KEY"))
	if rpcURL == "" || wssURL == "" {
		log.Fatal("RPC_URL and WSS_URL are required (Helius/Alchemy mainnet-beta)")
	}

	rpcClient := rpc.New(rpcURL)

	// Официальные mainnet ID: pump-public-docs и Raydium (CP-Swap). В промпте часто встречаются опечатки — правьте .env.
	pumpFunProgram = publicKeyFromEnvOrDefault("PUMP_PROGRAM_ID", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
	raydiumCPMMProgram = publicKeyFromEnvOrDefault("RAYDIUM_CPMM_PROGRAM_ID", "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C")
	log.Printf("listener: pump=%s… cpmm=%s…", pumpFunProgram.String()[:8], raydiumCPMMProgram.String()[:8])

	var wallet solana.PrivateKey
	if !IS_SIMULATION {
		if privKey == "" {
			log.Fatal("PRIVATE_KEY is required when IS_SIMULATION=false")
		}
		var err error
		wallet, err = solana.PrivateKeyFromBase58(privKey)
		if err != nil {
			log.Fatalf("PRIVATE_KEY base58 decode: %v", err)
		}
		log.Printf("live mode: wallet %s", wallet.PublicKey().String())
	}
	// В SIMULATION режиме консоль без лишнего шума: только heartbeat и баннеры BUY/SELL.

	ctx := context.Background()

	go heartbeatLoop(ctx)
	go runRaydiumListenerForever(ctx, wssURL, rpcClient, wallet)
	if IS_SIMULATION {
		fmt.Printf("[SIM Experiment] длительность %v | старт $%.2f | ставка $%.2f + комиссия покупки $%.2f | комиссия продажи $%.2f | лог портфеля каждые %v\n",
			simExperimentDuration, simStartingBalanceUSD, simBetUSD, simTradeFeeBuyUSD, simTradeFeeSellUSD, simPortfolioLogEvery)
		go simulationPortfolioLogLoop(ctx)
		go simulationExperimentTimer(ctx)
	}

	select {}
}

// ---------- 1) WebSocket: логи по упоминанию программ (V4 + Pump + CPMM) ----------

// runRaydiumListenerForever переподключается к WSS и заново подписывается на logsSubscribe при обрыве.
func runRaydiumListenerForever(ctx context.Context, wssURL string, rpcClient *rpc.Client, wallet solana.PrivateKey) {
	backoff := 2 * time.Second
	const maxBackoff = 60 * time.Second
	resetBackoff := func() { backoff = 2 * time.Second }
	for {
		if ctx.Err() != nil {
			return
		}
		err := runMultiProgramListenerSession(ctx, wssURL, rpcClient, wallet, resetBackoff)
		if ctx.Err() != nil {
			return
		}
		log.Printf("[ws] session ended: %v — reconnect in %s", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			next := backoff * 2
			if next > maxBackoff {
				next = maxBackoff
			}
			backoff = next
		}
	}
}

// runMultiProgramListenerSession — одно WSS-подключение, три подписки (V4 + Pump + CPMM) до ошибки Recv.
func runMultiProgramListenerSession(ctx context.Context, wssURL string, rpcClient *rpc.Client, wallet solana.PrivateKey, onSubscribed func()) error {
	client, err := ws.Connect(ctx, wssURL)
	if err != nil {
		return fmt.Errorf("ws connect: %w", err)
	}
	defer client.Close()

	const expectedRaydiumAMM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
	if raydiumAMMProgram.String() != expectedRaydiumAMM {
		return fmt.Errorf("program id mismatch: got %s want %s", raydiumAMMProgram, expectedRaydiumAMM)
	}

	subV4, err := client.LogsSubscribeMentions(raydiumAMMProgram, rpc.CommitmentProcessed)
	if err != nil {
		return fmt.Errorf("logsSubscribeMentions v4: %w", err)
	}
	defer subV4.Unsubscribe()

	subPump, err := client.LogsSubscribeMentions(pumpFunProgram, rpc.CommitmentProcessed)
	if err != nil {
		return fmt.Errorf("logsSubscribeMentions pump: %w", err)
	}
	defer subPump.Unsubscribe()

	subCPMM, err := client.LogsSubscribeMentions(raydiumCPMMProgram, rpc.CommitmentProcessed)
	if err != nil {
		return fmt.Errorf("logsSubscribeMentions cpmm: %w", err)
	}
	defer subCPMM.Unsubscribe()

	if onSubscribed != nil {
		onSubscribed()
	}
	log.Printf("[ws] subscribed logsSubscribeMentions: v4=%s… pump=%s… cpmm=%s… commitment=processed",
		raydiumAMMProgram.String()[:8], pumpFunProgram.String()[:8], raydiumCPMMProgram.String()[:8])

	sessCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup

	recordErr := func(e error) {
		if e == nil {
			return
		}
		mu.Lock()
		if firstErr == nil {
			firstErr = fmt.Errorf("ws recv: %w", e)
		}
		mu.Unlock()
		cancel()
	}

	run := func(kind int, sub *ws.LogSubscription) {
		defer wg.Done()
		err := recvProgramLogsLoop(sessCtx, kind, sub, rpcClient, wallet)
		if err != nil && !errors.Is(err, context.Canceled) {
			recordErr(err)
		}
	}

	wg.Add(3)
	go run(listenKindV4, subV4)
	go run(listenKindPump, subPump)
	go run(listenKindCPMM, subCPMM)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if firstErr != nil {
		return firstErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func recvProgramLogsLoop(ctx context.Context, kind int, sub *ws.LogSubscription, rpcClient *rpc.Client, wallet solana.PrivateKey) error {
	for {
		msg, err := sub.Recv(ctx)
		if err != nil {
			return err
		}
		switch kind {
		case listenKindV4:
			onV4LogMessage(ctx, rpcClient, wallet, msg)
		case listenKindPump:
			onPumpLogMessage(ctx, rpcClient, wallet, msg)
		case listenKindCPMM:
			onCPMMLogMessage(ctx, rpcClient, wallet, msg)
		}
	}
}

func onV4LogMessage(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, msg *ws.LogResult) {
	if msg.Value.Err != nil {
		return
	}
	wsNotifOK.Add(1)
	if logsLookLikeRaydiumSwap(msg.Value.Logs) {
		wsSwapLike.Add(1)
	}

	payload, found := firstRayLogPayload(msg.Value.Logs)
	if found {
		wsRayLogLine.Add(1)
	}
	initByPayload := found && rayLogPayloadIsInitPool(payload)
	initByLogs := logsContainRaydiumInitialize2(msg.Value.Logs)
	if !initByPayload && !initByLogs {
		return
	}
	wsHybridPass.Add(1)
	if found {
		printRayLogSample(msg.Value.Signature.String(), payload)
	} else {
		log.Printf("[ray_log sample] sig=%s payload=(нет строки ray_log, но в логах initialize2)", msg.Value.Signature.String())
	}

	sig := msg.Value.Signature
	if _, loaded := seenSignatures.LoadOrStore(sig.String(), true); loaded {
		return
	}
	go handleRaydiumLogNotification(ctx, rpcClient, wallet, sig)
}

func onPumpLogMessage(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, msg *ws.LogResult) {
	if msg.Value.Err != nil {
		return
	}
	wsNotifOK.Add(1)
	if !logsAnchorInstructionForProgram(msg.Value.Logs, pumpFunProgram, "create") {
		return
	}
	wsPassedPump.Add(1)

	sig := msg.Value.Signature
	if _, loaded := seenSignatures.LoadOrStore(sig.String(), true); loaded {
		return
	}
	go handlePumpCreateNotification(ctx, rpcClient, wallet, sig)
}

func onCPMMLogMessage(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, msg *ws.LogResult) {
	if msg.Value.Err != nil {
		return
	}
	wsNotifOK.Add(1)
	if !logsAnchorInstructionForProgram(msg.Value.Logs, raydiumCPMMProgram, "initialize") {
		return
	}
	wsPassedCPMM.Add(1)

	sig := msg.Value.Signature
	if _, loaded := seenSignatures.LoadOrStore(sig.String(), true); loaded {
		return
	}
	go handleCPMMInitializeNotification(ctx, rpcClient, wallet, sig)
}

// logsAnchorInstructionForProgram — «Instruction: <name>» только для логов текущего CPI-фрейма target program,
// с точной границей имени (не срабатывает на CreateMetadata, InitializeMint, initialize2 и т.д.).
func logsAnchorInstructionForProgram(logs []string, wantProgram solana.PublicKey, instNameLower string) bool {
	var stack []solana.PublicKey
	for _, line := range logs {
		switch {
		case strings.HasPrefix(line, "Program ") && strings.Contains(line, " invoke ["):
			if pk, ok := parseProgramLinePubkeyInvoke(line); ok {
				stack = append(stack, pk)
			}
		case strings.HasPrefix(line, "Program ") && (strings.Contains(line, " success") || strings.Contains(line, " failed")):
			if pk, ok := parseProgramLinePubkeyEnd(line); ok {
				for len(stack) > 0 && !stack[len(stack)-1].Equals(pk) {
					stack = stack[:len(stack)-1]
				}
				if len(stack) > 0 {
					stack = stack[:len(stack)-1]
				}
			}
		case strings.Contains(line, "Program log:"):
			if len(stack) == 0 || !stack[len(stack)-1].Equals(wantProgram) {
				continue
			}
			if anchorInstructionNameMatch(strings.ToLower(line), instNameLower) {
				return true
			}
		}
	}
	return false
}

func parseProgramLinePubkeyInvoke(line string) (solana.PublicKey, bool) {
	const prefix = "Program "
	if !strings.HasPrefix(line, prefix) {
		return solana.PublicKey{}, false
	}
	idx := strings.Index(line, " invoke [")
	if idx <= len(prefix) {
		return solana.PublicKey{}, false
	}
	s := strings.TrimSpace(line[len(prefix):idx])
	if s == "" {
		return solana.PublicKey{}, false
	}
	pk, err := solana.PublicKeyFromBase58(s)
	if err != nil {
		return solana.PublicKey{}, false
	}
	return pk, true
}

func parseProgramLinePubkeyEnd(line string) (solana.PublicKey, bool) {
	const prefix = "Program "
	if !strings.HasPrefix(line, prefix) {
		return solana.PublicKey{}, false
	}
	rest := line[len(prefix):]
	var cut int
	switch {
	case strings.Contains(rest, " success"):
		cut = strings.Index(rest, " success")
	case strings.Contains(rest, " failed"):
		cut = strings.Index(rest, " failed")
	default:
		return solana.PublicKey{}, false
	}
	s := strings.TrimSpace(rest[:cut])
	if s == "" {
		return solana.PublicKey{}, false
	}
	pk, err := solana.PublicKeyFromBase58(s)
	if err != nil {
		return solana.PublicKey{}, false
	}
	return pk, true
}

func anchorInstructionNameMatch(lineLower, instLower string) bool {
	needle := "instruction: " + instLower
	idx := strings.Index(lineLower, needle)
	if idx < 0 {
		return false
	}
	tail := idx + len(needle)
	if tail >= len(lineLower) {
		return true
	}
	c := lineLower[tail]
	// Продолжение идентификатора → это другая инструкция (CreateMetadata, InitializeMint, initialize2…).
	if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
		return false
	}
	return true
}

// rayLogPayloadIsInitPool — ray_log = base64(bincode); LogType::Init = 0 (program/src/log.rs).
func rayLogPayloadIsInitPool(payload string) bool {
	s := strings.TrimSpace(payload)
	s = strings.TrimLeft(s, " :\t")
	if s == "" {
		return false
	}
	raw, err := decodeBase64StdLoose(s)
	if err != nil {
		raw, err = base64.URLEncoding.DecodeString(s)
	}
	if err != nil || len(raw) < 1 {
		return false
	}
	return raw[0] == 0
}

func decodeBase64StdLoose(s string) ([]byte, error) {
	switch len(s) % 4 {
	case 2:
		s += "=="
	case 3:
		s += "="
	}
	return base64.StdEncoding.DecodeString(s)
}

// logsContainRaydiumInitialize2 — ветка Initialize2 в processor.rs (msg с подстрокой initialize2).
// Ищем без обязательного «:» — часть RPC обрезает длинные Program log.
func logsContainRaydiumInitialize2(logs []string) bool {
	for _, line := range logs {
		if strings.Contains(strings.ToLower(line), "initialize2") {
			return true
		}
	}
	return false
}

// logsLookLikeRaydiumSwap — msg! из processor.rs при свопах (не создание пула).
func logsLookLikeRaydiumSwap(logs []string) bool {
	for _, line := range logs {
		low := strings.ToLower(line)
		if strings.Contains(low, "swap_base_in") || strings.Contains(low, "swap_base_out") {
			return true
		}
	}
	return false
}

// firstRayLogPayload — первый непустой фрагмент после маркера ray_log в логах.
func firstRayLogPayload(logs []string) (payload string, ok bool) {
	const marker = "ray_log"
	for _, line := range logs {
		i := strings.Index(line, marker)
		if i < 0 {
			continue
		}
		rest := line[i+len(marker):]
		rest = strings.TrimLeft(rest, " :\t")
		if rest != "" {
			return rest, true
		}
	}
	return "", false
}

func printRayLogSample(sig string, payload string) {
	sample := payload
	if len(sample) > 50 {
		sample = sample[:50]
	}
	log.Printf("[ray_log sample] sig=%s payload_len=%d first50=%q", sig, len(payload), sample)
}

func heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			fmt.Printf("Heartbeat: Connection alive at %s | ws: mentions=%d with_ray_log=%d swap_like=%d passed_v4_init=%d passed_pump=%d passed_cpmm=%d\n",
				now.Format(time.RFC3339Nano),
				wsNotifOK.Load(), wsRayLogLine.Load(), wsSwapLike.Load(),
				wsHybridPass.Load(), wsPassedPump.Load(), wsPassedCPMM.Load())
		}
	}
}

// appendSnipeLog дописывает строку в snipes.log (потокобезопасно).
func appendSnipeLog(line string) {
	snipeLogMu.Lock()
	defer snipeLogMu.Unlock()
	f, err := os.OpenFile(snipesLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintln(f, line)
	_ = f.Close()
}

// reportSimulationBuy списывает simBetUSD + комиссию покупки, фиксирует цену входа. false — недостаточно кэша или дубликат mint.
func reportSimulationBuy(mint string, entryUSD float64) bool {
	if !IS_SIMULATION {
		return true
	}
	if entryUSD <= 0 {
		log.Printf("[SIM] skip BUY: нет цены входа (Jupiter) для %s", mint)
		return false
	}
	cost := simBetUSD + simTradeFeeBuyUSD
	simMu.Lock()
	if _, dup := simOpenEntryUSD[mint]; dup {
		simMu.Unlock()
		log.Printf("[SIM] skip BUY: позиция по %s уже открыта", mint)
		return false
	}
	if simCashUSD < cost {
		have := simCashUSD
		simMu.Unlock()
		log.Printf("[SIM] skip BUY: недостаточно виртуального баланса (есть $%.4f, нужно $%.4f) mint=%s", have, cost, mint)
		return false
	}
	simCashUSD -= cost
	simOpenEntryUSD[mint] = entryUSD
	cashAfter := simCashUSD
	simMu.Unlock()

	ts := time.Now().UTC().Format(time.RFC3339Nano)
	sol := float64(BUY_LAMPORTS) / 1e9
	fmt.Println(bannerSep)
	fmt.Printf("🟢 SIMULATION BUY: %s at %s | ставка $%.2f + fee $%.2f | cash после $%.2f | entry_usd=%.10f\n",
		mint, ts, simBetUSD, simTradeFeeBuyUSD, cashAfter, entryUSD)
	fmt.Println(bannerSep)
	appendSnipeLog(fmt.Sprintf("%s | BUY | mint=%s | bet_usd=%.4f | buy_fee_usd=%.4f | cash_after_usd=%.4f | entry_usd=%.10f | amount_sol=%.4f | pnl_pct=n/a",
		ts, mint, simBetUSD, simTradeFeeBuyUSD, cashAfter, entryUSD, sol))
	notifyNewTokenFoundWhatsApp(mint)
	return true
}

func reportSimulationSell(mint, reason string, entry, exit float64) {
	if IS_SIMULATION {
		var proceeds float64
		simMu.Lock()
		if _, had := simOpenEntryUSD[mint]; had {
			delete(simOpenEntryUSD, mint)
			if entry > 0 {
				proceeds = simBetUSD * (exit / entry)
			}
		} else if entry > 0 {
			proceeds = simBetUSD * (exit / entry)
		}
		simCashUSD += proceeds - simTradeFeeSellUSD
		if simCashUSD < 0 {
			simCashUSD = 0
		}
		simMu.Unlock()

		simClosedTrades.Add(1)
		switch reason {
		case "TAKE_PROFIT":
			simTakeProfitHits.Add(1)
		case "STOP_LOSS":
			simStopLossHits.Add(1)
		}
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)
	pnlPct := 0.0
	if entry > 0 {
		pnlPct = (exit/entry - 1) * 100
	}
	fmt.Println(bannerSep)
	fmt.Printf("🔴 SIMULATION SELL: %s at %s\n", mint, ts)
	fmt.Printf("   %s  entry=%.8f exit=%.8f  pnl=%.2f%%\n", reason, entry, exit, pnlPct)
	fmt.Println(bannerSep)
	var cashLine string
	if IS_SIMULATION {
		simMu.Lock()
		cashLine = fmt.Sprintf(" | cash_after_usd=%.4f", simCashUSD)
		simMu.Unlock()
	}
	appendSnipeLog(fmt.Sprintf("%s | SELL | mint=%s | reason=%s | entry=%.8f | exit=%.8f | pnl_pct=%.2f | sell_fee_usd=%.4f%s",
		ts, mint, reason, entry, exit, pnlPct, simTradeFeeSellUSD, cashLine))
}

func simulationMarkToMarketOpen(ctx context.Context) (cash float64, mtm float64, nOpen int) {
	simMu.Lock()
	cash = simCashUSD
	type pair struct {
		m   string
		ent float64
	}
	list := make([]pair, 0, len(simOpenEntryUSD))
	for m, e := range simOpenEntryUSD {
		list = append(list, pair{m, e})
	}
	nOpen = len(list)
	simMu.Unlock()

	for _, it := range list {
		select {
		case <-ctx.Done():
			return cash, mtm, nOpen
		default:
		}
		p, err := fetchJupiterPriceUSD(it.m)
		if err != nil || it.ent <= 0 {
			mtm += simBetUSD
			continue
		}
		mtm += simBetUSD * (p / it.ent)
	}
	return cash, mtm, nOpen
}

func simulationPortfolioLogLoop(ctx context.Context) {
	t := time.NewTicker(simPortfolioLogEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cash, mtm, n := simulationMarkToMarketOpen(context.Background())
			total := cash + mtm
			ts := time.Now().UTC().Format(time.RFC3339Nano)
			fmt.Printf("[SIM Portfolio] %s | cash=$%.2f | открытые (MTM)=$%.2f | всего=$%.2f | позиций=%d\n",
				ts, cash, mtm, total, n)
			appendSnipeLog(fmt.Sprintf("%s | PORTFOLIO | cash_usd=%.4f | open_mtm_usd=%.4f | total_usd=%.4f | open_n=%d",
				ts, cash, mtm, total, n))
		}
	}
}

func simulationExperimentTimer(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(simExperimentDuration):
	}
	simFinalReportOnce.Do(func() { printFinalSimulationReport() })
}

func printFinalSimulationReport() {
	cash, mtm, nOpen := simulationMarkToMarketOpen(context.Background())
	total := cash + mtm
	fmt.Println(bannerSep)
	fmt.Println("SIM EXPERIMENT — финальный отчёт (60 мин)")
	fmt.Printf("Total Trades: %d\n", simClosedTrades.Load())
	fmt.Printf("Successful (TP hit): %d\n", simTakeProfitHits.Load())
	fmt.Printf("Failed (SL hit): %d\n", simStopLossHits.Load())
	fmt.Printf("Final Balance: $%.2f  (cash $%.2f + открытые MTM $%.2f)\n", total, cash, mtm)
	if nOpen > 0 {
		fmt.Printf("Открытых позиций на момент отчёта: %d (учтены в Final Balance)\n", nOpen)
	}
	fmt.Println(bannerSep)
	appendSnipeLog(fmt.Sprintf("%s | SIM_FINAL | trades=%d tp=%d sl=%d total_usd=%.4f cash_usd=%.4f open_mtm_usd=%.4f open_n=%d",
		time.Now().UTC().Format(time.RFC3339Nano), simClosedTrades.Load(), simTakeProfitHits.Load(), simStopLossHits.Load(),
		total, cash, mtm, nOpen))
}

func logRejected(mintAddr, reason string) {
	if mintAddr == "" {
		mintAddr = "(unknown)"
	}
	log.Printf("[REJECTED] Mint: %s Reason: %s", mintAddr, reason)
}

func publicKeyFromEnvOrDefault(envKey, defaultBase58 string) solana.PublicKey {
	s := strings.TrimSpace(os.Getenv(envKey))
	if s == "" {
		return solana.MustPublicKeyFromBase58(defaultBase58)
	}
	pk, err := solana.PublicKeyFromBase58(s)
	if err != nil {
		log.Fatalf("%s: invalid base58 pubkey: %v", envKey, err)
	}
	return pk
}

func refSolUSD() float64 {
	s := strings.TrimSpace(os.Getenv("REF_SOL_USD"))
	if s == "" {
		return defaultRefSOLUSD
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		return defaultRefSOLUSD
	}
	return v
}

func minLiquidityUSDFromEnv() float64 {
	s := strings.TrimSpace(os.Getenv("MIN_LIQUIDITY_USD"))
	if s == "" {
		return defaultMinLiquidityUSD
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		return defaultMinLiquidityUSD
	}
	return v
}

// knownStableOrSOLUSD — оценка ноги пула в USD, если mint WSOL / USDC / USDT; иначе 0.
func knownStableOrSOLUSD(mint solana.PublicKey, raw uint64, solPrice float64) float64 {
	if mint.Equals(wsolMint) {
		return float64(raw) / 1e9 * solPrice
	}
	if mint.Equals(usdcMint) || mint.Equals(usdtMint) {
		return float64(raw) / 1e6
	}
	return 0
}

// poolLiquidityCheck — если хотя бы одна нога оценивается в USD и сумма < порога — отклонить.
func poolLiquidityCheck(keys *RaydiumPoolKeys, baseBal, quoteBal uint64) (ok bool, reason string) {
	solp := refSolUSD()
	qUSD := knownStableOrSOLUSD(keys.QuoteMint, quoteBal, solp)
	bUSD := knownStableOrSOLUSD(keys.BaseMint, baseBal, solp)
	visible := qUSD + bUSD
	minU := minLiquidityUSDFromEnv()
	if visible <= 0 {
		// Обе стороны «неизвестны» для грубой оценки — не режем.
		return true, ""
	}
	if visible < minU {
		return false, fmt.Sprintf("Liquidity too low ($%.2fk, min ~$%.1fk @ REF_SOL_USD=%.0f)", visible/1000, minU/1000, solp)
	}
	return true, ""
}

// getTransactionConfirmedRetry: лог приходит почти сразу (processed), а getTransaction с commitment=confirmed
// часто отвечает not found, пока слот не подтверждён — несколько попыток с паузой.
func getTransactionConfirmedRetry(ctx context.Context, rpcClient *rpc.Client, sig solana.Signature) (*rpc.GetTransactionResult, error) {
	v := uint64(0)
	opts := &rpc.GetTransactionOpts{
		Encoding:                       solana.EncodingBase64,
		Commitment:                     rpc.CommitmentConfirmed,
		MaxSupportedTransactionVersion: &v,
	}
	const maxAttempts = 24
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		out, err := rpcClient.GetTransaction(ctx, sig, opts)
		if err == nil && out != nil && out.Meta != nil {
			return out, nil
		}
		if err != nil {
			lastErr = err
			if !errors.Is(err, rpc.ErrNotFound) {
				return nil, err
			}
		} else {
			lastErr = rpc.ErrNotFound
		}
		if attempt+1 == maxAttempts {
			break
		}
		delay := 100 * time.Millisecond
		if attempt > 8 {
			delay = 200 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, lastErr
}

// handleRaydiumLogNotification загружает транзакцию по сигнатуре и пытается извлечь Initialize2.
func handleRaydiumLogNotification(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, sig solana.Signature) {
	sigStr := sig.String()
	out, err := getTransactionConfirmedRetry(ctx, rpcClient, sig)
	if err != nil {
		logRejected("", fmt.Sprintf("getTransaction failed sig=%s: %v", sigStr, err))
		return
	}
	if out.Meta.Err != nil {
		logRejected("", fmt.Sprintf("tx on-chain failed sig=%s: %v", sigStr, out.Meta.Err))
		return
	}

	tx, err := out.Transaction.GetTransaction()
	if err != nil {
		logRejected("", fmt.Sprintf("decode transaction sig=%s: %v", sigStr, err))
		return
	}

	keys := fullAccountKeys(tx.Message.AccountKeys, out.Meta)
	init, ok := findInitialize2InTransaction(tx, out.Meta, keys)
	if !ok {
		logRejected("", fmt.Sprintf("no Raydium Initialize2 in tx sig=%s", sigStr))
		return
	}

	targetMint := pickNonBaseQuoteMint(init.CoinMint, init.PcMint)

	if ok, why := passesMintSecurity(ctx, rpcClient, targetMint); !ok {
		logRejected(targetMint.String(), fmt.Sprintf("risk-check: %s", why))
		return
	}

	poolKeys, err := fetchRaydiumPoolKeys(ctx, rpcClient, init.AMM)
	if err != nil {
		logRejected(targetMint.String(), fmt.Sprintf("pool/keys fetch: %v", err))
		return
	}
	if err := validatePoolMintsAgainstOnChain(poolKeys, init.CoinMint, init.PcMint); err != nil {
		logRejected(targetMint.String(), fmt.Sprintf("mint layout mismatch: %v", err))
		return
	}

	baseBal, quoteBal, err := fetchVaultBalances(ctx, rpcClient, poolKeys.BaseVault, poolKeys.QuoteVault)
	if err != nil {
		logRejected(targetMint.String(), fmt.Sprintf("vault balances: %v", err))
		return
	}
	if liqOK, why := poolLiquidityCheck(poolKeys, baseBal, quoteBal); !liqOK {
		logRejected(targetMint.String(), why)
		return
	}

	poolRegistry.Store(targetMint.String(), poolKeys)

	if IS_SIMULATION {
		entry, err := fetchJupiterPriceUSD(targetMint.String())
		if err != nil {
			return
		}
		if !reportSimulationBuy(targetMint.String(), entry) {
			return
		}
		startExitTracker(targetMint.String(), entry)
		return
	}
	if err := SwapToken(ctx, rpcClient, wallet, targetMint.String(), BUY_LAMPORTS); err != nil {
		log.Printf("swap error: %v", err)
		return
	}
	notifyNewTokenFoundWhatsApp(targetMint.String())
	entry, err := fetchJupiterPriceUSD(targetMint.String())
	if err != nil {
		return
	}
	startExitTracker(targetMint.String(), entry)
}

// handlePumpCreateNotification — mint из Anchor create (первый аккаунт); без Raydium V4 пула — только симуляция / Jupiter.
func handlePumpCreateNotification(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, sig solana.Signature) {
	sigStr := sig.String()
	out, err := getTransactionConfirmedRetry(ctx, rpcClient, sig)
	if err != nil {
		logRejected("", fmt.Sprintf("[PUMP] getTransaction failed sig=%s: %v", sigStr, err))
		return
	}
	if out.Meta == nil {
		logRejected("", fmt.Sprintf("[PUMP] missing meta sig=%s", sigStr))
		return
	}
	if out.Meta.Err != nil {
		logRejected("", fmt.Sprintf("[PUMP] tx failed sig=%s: %v", sigStr, out.Meta.Err))
		return
	}
	tx, err := out.Transaction.GetTransaction()
	if err != nil {
		logRejected("", fmt.Sprintf("[PUMP] decode tx sig=%s: %v", sigStr, err))
		return
	}
	keys := fullAccountKeys(tx.Message.AccountKeys, out.Meta)
	mint, ok := findPumpCreateMintInTransaction(tx, out.Meta, keys)
	if !ok {
		logRejected("", fmt.Sprintf("[PUMP] no create ix sig=%s", sigStr))
		return
	}

	if ok, why := passesMintSecurity(ctx, rpcClient, mint); !ok {
		logRejected(mint.String(), fmt.Sprintf("[PUMP] risk-check: %s", why))
		return
	}

	if IS_SIMULATION {
		entry, err := fetchJupiterPriceUSD(mint.String())
		if err != nil {
			return
		}
		if !reportSimulationBuy(mint.String(), entry) {
			return
		}
		startExitTracker(mint.String(), entry)
		return
	}
	log.Printf("[PUMP] live swap не реализован для bonding curve; mint=%s", mint)
}

// handleCPMMInitializeNotification — новый CP-пул; «целевой» mint как у V4 (не WSOL/USDC/USDT).
func handleCPMMInitializeNotification(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, sig solana.Signature) {
	sigStr := sig.String()
	out, err := getTransactionConfirmedRetry(ctx, rpcClient, sig)
	if err != nil {
		logRejected("", fmt.Sprintf("[CPMM] getTransaction failed sig=%s: %v", sigStr, err))
		return
	}
	if out.Meta == nil {
		logRejected("", fmt.Sprintf("[CPMM] missing meta sig=%s", sigStr))
		return
	}
	if out.Meta.Err != nil {
		logRejected("", fmt.Sprintf("[CPMM] tx failed sig=%s: %v", sigStr, out.Meta.Err))
		return
	}
	tx, err := out.Transaction.GetTransaction()
	if err != nil {
		logRejected("", fmt.Sprintf("[CPMM] decode tx sig=%s: %v", sigStr, err))
		return
	}
	keys := fullAccountKeys(tx.Message.AccountKeys, out.Meta)
	t0, t1, ok := findCPMMInitializeMintsInTransaction(tx, out.Meta, keys)
	if !ok {
		logRejected("", fmt.Sprintf("[CPMM] no initialize ix sig=%s", sigStr))
		return
	}
	targetMint := pickNonBaseQuoteMint(t0, t1)

	if ok, why := passesMintSecurity(ctx, rpcClient, targetMint); !ok {
		logRejected(targetMint.String(), fmt.Sprintf("[CPMM] risk-check: %s", why))
		return
	}

	if IS_SIMULATION {
		entry, err := fetchJupiterPriceUSD(targetMint.String())
		if err != nil {
			return
		}
		if !reportSimulationBuy(targetMint.String(), entry) {
			return
		}
		startExitTracker(targetMint.String(), entry)
		return
	}
	log.Printf("[CPMM] live swap не реализован для CP-Swap; mint=%s", targetMint)
}

// ---------- 2) Парсинг Initialize2 + выбор mint ----------

// InitPoolAccounts — аккаунты инструкции Initialize2 / create pool по порядку Raydium SDK:
// индекс 4 = AMM, 8 = coin mint, 9 = pc mint.
// Важно: адреса Base/Quote mint лежат в метах аккаунтов инструкции, а не в data.
// В data — nonce (u8), open_time (u64), объёмы pc/coin (u64×2) либо Anchor-дискриминатор + те же поля.
type InitPoolAccounts struct {
	AMM      solana.PublicKey
	CoinMint solana.PublicKey
	PcMint   solana.PublicKey
}

func fullAccountKeys(static solana.PublicKeySlice, meta *rpc.TransactionMeta) solana.PublicKeySlice {
	out := make(solana.PublicKeySlice, 0, len(static)+len(meta.LoadedAddresses.Writable)+len(meta.LoadedAddresses.ReadOnly))
	out = append(out, static...)
	out = append(out, meta.LoadedAddresses.Writable...)
	out = append(out, meta.LoadedAddresses.ReadOnly...)
	return out
}

func findInitialize2InTransaction(tx *solana.Transaction, meta *rpc.TransactionMeta, keys solana.PublicKeySlice) (InitPoolAccounts, bool) {
	inspect := func(programIDIndex uint16, accounts []uint16, data []byte) (InitPoolAccounts, bool) {
		if int(programIDIndex) >= len(keys) {
			return InitPoolAccounts{}, false
		}
		if !keys[programIDIndex].Equals(raydiumAMMProgram) {
			return InitPoolAccounts{}, false
		}
		if !isRaydiumInitialize2Data(data) {
			return InitPoolAccounts{}, false
		}
		for _, i := range []int{4, 8, 9} {
			if i >= len(accounts) {
				return InitPoolAccounts{}, false
			}
		}
		idxAMM, idxCoin, idxPc := accounts[4], accounts[8], accounts[9]
		if int(idxAMM) >= len(keys) || int(idxCoin) >= len(keys) || int(idxPc) >= len(keys) {
			return InitPoolAccounts{}, false
		}
		return InitPoolAccounts{
			AMM:      keys[idxAMM],
			CoinMint: keys[idxCoin],
			PcMint:   keys[idxPc],
		}, true
	}

	for _, ci := range tx.Message.Instructions {
		if p, ok := inspect(ci.ProgramIDIndex, ci.Accounts, []byte(ci.Data)); ok {
			return p, true
		}
	}
	for _, block := range meta.InnerInstructions {
		for _, ci := range block.Instructions {
			if p, ok := inspect(ci.ProgramIDIndex, ci.Accounts, []byte(ci.Data)); ok {
				return p, true
			}
		}
	}
	return InitPoolAccounts{}, false
}

// findPumpCreateMintInTransaction — Pump IDL: create, accounts[0] = mint (signer).
func findPumpCreateMintInTransaction(tx *solana.Transaction, meta *rpc.TransactionMeta, keys solana.PublicKeySlice) (solana.PublicKey, bool) {
	return findAnchorIxMintAtAccountIndex(pumpFunProgram, pumpCreateDisc, tx, meta, keys, 0)
}

// findCPMMInitializeMintsInTransaction — raydium_cp_swap IDL initialize: token_0_mint / token_1_mint @ индексы 4 и 5.
func findCPMMInitializeMintsInTransaction(tx *solana.Transaction, meta *rpc.TransactionMeta, keys solana.PublicKeySlice) (solana.PublicKey, solana.PublicKey, bool) {
	if meta == nil {
		return solana.PublicKey{}, solana.PublicKey{}, false
	}
	inspect := func(programIDIndex uint16, accounts []uint16, data []byte) (solana.PublicKey, solana.PublicKey, bool) {
		if int(programIDIndex) >= len(keys) || !keys[programIDIndex].Equals(raydiumCPMMProgram) {
			return solana.PublicKey{}, solana.PublicKey{}, false
		}
		if len(data) < 8 || !bytes.Equal(data[:8], cpmmInitializeAnchor) {
			return solana.PublicKey{}, solana.PublicKey{}, false
		}
		if len(accounts) < 6 {
			return solana.PublicKey{}, solana.PublicKey{}, false
		}
		i0, i1 := accounts[4], accounts[5]
		if int(i0) >= len(keys) || int(i1) >= len(keys) {
			return solana.PublicKey{}, solana.PublicKey{}, false
		}
		return keys[i0], keys[i1], true
	}

	for _, ci := range tx.Message.Instructions {
		if a, b, ok := inspect(ci.ProgramIDIndex, ci.Accounts, []byte(ci.Data)); ok {
			return a, b, true
		}
	}
	for _, block := range meta.InnerInstructions {
		for _, ci := range block.Instructions {
			if a, b, ok := inspect(ci.ProgramIDIndex, ci.Accounts, []byte(ci.Data)); ok {
				return a, b, true
			}
		}
	}
	return solana.PublicKey{}, solana.PublicKey{}, false
}

func findAnchorIxMintAtAccountIndex(program solana.PublicKey, disc []byte, tx *solana.Transaction, meta *rpc.TransactionMeta, keys solana.PublicKeySlice, accountIdx int) (solana.PublicKey, bool) {
	if meta == nil {
		return solana.PublicKey{}, false
	}
	inspect := func(programIDIndex uint16, accounts []uint16, data []byte) (solana.PublicKey, bool) {
		if int(programIDIndex) >= len(keys) || !keys[programIDIndex].Equals(program) {
			return solana.PublicKey{}, false
		}
		if len(data) < 8 || !bytes.Equal(data[:8], disc) {
			return solana.PublicKey{}, false
		}
		if accountIdx >= len(accounts) {
			return solana.PublicKey{}, false
		}
		idx := accounts[accountIdx]
		if int(idx) >= len(keys) {
			return solana.PublicKey{}, false
		}
		return keys[idx], true
	}
	for _, ci := range tx.Message.Instructions {
		if pk, ok := inspect(ci.ProgramIDIndex, ci.Accounts, []byte(ci.Data)); ok {
			return pk, true
		}
	}
	for _, block := range meta.InnerInstructions {
		for _, ci := range block.Instructions {
			if pk, ok := inspect(ci.ProgramIDIndex, ci.Accounts, []byte(ci.Data)); ok {
				return pk, true
			}
		}
	}
	return solana.PublicKey{}, false
}

func isRaydiumInitialize2Data(data []byte) bool {
	if len(data) >= 26 && data[0] == raydiumInstructionInitialize2 {
		return true
	}
	if len(data) >= 8+25 && bytes.Equal(data[:8], initialize2AnchorDisc) {
		return true
	}
	return false
}

func anchorDiscriminator(name string) []byte {
	h := sha256.Sum256([]byte(name))
	return h[:8]
}

func pickNonBaseQuoteMint(coin, pc solana.PublicKey) solana.PublicKey {
	// Обычно одна сторона — WSOL/USDC/USDT, вторая — новый токен.
	if coin.Equals(wsolMint) || coin.Equals(usdcMint) || coin.Equals(usdtMint) {
		return pc
	}
	if pc.Equals(wsolMint) || pc.Equals(usdcMint) || pc.Equals(usdtMint) {
		return coin
	}
	// Если обе «альткоины», снайпер по умолчанию берёт coin как базовый актив пула.
	return coin
}

// ---------- 3) Rug-check: Mint account (mint authority & freeze authority) ----------

// passesMintSecurity возвращает true, если mint authority и freeze authority выключены (COption = None).
func passesMintSecurity(ctx context.Context, c *rpc.Client, mint solana.PublicKey) (bool, string) {
	const maxAttempts = 5
	mintAccountMissing := func(acc *rpc.GetAccountInfoResult, err error) bool {
		return err != nil || acc == nil || acc.Value == nil || acc.Value.Data == nil
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		acc, err := c.GetAccountInfo(ctx, mint)
		if !mintAccountMissing(acc, err) {
			data := acc.Value.Data.GetBinary()
			if len(data) < 82 {
				return false, "unexpected mint data length"
			}
			// SPL Mint layout: mint_authority COption (u32 tag + 32 bytes) @0, freeze @46
			if binary.LittleEndian.Uint32(data[0:4]) != 0 {
				return false, "mint authority is NOT null (can mint more supply)"
			}
			if binary.LittleEndian.Uint32(data[46:50]) != 0 {
				return false, "freeze authority is NOT null"
			}
			return true, ""
		}
		if attempt+1 == maxAttempts {
			break
		}
		fmt.Printf("Account not found, retrying... (%d/5)\n", attempt+1)
		select {
		case <-ctx.Done():
			return false, ctx.Err().Error()
		default:
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false, "mint account not found"
}

// ---------- 4) Сбор ключей пула + рынка для swap_base_in ----------

// validatePoolMintsAgainstOnChain сверяет coin/pc из Initialize2 с baseMint/quoteMint из аккаунта AMM (источник истины).
func validatePoolMintsAgainstOnChain(keys *RaydiumPoolKeys, coin, pc solana.PublicKey) error {
	coinOK := keys.BaseMint.Equals(coin) || keys.QuoteMint.Equals(coin)
	pcOK := keys.BaseMint.Equals(pc) || keys.QuoteMint.Equals(pc)
	if !coinOK || !pcOK {
		return fmt.Errorf("coin/pc not both present in pool mints (base=%s quote=%s)", keys.BaseMint, keys.QuoteMint)
	}
	if keys.BaseMint.Equals(keys.QuoteMint) {
		return fmt.Errorf("base and quote mint are identical")
	}
	return nil
}

func fetchRaydiumPoolKeys(ctx context.Context, c *rpc.Client, amm solana.PublicKey) (*RaydiumPoolKeys, error) {
	info, err := c.GetAccountInfo(ctx, amm)
	if err != nil || info == nil || info.Value == nil || info.Value.Data == nil {
		return nil, fmt.Errorf("get AMM account")
	}
	raw := info.Value.Data.GetBinary()
	if len(raw) < liqV4BaseVaultOffset+32*9+8 {
		return nil, fmt.Errorf("AMM data too short")
	}
	readPK := func(off int) solana.PublicKey {
		var pk solana.PublicKey
		copy(pk[:], raw[off:off+32])
		return pk
	}
	baseVault := readPK(liqV4BaseVaultOffset)
	quoteVault := readPK(liqV4BaseVaultOffset + 32)
	baseMint := readPK(liqV4BaseVaultOffset + 64)
	quoteMint := readPK(liqV4BaseVaultOffset + 96)
	openOrders := readPK(liqV4BaseVaultOffset + 160)
	marketID := readPK(liqV4BaseVaultOffset + 192)
	marketProgram := readPK(liqV4BaseVaultOffset + 224)
	targetOrders := readPK(liqV4BaseVaultOffset + 256)

	marketInfo, err := c.GetAccountInfo(ctx, marketID)
	if err != nil || marketInfo == nil || marketInfo.Value == nil || marketInfo.Value.Data == nil {
		return nil, fmt.Errorf("get market account")
	}
	md := marketInfo.Value.Data.GetBinary()
	if len(md) < mktOffsetAsks+32 {
		return nil, fmt.Errorf("market data too short")
	}
	nonce := binary.LittleEndian.Uint64(md[mktOffsetVaultSignerNonce : mktOffsetVaultSignerNonce+8])
	marketAuthority, _, err := solana.FindProgramAddress(
		[][]byte{marketID.Bytes(), u64LEBytes(nonce)},
		marketProgram,
	)
	if err != nil {
		return nil, err
	}

	return &RaydiumPoolKeys{
		AMM:              amm,
		Authority:        deriveAMMAuthority(amm),
		OpenOrders:       openOrders,
		TargetOrders:     targetOrders,
		BaseVault:        baseVault,
		QuoteVault:       quoteVault,
		BaseMint:         baseMint,
		QuoteMint:        quoteMint,
		MarketProgramID:  marketProgram,
		MarketID:         marketID,
		MarketBids:       readPKAt(md, mktOffsetBids),
		MarketAsks:       readPKAt(md, mktOffsetAsks),
		MarketEventQueue: readPKAt(md, mktOffsetEventQueue),
		MarketBaseVault:  readPKAt(md, mktOffsetBaseVault),
		MarketQuoteVault: readPKAt(md, mktOffsetQuoteVault),
		MarketAuthority:  marketAuthority,
	}, nil
}

func readPKAt(buf []byte, off int) solana.PublicKey {
	var pk solana.PublicKey
	copy(pk[:], buf[off:off+32])
	return pk
}

func u64LEBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

// deriveAMMAuthority — PDA authority Raydium для конкретного AMM (как в SDK: seed = amm id).
func deriveAMMAuthority(amm solana.PublicKey) solana.PublicKey {
	pda, _, err := solana.FindProgramAddress([][]byte{amm.Bytes()}, raydiumAMMProgram)
	if err != nil {
		return solana.PublicKey{}
	}
	return pda
}

// ---------- 5) Swap: Raydium swap_base_in + priority fee + WSOL ----------

// SwapToken ищет пул в реестре по mint и исполняет swap (или симулирует).
func SwapToken(ctx context.Context, rpcClient *rpc.Client, wallet solana.PrivateKey, mintAddress string, amount uint64) error {
	v, ok := poolRegistry.Load(mintAddress)
	if !ok {
		return fmt.Errorf("no pool in registry for mint %s (wait for initialize2)", mintAddress)
	}
	keys := v.(*RaydiumPoolKeys)
	mint := solana.MustPublicKeyFromBase58(mintAddress)

	if IS_SIMULATION {
		entry, err := fetchJupiterPriceUSD(mintAddress)
		if err != nil {
			return err
		}
		if !reportSimulationBuy(mintAddress, entry) {
			return nil
		}
		startExitTracker(mintAddress, entry)
		return nil
	}

	owner := wallet.PublicKey()
	userQuoteAta, _, err := solana.FindAssociatedTokenAddress(owner, keys.QuoteMint)
	if err != nil {
		return err
	}
	userBaseAta, _, err := solana.FindAssociatedTokenAddress(owner, keys.BaseMint)
	if err != nil {
		return err
	}

	// Покупаем base за quote (WSOL/USDC на стороне quote): quote → base.
	if !mint.Equals(keys.BaseMint) {
		return fmt.Errorf("mint %s is not pool base mint", mintAddress)
	}

	baseBal, quoteBal, err := fetchVaultBalances(ctx, rpcClient, keys.BaseVault, keys.QuoteVault)
	if err != nil {
		return err
	}
	expectedBaseOut := quoteToBaseOut(amount, baseBal, quoteBal)
	minOut := applySlippage(expectedBaseOut, SlippageBps)

	swapIx, err := buildRaydiumSwapBaseIn(keys, userQuoteAta, userBaseAta, owner, amount, minOut)
	if err != nil {
		return err
	}

	cuLimitIx, err := computebudget.NewSetComputeUnitLimitInstruction(ComputeUnitLimit).ValidateAndBuild()
	if err != nil {
		return err
	}
	microPerCU := effectiveMicroLamportsPerCU()
	cuPriceIx, err := computebudget.NewSetComputeUnitPriceInstruction(microPerCU).ValidateAndBuild()
	if err != nil {
		return err
	}

	var preIxs []solana.Instruction

	// 1) ATA для входа (quote) и выхода (base) в той же транзакции, что и swap.
	if _, err := ensureATAInstruction(ctx, rpcClient, &preIxs, owner, keys.QuoteMint); err != nil {
		return fmt.Errorf("quote ATA: %w", err)
	}
	if _, err := ensureATAInstruction(ctx, rpcClient, &preIxs, owner, keys.BaseMint); err != nil {
		return fmt.Errorf("base ATA: %w", err)
	}

	// 2) WSOL: если quote = wrapped SOL, добираем баланс с нативного SOL (system.Transfer + spltoken.SyncNative).
	if keys.QuoteMint.Equals(wsolMint) {
		if err := appendWSOLWrapInstructions(ctx, rpcClient, owner, userQuoteAta, amount, &preIxs); err != nil {
			return err
		}
	} else {
		if err := requireSufficientQuoteBalance(ctx, rpcClient, userQuoteAta, amount, keys.QuoteMint); err != nil {
			return err
		}
	}

	recent, err := rpcClient.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return err
	}

	all := append([]solana.Instruction{}, cuLimitIx, cuPriceIx)
	all = append(all, preIxs...)
	all = append(all, swapIx)

	tx, err := solana.NewTransaction(all, recent.Value.Blockhash, solana.TransactionPayer(owner))
	if err != nil {
		return err
	}
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(wallet.PublicKey()) {
			return &wallet
		}
		return nil
	})
	if err != nil {
		return err
	}

	sig, err := sendSwapTransaction(ctx, rpcClient, tx)
	if err != nil {
		return err
	}
	log.Printf("submitted swap tx: %s", sig.String())
	return nil
}

// effectiveMicroLamportsPerCU — не ниже MinMicroLamportsPerCU и при желании выше из PriorityFeeLamports.
func effectiveMicroLamportsPerCU() uint64 {
	calc := PriorityFeeLamports * 1_000_000 / uint64(ComputeUnitLimit)
	if calc < MinMicroLamportsPerCU {
		return MinMicroLamportsPerCU
	}
	return calc
}

// appendWSOLWrapInstructions перечисляет lamports на ATA WSOL и вызывает SyncNative, если в ATA не хватает quote для swap.
func appendWSOLWrapInstructions(ctx context.Context, c *rpc.Client, owner, wsolATA solana.PublicKey, needRaw uint64, out *[]solana.Instruction) error {
	have, err := splTokenAccountRawAmount(ctx, c, wsolATA)
	if err != nil {
		return err
	}
	if have >= needRaw {
		return nil
	}
	delta := needRaw - have
	bal, err := c.GetBalance(ctx, owner, rpc.CommitmentProcessed)
	if err != nil || bal == nil {
		return fmt.Errorf("get native SOL balance: %w", err)
	}
	// Грубый запас под rent/fee (ATA уже создана в preIxs).
	const feeHeadroom uint64 = 2_000_000
	if bal.Value < delta+feeHeadroom {
		return fmt.Errorf("insufficient native SOL: need at least %d lamports for wrap (+fees), have %d", delta+feeHeadroom, bal.Value)
	}
	tfer, err := system.NewTransferInstruction(delta, owner, wsolATA).ValidateAndBuild()
	if err != nil {
		return err
	}
	syncIx, err := spltoken.NewSyncNativeInstruction(wsolATA).ValidateAndBuild()
	if err != nil {
		return err
	}
	*out = append(*out, tfer, syncIx)
	log.Printf("WSOL: wrapping %d lamports into %s (had %d, need %d)", delta, wsolATA.String(), have, needRaw)
	return nil
}

func requireSufficientQuoteBalance(ctx context.Context, c *rpc.Client, quoteATA solana.PublicKey, need uint64, quoteMint solana.PublicKey) error {
	have, err := splTokenAccountRawAmount(ctx, c, quoteATA)
	if err != nil {
		return err
	}
	if have < need {
		return fmt.Errorf("insufficient quote token in ATA %s (mint %s): have %d need %d — пополни ATA вручную",
			quoteATA.String(), quoteMint.String(), have, need)
	}
	return nil
}

func splTokenAccountRawAmount(ctx context.Context, c *rpc.Client, ata solana.PublicKey) (uint64, error) {
	res, err := c.GetTokenAccountBalance(ctx, ata, rpc.CommitmentProcessed)
	if err != nil || res == nil || res.Value == nil {
		return 0, nil
	}
	return strconv.ParseUint(res.Value.Amount, 10, 64)
}

// sendSwapTransaction — обычный RPC; Jito bundle в этом репозитории не реализован (см. комментарий).
func sendSwapTransaction(ctx context.Context, rpcClient *rpc.Client, tx *solana.Transaction) (solana.Signature, error) {
	if j := strings.TrimSpace(os.Getenv("JITO_BLOCK_ENGINE_URL")); j != "" {
		log.Printf("JITO_BLOCK_ENGINE_URL задан — транзакция всё равно уходит через RPC. " +
			"Атомарный bundle (tip + порядок) против sandwich требует Jito Block Engine API: https://docs.jito.wtf/ — интеграцию добавь отдельно.")
	}
	return rpcClient.SendTransactionWithOpts(ctx, tx, rpc.TransactionOpts{
		SkipPreflight:       false,
		PreflightCommitment: rpc.CommitmentProcessed,
	})
}

func ensureATAInstruction(ctx context.Context, c *rpc.Client, out *[]solana.Instruction, owner, mint solana.PublicKey) (solana.PublicKey, error) {
	ata, _, err := solana.FindAssociatedTokenAddress(owner, mint)
	if err != nil {
		return solana.PublicKey{}, err
	}
	info, err := c.GetAccountInfo(ctx, ata)
	if err == nil && info != nil && info.Value != nil {
		return ata, nil
	}
	create, err := associatedtokenaccount.NewCreateInstruction(owner, owner, mint).ValidateAndBuild()
	if err != nil {
		return solana.PublicKey{}, err
	}
	*out = append(*out, create)
	return ata, nil
}

func fetchVaultBalances(ctx context.Context, c *rpc.Client, baseVault, quoteVault solana.PublicKey) (uint64, uint64, error) {
	b, err := c.GetTokenAccountBalance(ctx, baseVault, rpc.CommitmentProcessed)
	if err != nil || b == nil || b.Value == nil {
		return 0, 0, fmt.Errorf("base vault balance: %w", err)
	}
	q, err := c.GetTokenAccountBalance(ctx, quoteVault, rpc.CommitmentProcessed)
	if err != nil || q == nil || q.Value == nil {
		return 0, 0, fmt.Errorf("quote vault balance: %w", err)
	}
	baseAmt, err := strconv.ParseUint(b.Value.Amount, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	quoteAmt, err := strconv.ParseUint(q.Value.Amount, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return baseAmt, quoteAmt, nil
}

// quoteToBaseOut — constant product x*y=k без учёта комиссий (грубая оценка для min_out).
func quoteToBaseOut(quoteIn, baseReserve, quoteReserve uint64) uint64 {
	if quoteReserve == 0 || baseReserve == 0 || quoteIn == 0 {
		return 0
	}
	var br, qr, qi, num, den big.Int
	br.SetUint64(baseReserve)
	qr.SetUint64(quoteReserve)
	qi.SetUint64(quoteIn)
	num.Mul(&br, &qi)
	den.Add(&qr, &qi)
	if den.Sign() == 0 {
		return 0
	}
	num.Div(&num, &den)
	if !num.IsUint64() {
		return 0
	}
	return num.Uint64()
}

func applySlippage(amount uint64, slippageBps uint64) uint64 {
	if amount == 0 {
		return 0
	}
	return amount * (10_000 - slippageBps) / 10_000
}

func buildRaydiumSwapBaseIn(
	keys *RaydiumPoolKeys,
	userSource, userDest, owner solana.PublicKey,
	amountIn, minOut uint64,
) (solana.Instruction, error) {
	data := make([]byte, 17)
	data[0] = raydiumSwapBaseIn
	binary.LittleEndian.PutUint64(data[1:9], amountIn)
	binary.LittleEndian.PutUint64(data[9:17], minOut)

	metas := []*solana.AccountMeta{
		{PublicKey: solana.TokenProgramID, IsSigner: false, IsWritable: false},
		{PublicKey: keys.AMM, IsSigner: false, IsWritable: true},
		{PublicKey: keys.Authority, IsSigner: false, IsWritable: false},
		{PublicKey: keys.OpenOrders, IsSigner: false, IsWritable: true},
		{PublicKey: keys.TargetOrders, IsSigner: false, IsWritable: true},
		{PublicKey: keys.BaseVault, IsSigner: false, IsWritable: true},
		{PublicKey: keys.QuoteVault, IsSigner: false, IsWritable: true},
		{PublicKey: keys.MarketProgramID, IsSigner: false, IsWritable: false},
		{PublicKey: keys.MarketID, IsSigner: false, IsWritable: true},
		{PublicKey: keys.MarketBids, IsSigner: false, IsWritable: true},
		{PublicKey: keys.MarketAsks, IsSigner: false, IsWritable: true},
		{PublicKey: keys.MarketEventQueue, IsSigner: false, IsWritable: true},
		{PublicKey: keys.MarketBaseVault, IsSigner: false, IsWritable: true},
		{PublicKey: keys.MarketQuoteVault, IsSigner: false, IsWritable: true},
		{PublicKey: keys.MarketAuthority, IsSigner: false, IsWritable: false},
		{PublicKey: userSource, IsSigner: false, IsWritable: true},
		{PublicKey: userDest, IsSigner: false, IsWritable: true},
		{PublicKey: owner, IsSigner: true, IsWritable: false},
	}
	return solana.NewInstruction(raydiumAMMProgram, metas, data), nil
}

// ---------- 6) Выход по цене (Jupiter) ----------

func fetchJupiterPriceUSD(mint string) (float64, error) {
	client := &http.Client{Timeout: 8 * time.Second}
	try := func(base string) (float64, error) {
		resp, err := client.Get(base + mint)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			return 0, fmt.Errorf("http %d: %s", resp.StatusCode, string(body))
		}
		return parseJupiterPriceJSON(body, mint)
	}
	p, err := try(jupiterPricePrimary)
	if err == nil {
		return p, nil
	}
	return try(jupiterPriceFallback)
}

func parseJupiterPriceJSON(body []byte, mint string) (float64, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		return 0, err
	}
	// Jupiter v2: { "data": { "MINT": { "price": "..." } } }
	if raw, ok := root["data"]; ok {
		var data map[string]json.RawMessage
		if err := json.Unmarshal(raw, &data); err == nil {
			if entry, ok := data[mint]; ok {
				var wrap struct {
					Price json.RawMessage `json:"price"`
				}
				if json.Unmarshal(entry, &wrap) == nil {
					if p, ok := parseJSONFloat(wrap.Price); ok {
						return p, nil
					}
				}
			}
		}
	}
	// lite-api v3: { "MINT": { "usdPrice": 1.23 } } или вложенный объект
	if raw, ok := root[mint]; ok {
		var wrap struct {
			USDPrice float64 `json:"usdPrice"`
			Price    float64 `json:"price"`
		}
		if json.Unmarshal(raw, &wrap) == nil {
			if wrap.USDPrice > 0 {
				return wrap.USDPrice, nil
			}
			if wrap.Price > 0 {
				return wrap.Price, nil
			}
		}
	}
	return 0, fmt.Errorf("price not found for mint in response")
}

func parseJSONFloat(raw json.RawMessage) (float64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil && f > 0 {
		return f, true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if f, err := strconv.ParseFloat(s, 64); err == nil && f > 0 {
			return f, true
		}
	}
	return 0, false
}

func startExitTracker(mint string, entry float64) {
	go func() {
		tick := func() bool {
			p, err := fetchJupiterPriceUSD(mint)
			if err != nil {
				return false
			}
			if p >= entry*takeProfitMultiplier {
				reportSimulationSell(mint, "TAKE_PROFIT", entry, p)
				return true
			}
			if p <= entry*stopLossMultiplier {
				reportSimulationSell(mint, "STOP_LOSS", entry, p)
				return true
			}
			return false
		}
		if tick() {
			return
		}
		t := time.NewTicker(pricePollInterval)
		defer t.Stop()
		for range t.C {
			if tick() {
				return
			}
		}
	}()
}

// ---------- WhatsApp (whatsmeow) — в main.go, чтобы работал `go run main.go` ----------

// InitWhatsApp подключает клиент whatsmeow: сессия в session.db (SQLite).
// Если сессии нет (Store.ID == nil) — один раз показывает QR в терминале.
// WHATSAPP_PHONE_E164 — номер получателя в формате E.164 без «+».
func InitWhatsApp(ctx context.Context) error {
	raw := strings.TrimSpace(os.Getenv("WHATSAPP_PHONE_E164"))
	if raw == "" {
		return nil
	}
	digits := digitsOnlyE164(raw)
	if len(digits) < 10 {
		return fmt.Errorf("WHATSAPP_PHONE_E164 invalid (need digits, e.g. 79991234567)")
	}
	waRecipient = waptypes.NewJID(digits, waptypes.DefaultUserServer)
	waConfigured = true

	container, err := sqlstore.New(ctx, whatsappSQLDialect, whatsappSessionDB, waLog.Noop)
	if err != nil {
		return fmt.Errorf("whatsapp sqlstore: %w", err)
	}

	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		_ = container.Close()
		return fmt.Errorf("whatsapp GetFirstDevice: %w", err)
	}

	cli := whatsmeow.NewClient(device, waLog.Noop)

	if cli.Store.ID == nil {
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "WhatsApp: первый запуск — отсканируй QR в приложении → Связанные устройства.")
		fmt.Fprintln(os.Stderr, "Сессия сохранится в session.db, повторно QR не понадобится.")
		fmt.Fprintln(os.Stderr, "")

		qrChan, err := cli.GetQRChannel(ctx)
		if err != nil {
			_ = container.Close()
			return fmt.Errorf("whatsapp GetQRChannel: %w", err)
		}
		if err := cli.Connect(); err != nil {
			_ = container.Close()
			return fmt.Errorf("whatsapp Connect: %w", err)
		}
		for evt := range qrChan {
			switch evt.Event {
			case whatsmeow.QRChannelEventCode:
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.M, os.Stdout)
			case whatsmeow.QRChannelEventError:
				fmt.Fprintf(os.Stderr, "WhatsApp QR error: %v\n", evt.Error)
			default:
				if evt.Event != "success" {
					fmt.Fprintf(os.Stderr, "WhatsApp login event: %s\n", evt.Event)
				}
			}
		}
	} else {
		if err := cli.Connect(); err != nil {
			_ = container.Close()
			return fmt.Errorf("whatsapp Connect: %w", err)
		}
	}

	if err := waitWhatsAppLoggedIn(ctx, cli, 2*time.Minute); err != nil {
		cli.Disconnect()
		_ = container.Close()
		return err
	}

	waMu.Lock()
	waClient = cli
	waMu.Unlock()
	return nil
}

func waitWhatsAppLoggedIn(ctx context.Context, cli *whatsmeow.Client, maxWait time.Duration) error {
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if cli.IsLoggedIn() {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("whatsapp: login timeout (not logged in after %s)", maxWait)
}

// SendWA отправляет текст на WHATSAPP_PHONE_E164.
func SendWA(message string) {
	if !waConfigured {
		return
	}
	waMu.Lock()
	c := waClient
	waMu.Unlock()
	if c == nil || !c.IsLoggedIn() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	_, _ = c.SendMessage(ctx, waRecipient, &waE2E.Message{
		Conversation: proto.String(message),
	})
}

// notifyNewTokenFoundWhatsApp — только успешная детекция (не [REJECTED]).
func notifyNewTokenFoundWhatsApp(mint string) {
	ctx := context.Background()
	name := fetchTokenDisplayNameDexscreener(ctx, mint)
	if name == "" {
		name = "name not on Dexscreener yet"
	}
	dex := fmt.Sprintf("https://dexscreener.com/solana/%s", mint)
	SendWA(fmt.Sprintf("💎 New Token Found: %s | %s | %s", name, mint, dex))
}

type dexscreenerTokenPairs struct {
	Pairs []struct {
		BaseToken struct {
			Address string `json:"address"`
			Name    string `json:"name"`
			Symbol  string `json:"symbol"`
		} `json:"baseToken"`
		QuoteToken struct {
			Address string `json:"address"`
			Name    string `json:"name"`
			Symbol  string `json:"symbol"`
		} `json:"quoteToken"`
	} `json:"pairs"`
}

func fetchTokenDisplayNameDexscreener(ctx context.Context, mint string) string {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.dexscreener.com/latest/dex/tokens/"+mint, nil)
	if err != nil {
		return ""
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	var body dexscreenerTokenPairs
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return ""
	}
	want := strings.ToLower(strings.TrimSpace(mint))
	for _, p := range body.Pairs {
		if strings.ToLower(strings.TrimSpace(p.BaseToken.Address)) == want {
			if n := strings.TrimSpace(p.BaseToken.Name); n != "" {
				return n
			}
			return strings.TrimSpace(p.BaseToken.Symbol)
		}
		if strings.ToLower(strings.TrimSpace(p.QuoteToken.Address)) == want {
			if n := strings.TrimSpace(p.QuoteToken.Name); n != "" {
				return n
			}
			return strings.TrimSpace(p.QuoteToken.Symbol)
		}
	}
	return ""
}

func digitsOnlyE164(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
