// Follow the Smart Money: вход на pump.fun с задержкой после покупки отслеживаемых кошельков.
package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
)

// smartMoneySkipIkemeRandomDelay — по умолчанию true: не ждать PUMP_IKEME_DELAY_* минутами при follow (уже есть задержка после лидера).
func smartMoneySkipIkemeRandomDelay() bool {
	s := strings.TrimSpace(strings.ToLower(os.Getenv("PUMP_SMART_MONEY_SKIP_IKEME_DELAY")))
	if s == "" {
		return true
	}
	if s == "0" || s == "false" || s == "no" || s == "off" {
		return false
	}
	return s == "1" || s == "true" || s == "yes" || s == "on"
}

// Список «лидеров» (адреса с DexScreener / ваши логи). Пустой список = режим выключен.
var (
	smartMoneyMu            sync.RWMutex
	smartMoneyLeaderSet     = make(map[string]struct{})
	smartMoneyFollowEnabled bool
	smartMoneyFollowDelay   = 500 * time.Millisecond
)

func reloadSmartMoneyLeadersFromEnv() {
	smartMoneyMu.Lock()
	defer smartMoneyMu.Unlock()

	smartMoneyLeaderSet = make(map[string]struct{})

	add := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" || strings.HasPrefix(s, "#") {
			return
		}
		if _, err := solana.PublicKeyFromBase58(s); err != nil {
			log.Printf("[SMART] skip invalid pubkey: %q", s)
			return
		}
		smartMoneyLeaderSet[s] = struct{}{}
	}

	if raw := strings.TrimSpace(os.Getenv("PUMP_SMART_MONEY_WALLETS")); raw != "" {
		for _, p := range strings.Split(raw, ",") {
			add(p)
		}
	}
	if path := strings.TrimSpace(os.Getenv("PUMP_SMART_MONEY_WALLETS_FILE")); path != "" {
		f, err := os.Open(path)
		if err != nil {
			log.Printf("[SMART] cannot open PUMP_SMART_MONEY_WALLETS_FILE %q: %v", path, err)
		} else {
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				add(sc.Text())
			}
			_ = f.Close()
			if err := sc.Err(); err != nil {
				log.Printf("[SMART] read wallets file: %v", err)
			}
		}
	}

	smartMoneyFollowEnabled = len(smartMoneyLeaderSet) > 0
	if s := strings.TrimSpace(strings.ToLower(os.Getenv("PUMP_SMART_MONEY_FOLLOW"))); s == "false" || s == "0" || s == "no" {
		smartMoneyFollowEnabled = false
	}

	ms := 500
	if s := strings.TrimSpace(os.Getenv("PUMP_SMART_MONEY_FOLLOW_DELAY_MS")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 0 && v <= 120_000 {
			ms = v
		}
	}
	smartMoneyFollowDelay = time.Duration(ms) * time.Millisecond

	if len(smartMoneyLeaderSet) > 0 {
		log.Printf("[SMART] Follow-the-smart-money: enabled=%v leaders=%d delay=%s",
			smartMoneyFollowEnabled, len(smartMoneyLeaderSet), smartMoneyFollowDelay)
	}
}

func isSmartMoneyLeader(pk solana.PublicKey) bool {
	smartMoneyMu.RLock()
	defer smartMoneyMu.RUnlock()
	_, ok := smartMoneyLeaderSet[pk.String()]
	return ok
}

func smartMoneyFollowActive() bool {
	smartMoneyMu.RLock()
	defer smartMoneyMu.RUnlock()
	return smartMoneyFollowEnabled && len(smartMoneyLeaderSet) > 0
}
