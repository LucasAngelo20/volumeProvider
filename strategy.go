package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"
)

// ===============================
// Tipos básicos
// ===============================
type DepthEvent struct {
	BestBid float64
	BestAsk float64
}

type OrderEvent struct {
	OrderID       string
	ClientOrderID string
	Symbol        string
	Side          string  // BUY / SELL
	Status        string  // NEW / FILLED / PARTIALLY_FILLED / CANCELED
	Filled        float64 // quantidade executada nesta atualização
	AvgPrice      float64 // preço médio do fill (ou preço do fill)
}

type ExchangeAdapter interface {
	SubscribeDepth(ctx context.Context, symbol string, out chan<- DepthEvent) error
	SubscribeUserData(ctx context.Context, out chan<- OrderEvent) error
	PlaceLimitOrderWithClientID(ctx context.Context, side string, price float64, qty float64, clientID string) (string, error)
	CancelOrder(ctx context.Context, orderID string) error
	PlaceMarketOrder(ctx context.Context, side string, qty float64) error
	GetFees(symbol string) (maker, taker float64, err error)
}

// ===============================
// MarketMaker Spot
// ===============================
type MarketMaker struct {
	adapter      ExchangeAdapter
	symbol       string
	spread       float64
	orderSize    float64
	ttl          time.Duration
	inventoryUSD float64
	invThreshold float64

	totalUSD       float64 // saldo total da conta
	maxExposurePct float64 // limite de exposição, ex: 0.1 = 10%

	mu sync.Mutex

	lastMidPrice     float64
	lastPlacedAt     time.Time
	circuitBreakerOn bool
	cbThreshold      float64
	cbCooldown       time.Duration
	lastCBTime       time.Time

	lastBidID       string
	lastAskID       string
	lastBidPrice    float64
	lastAskPrice    float64
	lastBidClientID string
	lastAskClientID string

	// métricas
	realizedPnL   float64
	unrealizedPnL float64
	volumeBuy     float64
	volumeSell    float64
	ordersPlaced  int
	ordersFilled  int
}

// ===============================
func NewMarketMaker(adapter ExchangeAdapter, symbol string, spread, orderSize float64, invThreshold float64, totalUSD float64, maxExposurePct float64) *MarketMaker {
	return &MarketMaker{
		adapter:        adapter,
		symbol:         symbol,
		spread:         spread,
		orderSize:      orderSize,
		ttl:            10 * time.Second,
		invThreshold:   invThreshold,
		cbThreshold:    0.005,           // 0.5%
		cbCooldown:     5 * time.Second, // 5s cooldown
		totalUSD:       totalUSD,
		maxExposurePct: maxExposurePct,
	}
}

// ===============================
// Retry / backoff helper
// ===============================
func retryWithBackoff(ctx context.Context, attempts int, base time.Duration, fn func() error) error {
	var err error
	sleep := base
	for i := 0; i < attempts; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err = fn()
		if err == nil {
			return nil
		}

		// last attempt -> return
		if i == attempts-1 {
			break
		}

		// jittered backoff
		jitter := time.Duration(rand.Int63n(int64(sleep)))
		time.Sleep(sleep + jitter/2)
		sleep = sleep * 2
	}
	return err
}

// ===============================
// Safe wrappers (PlaceLimit, Cancel, Market) com retry/backoff
// ===============================
func (m *MarketMaker) safePlaceLimit(ctx context.Context, side string, price, qty float64, clientID string) (string, error) {
	var orderID string
	err := retryWithBackoff(ctx, 4, 150*time.Millisecond, func() error {
		id, e := m.adapter.PlaceLimitOrderWithClientID(ctx, side, price, qty, clientID)
		if e != nil {
			log.Printf("[retry] PlaceLimitOrder failed side=%s price=%.8f qty=%.8f clientID=%s err=%v", side, price, qty, clientID, e)
			return e
		}
		orderID = id
		return nil
	})
	return orderID, err
}

func (m *MarketMaker) safeCancel(ctx context.Context, orderID string) error {
	err := retryWithBackoff(ctx, 3, 150*time.Millisecond, func() error {
		e := m.adapter.CancelOrder(ctx, orderID)
		if e != nil {
			log.Printf("[retry] CancelOrder failed orderID=%s err=%v", orderID, e)
			return e
		}
		return nil
	})
	return err
}

func (m *MarketMaker) safeMarketOrder(ctx context.Context, side string, qty float64) error {
	err := retryWithBackoff(ctx, 3, 150*time.Millisecond, func() error {
		e := m.adapter.PlaceMarketOrder(ctx, side, qty)
		if e != nil {
			log.Printf("[retry] PlaceMarketOrder failed side=%s qty=%.8f err=%v", side, qty, e)
			return e
		}
		return nil
	})
	return err
}

// ===============================
// Rodar bot
// ===============================
func (m *MarketMaker) Run(ctx context.Context) {
	depthCh := make(chan DepthEvent, 200)
	orderCh := make(chan OrderEvent, 200)

	go func() {
		if err := m.adapter.SubscribeDepth(ctx, m.symbol, depthCh); err != nil {
			log.Fatal("depth subscribe:", err)
		}
	}()
	go func() {
		if err := m.adapter.SubscribeUserData(ctx, orderCh); err != nil {
			log.Fatal("userdata subscribe:", err)
		}
	}()

	// Log métricas periodicamente
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.mu.Lock()
				m.updateUnrealized(m.lastMidPrice)
				m.logMetrics()
				m.mu.Unlock()
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case depth := <-depthCh:
			m.mu.Lock()
			m.handleDepth(ctx, depth)
			m.mu.Unlock()
		case ev := <-orderCh:
			m.mu.Lock()
			m.handleOrder(ctx, ev)
			m.mu.Unlock()
		}
	}
}

// ===============================
// Handle depth event com Circuit-Breaker
// ===============================
func (m *MarketMaker) handleDepth(ctx context.Context, depth DepthEvent) {
	mid := (depth.BestBid + depth.BestAsk) / 2

	// Circuit breaker
	if m.lastMidPrice != 0 {
		move := math.Abs(mid-m.lastMidPrice) / m.lastMidPrice
		if move >= m.cbThreshold && !m.circuitBreakerOn {
			log.Printf("[CB ON] price moved %.4f%% | pausing orders", move*100)
			m.circuitBreakerOn = true
			m.lastCBTime = time.Now()
		}
	}

	if m.circuitBreakerOn && time.Since(m.lastCBTime) > m.cbCooldown {
		log.Println("[CB OFF] reactivating orders")
		m.circuitBreakerOn = false
	}

	if m.circuitBreakerOn {
		m.lastMidPrice = mid
		return
	}

	bidPrice := mid * (1 - m.spread/2)
	askPrice := mid * (1 + m.spread/2)

	if m.needsReprice(m.lastMidPrice, mid) {
		m.reprice(ctx, bidPrice, askPrice)
		m.lastMidPrice = mid
		m.lastPlacedAt = time.Now()
	}
}

// ===============================
// Cancel/replace inteligente
// ===============================
func (m *MarketMaker) needsReprice(oldMid, newMid float64) bool {
	if oldMid == 0 {
		return true
	}
	tol := 0.0005
	if math.Abs(newMid-oldMid)/oldMid < tol {
		return false
	}
	if time.Since(m.lastPlacedAt) > m.ttl {
		return true
	}
	return true
}

// ===============================
// Gerenciamento de capital
// ===============================
func (m *MarketMaker) allowedQty(side string, price float64) float64 {
	var exposure float64
	if side == "BUY" {
		exposure = m.inventoryUSD
	} else {
		exposure = -m.inventoryUSD
	}
	maxExp := m.totalUSD * m.maxExposurePct
	remaining := maxExp - exposure
	if remaining <= 0 {
		return 0
	}
	qty := m.orderSize
	if qty*price > remaining {
		qty = remaining / price
	}
	return qty
}

// ===============================
// Reprice ordens (usa safe wrappers)
// ===============================
func (m *MarketMaker) reprice(ctx context.Context, bidPrice, askPrice float64) {
	// cancel e só limpa se cancel ok
	if m.lastBidID != "" && m.needsReprice(m.lastBidPrice, bidPrice) {
		if err := m.safeCancel(ctx, m.lastBidID); err == nil {
			m.lastBidID = ""
			m.lastBidClientID = ""
		} else {
			log.Printf("[reprice] failed to cancel bid %s: %v (keeping it)", m.lastBidID, err)
		}
	}
	if m.lastAskID != "" && m.needsReprice(m.lastAskPrice, askPrice) {
		if err := m.safeCancel(ctx, m.lastAskID); err == nil {
			m.lastAskID = ""
			m.lastAskClientID = ""
		} else {
			log.Printf("[reprice] failed to cancel ask %s: %v (keeping it)", m.lastAskID, err)
		}
	}

	// place buy if allowed and not already present
	if m.lastBidID == "" {
		qty := m.allowedQty("BUY", bidPrice)
		if qty > 0 {
			clientID := generateClientOrderID("BUY")
			bidID, err := m.safePlaceLimit(ctx, "BUY", bidPrice, qty, clientID)
			if err == nil {
				m.lastBidID = bidID
				m.lastBidPrice = bidPrice
				m.lastBidClientID = clientID
				m.ordersPlaced++
			} else {
				log.Printf("[reprice] failed to place buy: %v", err)
			}
		}
	}

	// place sell
	if m.lastAskID == "" {
		qty := m.allowedQty("SELL", askPrice)
		if qty > 0 {
			clientID := generateClientOrderID("SELL")
			askID, err := m.safePlaceLimit(ctx, "SELL", askPrice, qty, clientID)
			if err == nil {
				m.lastAskID = askID
				m.lastAskPrice = askPrice
				m.lastAskClientID = clientID
				m.ordersPlaced++
			} else {
				log.Printf("[reprice] failed to place sell: %v", err)
			}
		}
	}

	log.Printf("[reprice] bid=%.8f ask=%.8f", bidPrice, askPrice)
}

// ===============================
// Handle ordem executada com reposição de fills parciais e hedge (usa safeMarketOrder)
// ===============================
func (m *MarketMaker) handleOrder(ctx context.Context, ev OrderEvent) {
	if ev.Status != "FILLED" && ev.Status != "PARTIALLY_FILLED" {
		// ignorar outros eventos por enquanto (NEW, CANCELED)
		return
	}

	notional := ev.Filled * ev.AvgPrice
	if ev.Side == "BUY" {
		m.inventoryUSD += notional
		m.volumeBuy += notional
	} else {
		m.inventoryUSD -= notional
		m.volumeSell += notional
	}
	m.ordersFilled++

	// Atualiza PnL realizado (aproximação usando last bid/ask)
	if ev.Status == "FILLED" {
		if ev.Side == "SELL" {
			// vendeu — lucro relativo ao lastBidPrice (simplificação)
			m.realizedPnL += (ev.AvgPrice - m.lastBidPrice) * ev.Filled
		} else {
			// comprou — negativo relativo ao lastAskPrice (simplificação)
			m.realizedPnL += (m.lastAskPrice - ev.AvgPrice) * ev.Filled
		}
	}

	// Hedge (usar safeMarketOrder)
	if m.inventoryUSD > m.invThreshold {
		hedgeQty := m.inventoryUSD / ev.AvgPrice
		if hedgeQty > 0 {
			if err := m.safeMarketOrder(ctx, "SELL", hedgeQty); err == nil {
				m.inventoryUSD = 0
				log.Printf("[hedge] sold inventory qty=%.8f", hedgeQty)
			} else {
				log.Printf("[hedge] failed market sell qty=%.8f err=%v", hedgeQty, err)
			}
		}
	} else if m.inventoryUSD < -m.invThreshold {
		hedgeQty := -m.inventoryUSD / ev.AvgPrice
		if hedgeQty > 0 {
			if err := m.safeMarketOrder(ctx, "BUY", hedgeQty); err == nil {
				m.inventoryUSD = 0
				log.Printf("[hedge] bought inventory qty=%.8f", hedgeQty)
			} else {
				log.Printf("[hedge] failed market buy qty=%.8f err=%v", hedgeQty, err)
			}
		}
	}

	// Reposição parcial (respeita exposure e usa safePlaceLimit)
	remainingQty := m.orderSize - ev.Filled
	allowedQty := m.allowedQty(ev.Side, ev.AvgPrice)
	if remainingQty > 0.00000001 && allowedQty > 0 {
		repostQty := math.Min(remainingQty, allowedQty)
		clientID := generateClientOrderID(ev.Side)
		newOrderID, err := m.safePlaceLimit(ctx, ev.Side, ev.AvgPrice, repostQty, clientID)
		if err == nil {
			log.Printf("[repost] %s %.8f @ %.8f | newOrderID=%s", ev.Side, repostQty, ev.AvgPrice, newOrderID)
			if ev.Side == "BUY" {
				m.lastBidID = newOrderID
				m.lastBidClientID = clientID
				m.lastBidPrice = ev.AvgPrice
			} else {
				m.lastAskID = newOrderID
				m.lastAskClientID = clientID
				m.lastAskPrice = ev.AvgPrice
			}
			m.ordersPlaced++
		} else {
			log.Printf("[repost] failed to repost %s err=%v", ev.Side, err)
		}
	} else {
		// se sem repost, limpa referência da ordem
		if ev.Side == "BUY" {
			m.lastBidID = ""
			m.lastBidClientID = ""
		} else {
			m.lastAskID = ""
			m.lastAskClientID = ""
		}
	}
}

// ===============================
// Atualiza PnL não realizado
// ===============================
func (m *MarketMaker) updateUnrealized(midPrice float64) {
	if m.lastBidPrice == 0 && m.lastAskPrice == 0 {
		m.unrealizedPnL = 0
		return
	}
	// estimativa simples: exposição em ordens colocadas vs mid
	bidExposure := 0.0
	askExposure := 0.0
	if m.lastBidID != "" {
		bidExposure = m.orderSize
	}
	if m.lastAskID != "" {
		askExposure = m.orderSize
	}
	m.unrealizedPnL = (midPrice-m.lastBidPrice)*bidExposure + (m.lastAskPrice-midPrice)*askExposure
}

// ===============================
// Log métricas
// ===============================
func (m *MarketMaker) logMetrics() {
	hitRatio := 0.0
	if m.ordersPlaced > 0 {
		hitRatio = float64(m.ordersFilled) / float64(m.ordersPlaced)
	}
	log.Printf("[metrics] RealizedPnL=%.8f | UnrealizedPnL=%.8f | BuyVol=%.4f | SellVol=%.4f | HitRatio=%.2f%% | Inventory=%.4f",
		m.realizedPnL, m.unrealizedPnL, m.volumeBuy, m.volumeSell, hitRatio*100, m.inventoryUSD)
}

// ===============================
// Helper: STP
// ===============================
func generateClientOrderID(side string) string {
	return fmt.Sprintf("BOT-%d-%s", time.Now().UnixNano(), side)
}
