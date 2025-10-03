package main

import (
	"context"
	"log"
)

func main() {
	ctx := context.Background()

	var adapter ExchangeAdapter = nil // TODO: plugar Binance Spot Adapter
	if adapter == nil {
		log.Fatal("Erro: ExchangeAdapter não implementado")
	}

	// params: adapter, symbol, spread, orderSize, invThreshold, totalUSD, maxExposurePct
	bot := NewMarketMaker(adapter, "USDCUSDT", 0.001, 100, 200, 10000, 0.1)
	bot.Run(ctx)
}
