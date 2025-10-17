package api

import (
	"context"
	"log"
	"m3terscan-api/internal/blockchain"
	"m3terscan-api/internal/m3tering"
	"m3terscan-api/internal/models"
	"m3terscan-api/internal/subgraph"
	"m3terscan-api/internal/util"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gin-gonic/gin"
	"github.com/machinebox/graphql"
)

func GetCurrentWeek(ctx *gin.Context) {
	id := ctx.Param("id")
	idInt, err := strconv.Atoi(id)
	if err != nil {
		ctx.JSON(400, gin.H{"error": "Invalid ID format"})
		return
	}

	tokenId := big.NewInt(int64(idInt))
	now := time.Now()
	days := int(now.Weekday()) + 1 // Sunday=0, so +1 → Monday=2, etc.
	interval := days * 96          // 96 fifteen-minute intervals per day

	contractAddress := common.HexToAddress("0xf8f2d4315DB5db38f3e5c45D0bCd59959c603d9b")

	client, err := blockchain.GetClient()
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	instance, err := m3tering.NewM3tering(contractAddress, client)
	if err != nil {
		log.Println(err)
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	latestNonceBytes, err := instance.Nonce(&bind.CallOpts{Context: context.Background()}, tokenId)
	if err != nil {
		log.Println(err)
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	latestNonceInt := util.Bytes6ToInt64(latestNonceBytes)
	startNonce := int64(latestNonceInt) - int64(interval)
	nonces := util.NonceRange(int(startNonce), int(latestNonceInt))

	req := graphql.NewRequest(`
		query ExampleQuery($meterNumber: Int!, $nonces: [Int!]) {
			meterDataPoints(meterNumber: $meterNumber, nonces: $nonces) {
				node {
					timestamp
					payload {
						energy
					}
				}
			}
		}
	`)
	req.Var("meterNumber", idInt)
	req.Var("nonces", nonces)

	var resp models.ReqStruct
	if err := subgraph.Client.Run(ctx, req, &resp); err != nil {
		log.Printf("GraphQL error: %v", err)
		ctx.JSON(500, gin.H{"error": "Failed to fetch data"})
		return
	}

	// Sum energy by day of week
	weekData := make(map[time.Weekday]int64)

	for _, item := range resp.MeterDataPoints {
		ts := item.Node.Timestamp
		if ts == 0 {
			continue
		}
		t := time.Unix(ts, 0)
		day := t.Weekday() // Sunday=0 ... Saturday=6
		weekData[day] += int64(item.Node.Payload.Energy)
	}

	// Prepare ordered output: Monday → Sunday
	daysOrder := []time.Weekday{
		time.Sunday, time.Monday, time.Tuesday, time.Wednesday,
		time.Thursday, time.Friday, time.Saturday,
	}

	var result []models.CurrentWeekResponse
	for _, d := range daysOrder {
		result = append(result, models.CurrentWeekResponse{
			DayOfWeek: d.String(),
			Energy:    weekData[d],
		})
	}

	ctx.JSON(200, gin.H{"data": result})
}
