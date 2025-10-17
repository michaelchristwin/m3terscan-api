package api

import (
	"database/sql"
	_ "embed"
	"errors"
	"log"
	"m3terscan-api/internal/models"
	"m3terscan-api/internal/tutorial"
	"m3terscan-api/internal/util"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gin-gonic/gin"
	_ "modernc.org/sqlite"
)

var (
	parsedABI abi.ABI
	Db        *sql.DB
	initOnce  sync.Once
)

//go:embed schema.sql
var ddl string

// func init() {
// 	var err error
// 	parsedABI, err = abi.JSON(strings.NewReader(m3tering.M3teringABI))
// 	if err != nil {
// 		panic(fmt.Errorf("failed to parse ABI: %w", err))
// 	}
// 	Db, err = sql.Open("sqlite", "file:m3ters.db?_foreign_keys=on")
// 	if err != nil {
// 		log.Fatal(err)

// 	}

// 	_, err = Db.Exec("PRAGMA journal_mode = WAL;")
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	log.Println("WAL mode enabled")
// 	initOnce.Do(func() {
// 		if _, err := Db.Exec(ddl); err != nil {
// 			log.Fatalf("failed to initialize schema: %v", err)
// 		}
// 		log.Println("Schema initialized successfully")
// 	})
// }

// GetCommitState godoc
// @Summary      Get commit state for a transaction
// @Description  Returns the commit state for a given transaction hash
// @Tags         commit
// @Param        hash   query   string  true  "Transaction hash"
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Failure      400  {object}  map[string]string
// @Failure      500  {object}  map[string]string
// @Router       /commit_state [get]
func GetCommitState(ctx *gin.Context, client *ethclient.Client) {
	hexStr := ctx.Query("hash")
	if hexStr == "" {
		ctx.JSON(400, gin.H{"error": "Hash is required"})
		return
	}

	queries := tutorial.New(Db)

	// Try fetching proposal from DB
	acc, err := queries.GetProposalMeters(ctx, hexStr)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	var resp []models.StateResponse
	for _, r := range acc {
		nonceInt, _ := new(big.Int).SetString(r.Nonce, 10)
		resp = append(resp, models.StateResponse{
			M3terNo: int(r.M3terNo),
			Account: r.Account,
			Nonce:   nonceInt,
		})
	}

	if len(resp) > 0 {
		log.Println("Proposal found in db")
		ctx.JSON(200, gin.H{"data": resp})
		return
	}

	// Proposal not found in DB — fetch from blockchain
	hsh := common.HexToHash(hexStr)
	tx, isPending, err := client.TransactionByHash(ctx.Request.Context(), hsh)
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}
	if isPending {
		ctx.JSON(200, gin.H{"status": "pending", "hash": hsh.Hex()})
		return
	}

	inputData := tx.Data()
	if len(inputData) < 4 {
		ctx.JSON(500, gin.H{"error": "invalid transaction data"})
		return
	}

	method, err := parsedABI.MethodById(inputData[:4])
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	args, err := method.Inputs.Unpack(inputData[4:])
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	accounts, err := util.BytesToChunks(args[0].([]byte), 6)
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}
	nonces, err := util.BytesToChunks(args[1].([]byte), 6)
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}
	meters, err := util.CombineAccountsNonces(accounts, nonces)
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	// ✅ Corrected transaction block
	sqlTx, err := Db.BeginTx(ctx, nil)
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}
	defer sqlTx.Rollback()

	q := tutorial.New(sqlTx)

	// Create proposal
	_, err = q.CreateProposal(ctx, hexStr)
	if err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	// Insert all meters
	for _, s := range meters {
		err = q.AddProposalMeter(ctx, tutorial.AddProposalMeterParams{
			ProposalID: hexStr,
			M3terNo:    int64(s.M3terNo),
			Account:    s.Account,
			Nonce:      s.Nonce.String(),
		})
		if err != nil {
			ctx.JSON(500, gin.H{"error": err.Error()})
			return
		}
	}

	if err = sqlTx.Commit(); err != nil {
		ctx.JSON(500, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(200, gin.H{"data": meters})
}
