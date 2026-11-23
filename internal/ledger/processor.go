package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	segmentio "github.com/segmentio/kafka-go"

	"github.com/aeemars/Strim/internal/domain"
)

var (
	ErrDuplicateTransaction   = errors.New("transaction already processed")
	ErrIdempotencyKeyConflict = errors.New("idempotency key already used for a different transaction")
	ErrAccountNotFound        = errors.New("account not found")
	ErrInsufficientFunds      = errors.New("insufficient funds in sender account")
	ErrCurrencyMismatch       = errors.New("currency mismatch between accounts or transaction")
	ErrSameAccount            = errors.New("sender and destination account must be distinct")
	ErrInvalidAmount          = errors.New("amount must be positive")
	ErrUnsupportedType        = errors.New("unsupported transaction type")
)

type Account struct {
	ID            string
	AccountNumber string
	BalanceMinor  int64
	Currency      string
	CreatedAt     time.Time
}

// DB abstracts the database transaction starter (*pgxpool.Pool or fake in tests).
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Processor struct {
	DB          DB
	MaxAttempts int
}

func NewProcessor(db DB) *Processor {
	return &Processor{
		DB:          db,
		MaxAttempts: 3,
	}
}

func (p *Processor) Process(ctx context.Context, message segmentio.Message) error {
	tx, err := ParseClearedTransaction(message.Value)
	if err != nil {
		return fmt.Errorf("decode cleared message: %w", err)
	}

	if err := tx.ValidateAt(tx.Timestamp); err != nil {
		return fmt.Errorf("validate cleared transaction: %w", err)
	}

	attempts := p.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}

	var lastErr error
	backoff := 50 * time.Millisecond
	for attempt := 1; attempt <= attempts; attempt++ {
		lastErr = p.settleTransaction(ctx, tx)
		if lastErr == nil {
			return nil
		}

		if errors.Is(lastErr, ErrDuplicateTransaction) {
			slog.Info("duplicate transaction acknowledged without mutation",
				"transaction_id", tx.ID,
				"idempotency_key", tx.IdempotencyKey)
			return nil
		}

		if !isRetryable(lastErr) || attempt == attempts {
			break
		}

		slog.Warn("retrying ledger settlement after transient error",
			"attempt", attempt,
			"transaction_id", tx.ID,
			"error", lastErr)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff *= 2
	}

	return lastErr
}

func (p *Processor) settleTransaction(ctx context.Context, tx domain.Transaction) error {
	if p.DB == nil {
		return errors.New("database connection is nil")
	}

	dbTx, err := p.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin db transaction: %w", err)
	}
	defer func() {
		_ = dbTx.Rollback(ctx)
	}()

	// 1. Idempotency Check & Registration
	cmdTag, err := dbTx.Exec(ctx, `
		INSERT INTO processed_transactions (idempotency_key, transaction_id, processed_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, tx.IdempotencyKey, tx.ID, time.Now().UTC())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// Duplicate transaction_id with a different idempotency key
			return fmt.Errorf("%w: transaction_id %s already exists", ErrDuplicateTransaction, tx.ID)
		}
		return fmt.Errorf("insert processed_transaction: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		// Idempotency key already exists. Verify if transaction_id matches.
		var existingTxID string
		err := dbTx.QueryRow(ctx, `
			SELECT transaction_id::text
			FROM processed_transactions
			WHERE idempotency_key = $1
		`, tx.IdempotencyKey).Scan(&existingTxID)
		if err != nil {
			return fmt.Errorf("query existing processed transaction: %w", err)
		}
		if existingTxID != tx.ID {
			return fmt.Errorf("%w: key %s bound to tx %s, received %s",
				ErrIdempotencyKeyConflict, tx.IdempotencyKey, existingTxID, tx.ID)
		}
		return ErrDuplicateTransaction
	}

	// 2. Invariants Check
	if tx.AmountMinor <= 0 {
		return ErrInvalidAmount
	}

	switch tx.Type {
	case domain.TransactionTypeTransfer, domain.TransactionTypePayment:
		if tx.DestinationAccountID == "" {
			return errors.New("destination_account_id is required for transfers")
		}
		if tx.AccountID == tx.DestinationAccountID {
			return ErrSameAccount
		}
		if err := p.executeTransfer(ctx, dbTx, tx); err != nil {
			return err
		}

	case domain.TransactionTypeWithdrawal:
		if err := p.executeWithdrawal(ctx, dbTx, tx); err != nil {
			return err
		}

	default:
		return fmt.Errorf("%w: %s", ErrUnsupportedType, tx.Type)
	}

	if err := dbTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit ledger transaction: %w", err)
	}

	slog.Info("transaction settled in ledger",
		"transaction_id", tx.ID,
		"account_id", tx.AccountID,
		"destination_account_id", tx.DestinationAccountID,
		"amount_minor", tx.AmountMinor,
		"currency", tx.Currency,
		"type", tx.Type)

	return nil
}

func (p *Processor) executeTransfer(ctx context.Context, dbTx pgx.Tx, tx domain.Transaction) error {
	// Deterministic account lock ordering to prevent deadlocks across concurrent transfers
	firstID, secondID := tx.AccountID, tx.DestinationAccountID
	if firstID > secondID {
		firstID, secondID = secondID, firstID
	}

	firstAcc, err := lockAccount(ctx, dbTx, firstID)
	if err != nil {
		return fmt.Errorf("lock account %s: %w", firstID, err)
	}
	secondAcc, err := lockAccount(ctx, dbTx, secondID)
	if err != nil {
		return fmt.Errorf("lock account %s: %w", secondID, err)
	}

	var sender, recipient Account
	if firstAcc.AccountNumber == tx.AccountID || firstAcc.ID == tx.AccountID {
		sender = firstAcc
		recipient = secondAcc
	} else {
		sender = secondAcc
		recipient = firstAcc
	}

	if sender.Currency != recipient.Currency {
		return fmt.Errorf("%w: sender currency %s, recipient currency %s",
			ErrCurrencyMismatch, sender.Currency, recipient.Currency)
	}
	if tx.Currency != sender.Currency {
		return fmt.Errorf("%w: transaction currency %s, account currency %s",
			ErrCurrencyMismatch, tx.Currency, sender.Currency)
	}
	if sender.BalanceMinor < tx.AmountMinor {
		return fmt.Errorf("%w: balance %d, required %d",
			ErrInsufficientFunds, sender.BalanceMinor, tx.AmountMinor)
	}

	// Mutate balances
	if _, err := dbTx.Exec(ctx, `
		UPDATE accounts
		SET balance_minor = balance_minor - $1
		WHERE id = $2
	`, tx.AmountMinor, sender.ID); err != nil {
		return fmt.Errorf("debit sender account: %w", err)
	}

	if _, err := dbTx.Exec(ctx, `
		UPDATE accounts
		SET balance_minor = balance_minor + $1
		WHERE id = $2
	`, tx.AmountMinor, recipient.ID); err != nil {
		return fmt.Errorf("credit recipient account: %w", err)
	}

	now := time.Now().UTC()
	// Insert Double-Entry Ledger records
	if _, err := dbTx.Exec(ctx, `
		INSERT INTO ledger_entries (transaction_id, account_id, entry_type, amount_minor, currency, created_at)
		VALUES ($1, $2, 'DEBIT', $3, $4, $5)
	`, tx.ID, sender.ID, tx.AmountMinor, tx.Currency, now); err != nil {
		return fmt.Errorf("insert debit ledger entry: %w", err)
	}

	if _, err := dbTx.Exec(ctx, `
		INSERT INTO ledger_entries (transaction_id, account_id, entry_type, amount_minor, currency, created_at)
		VALUES ($1, $2, 'CREDIT', $3, $4, $5)
	`, tx.ID, recipient.ID, tx.AmountMinor, tx.Currency, now); err != nil {
		return fmt.Errorf("insert credit ledger entry: %w", err)
	}

	return nil
}

func (p *Processor) executeWithdrawal(ctx context.Context, dbTx pgx.Tx, tx domain.Transaction) error {
	sender, err := lockAccount(ctx, dbTx, tx.AccountID)
	if err != nil {
		return fmt.Errorf("lock account %s: %w", tx.AccountID, err)
	}

	if tx.Currency != sender.Currency {
		return fmt.Errorf("%w: transaction currency %s, account currency %s",
			ErrCurrencyMismatch, tx.Currency, sender.Currency)
	}
	if sender.BalanceMinor < tx.AmountMinor {
		return fmt.Errorf("%w: balance %d, required %d",
			ErrInsufficientFunds, sender.BalanceMinor, tx.AmountMinor)
	}

	if _, err := dbTx.Exec(ctx, `
		UPDATE accounts
		SET balance_minor = balance_minor - $1
		WHERE id = $2
	`, tx.AmountMinor, sender.ID); err != nil {
		return fmt.Errorf("debit withdrawal account: %w", err)
	}

	now := time.Now().UTC()
	if _, err := dbTx.Exec(ctx, `
		INSERT INTO ledger_entries (transaction_id, account_id, entry_type, amount_minor, currency, created_at)
		VALUES ($1, $2, 'DEBIT', $3, $4, $5)
	`, tx.ID, sender.ID, tx.AmountMinor, tx.Currency, now); err != nil {
		return fmt.Errorf("insert withdrawal ledger entry: %w", err)
	}

	return nil
}

func lockAccount(ctx context.Context, tx pgx.Tx, identifier string) (Account, error) {
	var acc Account
	var accUUID uuid.UUID
	err := tx.QueryRow(ctx, `
		SELECT id, account_number, balance_minor, currency, created_at
		FROM accounts
		WHERE account_number = $1 OR id::text = $1
		FOR UPDATE
	`, identifier).Scan(&accUUID, &acc.AccountNumber, &acc.BalanceMinor, &acc.Currency, &acc.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Account{}, fmt.Errorf("%w: %s", ErrAccountNotFound, identifier)
		}
		return Account{}, err
	}
	acc.ID = accUUID.String()
	acc.Currency = strings.TrimSpace(acc.Currency)
	return acc, nil
}

func ParseClearedTransaction(payload []byte) (domain.Transaction, error) {
	var decision struct {
		Transaction domain.Transaction `json:"transaction"`
	}
	if err := json.Unmarshal(payload, &decision); err == nil && decision.Transaction.ID != "" {
		return decision.Transaction, nil
	}
	var tx domain.Transaction
	if err := json.Unmarshal(payload, &tx); err == nil && tx.ID != "" {
		return tx, nil
	}
	return domain.Transaction{}, errors.New("unable to parse cleared transaction from payload")
}

func isRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// 40001 = serialization_failure, 40P01 = deadlock_detected
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	return false
}
