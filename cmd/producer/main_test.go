package main

import (
	"math/rand"
	"testing"
)

func TestSplitBrokers(t *testing.T) {
	got := splitBrokers(" localhost:9092, kafka:9092 ,, ")
	want := []string{"localhost:9092", "kafka:9092"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestNormalTransactionUsesOneHundredAccountsAndNGN(t *testing.T) {
	random := rand.New(rand.NewSource(42))
	accounts := make(map[string]struct{})
	for i := 0; i < 100; i++ {
		transaction := normalTransaction(random)
		accounts[transaction.AccountID] = struct{}{}
		if transaction.Currency != "NGN" {
			t.Fatalf("got currency %q, want NGN", transaction.Currency)
		}
		if transaction.AmountMinor <= 0 {
			t.Fatalf("got non-positive amount %d", transaction.AmountMinor)
		}
		if transaction.AccountID == transaction.DestinationAccountID {
			t.Fatal("sender and destination accounts must differ")
		}
	}
	if len(accounts) == 0 || len(accounts) > 100 {
		t.Fatalf("got %d distinct account IDs", len(accounts))
	}
}
