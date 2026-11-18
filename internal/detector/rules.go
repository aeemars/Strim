package detector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aeemars/Strim/internal/domain"
)

const (
	HighValueThresholdMinor int64 = 1_000_000
	ImpossibleTravelWindow        = 10 * time.Minute
	RuleVersion                   = "detector.v1"
)

type RuleEngine struct {
	Velocity *VelocityTracker
	Geo      *redis.Client
}

type RuleResult struct {
	VelocityExceeded  bool  `json:"velocity_exceeded"`
	VelocityCount     int64 `json:"velocity_count"`
	HighValueExceeded bool  `json:"high_value_exceeded"`
	ImpossibleTravel  bool  `json:"impossible_travel"`
}

type RoutedDecision struct {
	DecisionID    string             `json:"decision_id"`
	RuleVersion   string             `json:"rule_version"`
	ProcessedAt   time.Time          `json:"processed_at"`
	Transaction   domain.Transaction `json:"transaction"`
	AlertReasons  []string           `json:"alert_reasons,omitempty"`
	AlertSeverity string             `json:"alert_severity,omitempty"`
	SnapshotData  RuleResult         `json:"snapshot_data"`
}

var impossibleTravelScript = redis.NewScript(`
local key = KEYS[1]
local country = ARGV[1]
local event_ns = tonumber(ARGV[2])
local window_ns = tonumber(ARGV[3])
local ttl_seconds = tonumber(ARGV[4])
local previous = redis.call("GET", key)
redis.call("SET", key, country .. "|" .. tostring(event_ns), "EX", ttl_seconds)
if not previous then
  return 0
end
local separator = string.find(previous, "|", 1, true)
if not separator then
  return 0
end
local previous_country = string.sub(previous, 1, separator - 1)
local previous_ns = tonumber(string.sub(previous, separator + 1))
if not previous_ns then
  return 0
end
if previous_ns > event_ns then
  return 0
end
if previous_country ~= country and event_ns - previous_ns < window_ns then
  return 1
end
return 0
`)

func (r *RuleEngine) Evaluate(ctx context.Context, transaction domain.Transaction) (RuleResult, []string, string, error) {
	if r.Velocity == nil {
		return RuleResult{}, nil, "", errors.New("velocity tracker is nil")
	}
	count, err := r.Velocity.Count(ctx, transaction.AccountID, transaction.ID, transaction.Timestamp)
	if err != nil {
		return RuleResult{}, nil, "", err
	}
	result := RuleResult{
		VelocityCount:     count,
		VelocityExceeded:  count > r.Velocity.Threshold,
		HighValueExceeded: transaction.AmountMinor > HighValueThresholdMinor,
	}

	impossible, err := r.checkImpossibleTravel(ctx, transaction)
	if err != nil {
		return RuleResult{}, nil, "", err
	}
	result.ImpossibleTravel = impossible

	reasons := make([]string, 0, 3)
	if result.VelocityExceeded {
		reasons = append(reasons, domain.AnomalyReasonVelocity)
	}
	if result.HighValueExceeded {
		reasons = append(reasons, domain.AnomalyReasonHighValue)
	}
	if result.ImpossibleTravel {
		reasons = append(reasons, domain.AnomalyReasonImpossibleTravel)
	}
	severity := ""
	switch {
	case result.ImpossibleTravel || result.HighValueExceeded:
		severity = domain.SeverityHigh
	case result.VelocityExceeded:
		severity = domain.SeverityMedium
	}
	return result, reasons, severity, nil
}

func (r *RuleEngine) checkImpossibleTravel(ctx context.Context, transaction domain.Transaction) (bool, error) {
	if r.Geo == nil {
		return false, errors.New("geo Redis client is nil")
	}
	country := strings.ToUpper(strings.TrimSpace(transaction.LocationCountry))
	if len(country) != 2 {
		return false, errors.New("location_country must be a two-letter country code")
	}
	key := "account:" + transaction.AccountID + ":last_loc"
	flagged, err := impossibleTravelScript.Run(ctx, r.Geo, []string{key},
		country,
		strconv.FormatInt(transaction.Timestamp.UnixNano(), 10),
		strconv.FormatInt(int64(ImpossibleTravelWindow), 10),
		strconv.FormatInt(int64((12*time.Hour)/time.Second), 10),
	).Int()
	if err != nil {
		return false, fmt.Errorf("evaluate impossible travel: %w", err)
	}
	return flagged == 1, nil
}

func (d RoutedDecision) JSON() ([]byte, error) {
	return json.Marshal(d)
}
