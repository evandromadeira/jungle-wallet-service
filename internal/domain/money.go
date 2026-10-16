package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

var (
	ErrInvalidAmount    = errors.New("invalid monetary amount")
	ErrInvalidCurrency  = errors.New("unsupported currency")
	ErrNegativeAmount   = errors.New("negative external monetary amount")
	ErrCurrencyMismatch = errors.New("currency mismatch")
	ErrMoneyOverflow    = errors.New("monetary amount overflow")
)

// Money is an immutable signed amount in minor units. Its zero value is invalid.
// Wallets and external inputs must enforce their own nonnegative constraints.
type Money struct {
	minor    int64
	currency string
}

// NewMoney creates an internal value in cents and validates its currency.
// Negative differences are allowed; this constructor does not enforce wallet balance rules.
func NewMoney(minor int64, currency string) (Money, error) {
	if !isValidCurrency(currency) {
		return Money{}, ErrInvalidCurrency
	}
	return Money{minor: minor, currency: currency}, nil
}

// ZeroMoney creates a valid zero amount in a supported currency.
func ZeroMoney(currency string) (Money, error) { return NewMoney(0, currency) }

// isValidCurrency reports whether the currency is BRL or USD, both using two decimal places.
func isValidCurrency(currency string) bool { return currency == "BRL" || currency == "USD" }

// Validate rejects uninitialized values and unsupported currencies; signed amounts are valid.
func (m Money) Validate() error {
	if !isValidCurrency(m.currency) {
		return ErrInvalidCurrency
	}
	return nil
}

// Minor returns the signed number of cents. Use the value only when the error is nil.
func (m Money) Minor() (int64, error) { return m.minor, m.Validate() }

// Currency returns the currency code. Use the value only when the error is nil.
func (m Money) Currency() (string, error) { return m.currency, m.Validate() }

// IsZero checks for a zero amount after validation; an uninitialized Money is not a valid zero.
func (m Money) IsZero() (bool, error) {
	if err := m.Validate(); err != nil {
		return false, err
	}
	return m.minor == 0, nil
}

// amountPattern requires a canonical unsigned decimal: e.g. 0.00 or 25.00, never 025.00.
var amountPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.[0-9]{2}$`)

// ParseMoney converts external decimal text to cents without rounding or floating point.
// It requires exactly two decimal places and rejects signs (including -0.00), leading zeros,
// whitespace, unsupported currencies, and values exceeding MaxInt64 cents.
func ParseMoney(amount, currency string) (Money, error) {
	if strings.HasPrefix(amount, "-") && amountPattern.MatchString(amount[1:]) {
		return Money{}, ErrNegativeAmount
	}
	if !amountPattern.MatchString(amount) {
		return Money{}, ErrInvalidAmount
	}
	minor, err := strconv.ParseInt(strings.Replace(amount, ".", "", 1), 10, 64)
	if err != nil {
		return Money{}, ErrMoneyOverflow
	}
	return NewMoney(minor, currency)
}

// Amount formats signed values without negating MinInt64 or using floating point.
func (m Money) Amount() (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	digits := strconv.FormatInt(m.minor, 10)
	sign := ""
	if m.minor < 0 {
		sign, digits = "-", digits[1:]
	}
	for len(digits) < 3 {
		digits = "0" + digits
	}
	return sign + digits[:len(digits)-2] + "." + digits[len(digits)-2:], nil
}

// sameCurrency validates both operands and rejects operations across different currencies.
func (m Money) sameCurrency(other Money) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if err := other.Validate(); err != nil {
		return err
	}
	if m.currency != other.currency {
		return ErrCurrencyMismatch
	}
	return nil
}

// Add returns a new sum in the same currency, rejecting int64 overflow before adding.
func (m Money) Add(other Money) (Money, error) {
	if err := m.sameCurrency(other); err != nil {
		return Money{}, err
	}
	if (other.minor > 0 && m.minor > math.MaxInt64-other.minor) || (other.minor < 0 && m.minor < math.MinInt64-other.minor) {
		return Money{}, ErrMoneyOverflow
	}
	return NewMoney(m.minor+other.minor, m.currency)
}

// Sub returns a new difference, which may be negative, and rejects int64 overflow before subtracting.
func (m Money) Sub(other Money) (Money, error) {
	if err := m.sameCurrency(other); err != nil {
		return Money{}, err
	}
	if (other.minor > 0 && m.minor < math.MinInt64+other.minor) || (other.minor < 0 && m.minor > math.MaxInt64+other.minor) {
		return Money{}, ErrMoneyOverflow
	}
	return NewMoney(m.minor-other.minor, m.currency)
}

// Negate returns the opposite amount; MinInt64 is rejected because its positive counterpart cannot fit.
func (m Money) Negate() (Money, error) {
	if err := m.Validate(); err != nil {
		return Money{}, err
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}
	return NewMoney(-m.minor, m.currency)
}

// Compare returns -1 when m is smaller, 0 when equal, and 1 when larger than other.
// Both operands must be valid and use the same currency.
func (m Money) Compare(other Money) (int, error) {
	if err := m.sameCurrency(other); err != nil {
		return 0, err
	}
	if m.minor < other.minor {
		return -1, nil
	}
	if m.minor > other.minor {
		return 1, nil
	}
	return 0, nil
}

// MarshalJSON encodes amount as a two-decimal string alongside the currency code.
// It rejects invalid Money and supports signed output, including reconciliation differences.
func (m Money) MarshalJSON() ([]byte, error) {
	amount, err := m.Amount()
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}{amount, m.currency})
}

// UnmarshalJSON is an external-input boundary: negative amounts are rejected.
// Failed decoding leaves the receiver unchanged. Internal persistence uses NewMoney.
func (m *Money) UnmarshalJSON(data []byte) error {
	if m == nil {
		return ErrInvalidAmount
	}
	var input struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}
	if err := json.Unmarshal(data, &input); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	value, err := ParseMoney(input.Amount, input.Currency)
	if err != nil {
		return err
	}
	*m = value
	return nil
}
