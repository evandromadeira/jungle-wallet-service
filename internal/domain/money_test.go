package domain

import (
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"testing"
)

// mustMoney builds a test value and immediately fails the test if construction fails.
func mustMoney(t *testing.T, value int64, currency string) Money {
	t.Helper()
	m, err := NewMoney(value, currency)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// TestParseMoney checks canonical external input, numeric limits, and currency rejection.
func TestParseMoney(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  int64
		err   error
	}{
		{"0.00", 0, nil}, {"0.01", 1, nil}, {"25.00", 2500, nil},
		{"92233720368547758.07", math.MaxInt64, nil},
		{"92233720368547758.08", 0, ErrMoneyOverflow},
		{"999999999999999999999999.00", 0, ErrMoneyOverflow},
		{"-0.00", 0, ErrNegativeAmount}, {"-1.00", 0, ErrNegativeAmount},
		{"", 0, ErrInvalidAmount}, {"NaN", 0, ErrInvalidAmount},
		{"Infinity", 0, ErrInvalidAmount}, {"1e2", 0, ErrInvalidAmount},
		{"1.001", 0, ErrInvalidAmount}, {"1.0", 0, ErrInvalidAmount},
		{"01.00", 0, ErrInvalidAmount}, {"+1.00", 0, ErrInvalidAmount},
		{" 1.00", 0, ErrInvalidAmount}, {"1.00\n", 0, ErrInvalidAmount},
		{"1,00", 0, ErrInvalidAmount}, {"1", 0, ErrInvalidAmount},
	} {
		t.Run(tc.input, func(t *testing.T) {
			m, err := ParseMoney(tc.input, "BRL")
			if !errors.Is(err, tc.err) {
				t.Fatalf("error = %v, want %v", err, tc.err)
			}
			if err == nil && m.minor != tc.want {
				t.Fatalf("minor = %d, want %d", m.minor, tc.want)
			}
		})
	}
	for _, currency := range []string{"", "ZZZ", "brl", "BR", "BRLL", "JPY"} {
		if _, err := NewMoney(0, currency); !errors.Is(err, ErrInvalidCurrency) {
			t.Fatalf("accepted %q", currency)
		}
		if _, err := ParseMoney("1.00", currency); !errors.Is(err, ErrInvalidCurrency) {
			t.Fatalf("parsed %q", currency)
		}
	}
}

// TestSignedArithmeticAgainstBigInt checks arithmetic and comparison against exact big integers.
func TestSignedArithmeticAgainstBigInt(t *testing.T) {
	values := []int64{math.MinInt64, math.MinInt64 + 1, -2500, -100, -1, 0, 1, 100, 2500, math.MaxInt64 - 1, math.MaxInt64}
	for _, a := range values {
		for _, b := range values {
			left, right := mustMoney(t, a, "BRL"), mustMoney(t, b, "BRL")
			for _, operation := range []string{"add", "sub"} {
				expected := new(big.Int)
				var got Money
				var err error
				if operation == "add" {
					expected.Add(big.NewInt(a), big.NewInt(b))
					got, err = left.Add(right)
				} else {
					expected.Sub(big.NewInt(a), big.NewInt(b))
					got, err = left.Sub(right)
				}
				if !expected.IsInt64() {
					if !errors.Is(err, ErrMoneyOverflow) {
						t.Fatalf("%d %s %d: %v", a, operation, b, err)
					}
				} else if err != nil || got.minor != expected.Int64() || got.currency != "BRL" {
					t.Fatalf("%d %s %d: got %+v, %v; want %s", a, operation, b, got, err, expected)
				}
				if left.minor != a || right.minor != b {
					t.Fatal("operands mutated")
				}
			}
			comparison, err := left.Compare(right)
			if err != nil || comparison != big.NewInt(a).Cmp(big.NewInt(b)) {
				t.Fatal("incorrect comparison")
			}
		}
		m := mustMoney(t, a, "BRL")
		negated, err := m.Negate()
		if a == math.MinInt64 {
			if !errors.Is(err, ErrMoneyOverflow) {
				t.Fatal("negation must reject overflow")
			}
		} else if err != nil || negated.minor != -a {
			t.Fatal("incorrect negation")
		}
	}
}

// TestAmountAndJSON checks exact output, external input validation, and unchanged values on decoding failure.
func TestAmountAndJSON(t *testing.T) {
	for _, tc := range []struct {
		minor  int64
		amount string
	}{
		{0, "0.00"}, {1, "0.01"}, {-1, "-0.01"}, {-99, "-0.99"}, {-100, "-1.00"},
		{-2500, "-25.00"}, {math.MinInt64, "-92233720368547758.08"}, {math.MaxInt64, "92233720368547758.07"},
	} {
		m := mustMoney(t, tc.minor, "BRL")
		amount, err := m.Amount()
		if err != nil || amount != tc.amount {
			t.Fatalf("amount = %s, %v", amount, err)
		}
		encoded, err := json.Marshal(m)
		if err != nil || string(encoded) != `{"amount":"`+tc.amount+`","currency":"BRL"}` {
			t.Fatalf("JSON = %s, %v", encoded, err)
		}
		var decoded Money
		err = json.Unmarshal(encoded, &decoded)
		if tc.minor < 0 {
			if !errors.Is(err, ErrNegativeAmount) {
				t.Fatal("external JSON accepted a negative amount")
			}
		} else if err != nil || decoded != m {
			t.Fatalf("round trip: %v", err)
		}
	}
	for _, input := range []string{`null`, `{}`, `{"amount":25,"currency":"BRL"}`, `{"amount":"1.00"}`, `{"amount":null,"currency":"BRL"}`, `{"amount":"-1.00","currency":"BRL"}`, `{"amount":"1.00","currency":"ZZZ"}`, `{`} {
		m := mustMoney(t, 123, "BRL")
		if err := json.Unmarshal([]byte(input), &m); err == nil {
			t.Fatalf("accepted %s", input)
		}
		if m.minor != 123 || m.currency != "BRL" {
			t.Fatal("failed decoding mutated receiver")
		}
	}
	var pointer *Money
	if err := pointer.UnmarshalJSON([]byte(`{}`)); !errors.Is(err, ErrInvalidAmount) {
		t.Fatal(err)
	}
}

// TestInvalidZeroValueAndCurrencyMismatch verifies validation across public operations and valid currency-specific zeros.
func TestInvalidZeroValueAndCurrencyMismatch(t *testing.T) {
	var invalid Money
	valid := mustMoney(t, 0, "BRL")
	usd := mustMoney(t, 0, "USD")
	for _, pair := range [][2]Money{{invalid, valid}, {valid, invalid}, {invalid, invalid}, {valid, usd}} {
		want := ErrInvalidCurrency
		if pair[0] == valid && pair[1] == usd {
			want = ErrCurrencyMismatch
		}
		if _, err := pair[0].Add(pair[1]); !errors.Is(err, want) {
			t.Fatal(err)
		}
		if _, err := pair[0].Sub(pair[1]); !errors.Is(err, want) {
			t.Fatal(err)
		}
		if _, err := pair[0].Compare(pair[1]); !errors.Is(err, want) {
			t.Fatal(err)
		}
	}
	if _, err := invalid.Minor(); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatal(err)
	}
	if _, err := invalid.Currency(); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatal(err)
	}
	if _, err := invalid.IsZero(); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatal(err)
	}
	if _, err := invalid.Amount(); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatal(err)
	}
	if _, err := invalid.Negate(); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatal(err)
	}
	if _, err := json.Marshal(invalid); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatal(err)
	}
	zero, err := ZeroMoney("USD")
	if err != nil {
		t.Fatal(err)
	}
	if isZero, err := zero.IsZero(); err != nil || !isZero {
		t.Fatal("invalid zero")
	}
	if minor, err := zero.Minor(); err != nil || minor != 0 {
		t.Fatal("invalid minor units")
	}
	if currency, err := zero.Currency(); err != nil || currency != "USD" {
		t.Fatal("invalid currency")
	}
	if _, err := ZeroMoney(""); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatal(err)
	}
}
