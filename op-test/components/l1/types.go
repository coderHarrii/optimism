package l1

import (
	"math/big"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"

	test "github.com/ethereum-optimism/optimism/op-test"
)

type L1Fork string

const (
	Shapella L1Fork = "shapella"
	Dencun   L1Fork = "dencun"
)

func (f L1Fork) String() string {
	return string(f)
}

var Forks = []L1Fork{
	Shapella,
	Dencun,
}

type L1 interface {
	ChainID() *big.Int
	ChainConfig() *params.ChainConfig
	Signer() types.Signer
	TargetBlockTime() uint64
	NetworkName() string
	GenesisELHeader() *types.Header
}

func Request(t test.Testing, opts ...Option) L1 {
	var settings Settings
	for i, opt := range opts {
		require.NoError(t, opt.Apply(&settings), "must apply option %d", i)
	}
	switch settings.Kind {
	case test.Live:
		return nil
	case test.Managed:
		return nil
	case test.Instant:
		return nil
	default:
		return nil
	}
}