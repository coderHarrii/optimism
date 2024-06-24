package sequencing

import (
	"context"
	"errors"
	"fmt"
	"github.com/ethereum-optimism/optimism/op-node/rollup/conductor"
	"github.com/ethereum-optimism/optimism/op-node/rollup/engine"
	"github.com/ethereum-optimism/optimism/op-service/sema"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// sealingDuration defines the expected time it takes to seal the block
const sealingDuration = time.Millisecond * 50

var (
	ErrSequencerAlreadyStarted = errors.New("sequencer already running")
	ErrSequencerAlreadyStopped = errors.New("sequencer not running")
)

type Downloader interface {
	InfoByHash(ctx context.Context, hash common.Hash) (eth.BlockInfo, error)
	FetchReceipts(ctx context.Context, blockHash common.Hash) (eth.BlockInfo, types.Receipts, error)
}

type L1OriginSelectorIface interface {
	FindL1Origin(ctx context.Context, l2Head eth.L2BlockRef) (eth.L1BlockRef, error)
}

type SequencerMetrics interface {
	RecordSequencerInconsistentL1Origin(from eth.BlockID, to eth.BlockID)
	RecordSequencerReset()
}

type SequencerStateListener interface {
	SequencerStarted() error
	SequencerStopped() error
}

type AsyncGossiper interface {
	Gossip(payload *eth.ExecutionPayloadEnvelope)
	Get() *eth.ExecutionPayloadEnvelope
	Clear()
	Stop()
	Start()
}

// SequencerActionEvent triggers the sequencer to start/seal a block, if active and ready to act.
// This event is used to prioritize sequencer work over derivation work,
// by emitting it before e.g. a derivation-pipeline step.
// A future sequencer in an async world may manage its own execution.
type SequencerActionEvent struct {
}

func (ev SequencerActionEvent) String() string {
	return "sequencer-action"
}

type BuildingState struct {
	Onto eth.L2BlockRef
	Info eth.PayloadInfo
}

type EngineBuildingUpdateEvent struct {
	Unsafe      eth.L2BlockRef
	PendingSafe eth.L2BlockRef

	Building BuildingState
}

func (ev EngineBuildingUpdateEvent) String() string {
	return "engine-building-event"
}

// Sequencer implements the sequencing interface of the driver: it starts and completes block building jobs.
type Sequencer struct {
	l sema.Lock

	// closed when driver system closes, to interrupt any ongoing API calls etc.
	ctx context.Context

	log       log.Logger
	rollupCfg *rollup.Config
	spec      *rollup.ChainSpec

	maxSafeLag atomic.Uint64

	active atomic.Bool

	// listener for sequencer-state changes. Blocking, may error.
	// May be used to ensure sequencer-state is accurately persisted.
	listener SequencerStateListener

	conductor conductor.SequencerConductor

	asyncGossip AsyncGossiper

	emitter rollup.EventEmitter

	attrBuilder      derive.AttributesBuilder
	l1OriginSelector L1OriginSelectorIface

	metrics SequencerMetrics

	// timeNow enables sequencer testing to mock the time
	timeNow func() time.Time

	// nextAction is when the next sequencing action should be performed
	nextAction   time.Time
	nextActionOK bool

	latest     BuildingState
	latestHead eth.L2BlockRef
}

var _ SequencerIface = (*Sequencer)(nil)

func NewSequencer(driverCtx context.Context, log log.Logger, rollupCfg *rollup.Config,
	attributesBuilder derive.AttributesBuilder,
	l1OriginSelector L1OriginSelectorIface,
	listener SequencerStateListener,
	conductor conductor.SequencerConductor,
	asyncGossip AsyncGossiper,
	metrics SequencerMetrics) *Sequencer {
	return &Sequencer{
		ctx:              driverCtx,
		log:              log,
		rollupCfg:        rollupCfg,
		spec:             rollup.NewChainSpec(rollupCfg),
		listener:         listener,
		conductor:        conductor,
		timeNow:          time.Now,
		attrBuilder:      attributesBuilder,
		l1OriginSelector: l1OriginSelector,
		metrics:          metrics,
	}
}

func (d *Sequencer) OnEvent(ev rollup.Event) {
	d.l.Lock()
	defer d.l.Unlock()

	switch x := ev.(type) {
	// TODO on payload job created
	case engine.GotPayloadEvent:
		if d.latest.Info != x.Info {
			return // not our payload, can be ignored.
		}
		d.asyncGossip.Gossip(x.Envelope)
		// TODO try to process as canonical
	case EngineBuildingUpdateEvent:
		d.log.Debug("Sequencer received block-building update from engine",
			"payloadID", x.Building.ID, "onto", x.Building.Onto, "head", x.Unsafe)
		// adjust planning of next sequencer action
		d.planNextSequencerAction(x)
		d.latest = x.Building
	case SequencerActionEvent:
		// TODO based on last perceived building state, command engine

		if d.latest.Info != (eth.PayloadInfo{}) {
			payload := d.asyncGossip.Get()
			if payload == nil {
				// no known payload, we have to retrieve it first.
				d.emitter.Emit(engine.GetPayloadEvent{Info: d.latest.Info})
			} else {
				// Payload is known, meaning that we have seen GotPayloadEvent already.
				// We can retry processing and making it canonical.
				// TODO
			}
		} else {
			d.startBuildingBlock()
		}
	case engine.InvalidPayloadAttributesEvent:
		// TODO
	case engine.InvalidPayloadEvent:
		// TODO
	case rollup.EngineTemporaryErrorEvent:
		d.log.Error("Engine failed temporarily, backing off sequencer", "err", x.Err)
		d.nextAction = d.timeNow().Add(time.Second)
		// We don't explicitly cancel block building jobs upon temporary errors: we may still finish the block (if any).
		// Any unfinished block building work eventually times out, and will be cleaned up that way.
	case rollup.ResetEvent:
		d.nextActionOK = false
		d.log.Error("Sequencer encountered reset signal, aborting work", "err", x.Err)
		d.metrics.RecordSequencerReset()

		// try to cancel any ongoing payload building job
		if d.latest.Info != (eth.PayloadInfo{}) {
			d.emitter.Emit(engine.CancelBuildingEvent{ID: d.latest.Info})
		}
	case engine.EngineResetConfirmedEvent:
		d.log.Info("Engine reset confirmed, sequencer may continue")
		d.nextActionOK = d.active.Load()
		d.nextAction = d.timeNow().Add(time.Second * time.Duration(d.rollupCfg.BlockTime))
	case engine.PendingSafeUpdateEvent:
		if x.Unsafe == x.PendingSafe { // if we are forcing new blocks onto the tip of the chain, don't try to build on top of it immediately
			onto := x.Unsafe
			d.log.Warn("Avoiding sequencing to not interrupt safe-head changes", "onto", onto, "onto_time", onto.Time)
			// approximates the worst-case time it takes to build a block, to reattempt sequencing after.
			d.nextAction = d.timeNow().Add(time.Second * time.Duration(d.rollupCfg.BlockTime))
		}
	case engine.ForkchoiceUpdateEvent:
		// If the safe head has fallen behind by a significant number of blocks, delay creating new blocks
		// until the safe lag is below SequencerMaxSafeLag.
		if maxSafeLag := d.maxSafeLag.Load(); maxSafeLag > 0 && x.SafeL2Head.Number+maxSafeLag <= x.UnsafeL2Head.Number {
			d.nextActionOK = false
		}
		// Drop stale block-building job if the chain has moved past it already.
		// TODO: cancel job if still ongoing
		if d.latest.Onto.Number < x.UnsafeL2Head.Number {
			d.latest = BuildingState{}
		}
	}
}

// TODO logs to add:
//d.log.Info("sequencer successfully built a new block", "block", payload.ID(), "time", uint64(payload.Timestamp), "txs", len(payload.Transactions))
//d.log.Info("sequencer started building new block", "payload_id", buildingID, "l2_parent_block", parent, "l2_parent_block_time", parent.Time)

// StartBuildingBlock initiates a block building job on top of the given L2 head, safe and finalized blocks, and using the provided l1Origin.
func (d *Sequencer) startBuildingBlock() {
	ctx := d.ctx
	l2Head := d.latestHead

	// Figure out which L1 origin block we're going to be building on top of.
	l1Origin, err := d.l1OriginSelector.FindL1Origin(ctx, l2Head)
	if err != nil {
		d.log.Error("Error finding next L1 Origin", "err", err)
		d.emitter.Emit(rollup.L1TemporaryErrorEvent{Err: err})
		return
	}

	if !(l2Head.L1Origin.Hash == l1Origin.ParentHash || l2Head.L1Origin.Hash == l1Origin.Hash) {
		d.metrics.RecordSequencerInconsistentL1Origin(l2Head.L1Origin, l1Origin.ID())
		d.emitter.Emit(rollup.ResetEvent{Err: fmt.Errorf("cannot build new L2 block with L1 origin %s (parent L1 %s) on current L2 head %s with L1 origin %s",
			l1Origin, l1Origin.ParentHash, l2Head, l2Head.L1Origin)})
		return
	}

	d.log.Info("creating new block", "parent", l2Head, "l1Origin", l1Origin)

	fetchCtx, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()

	attrs, err := d.attrBuilder.PreparePayloadAttributes(fetchCtx, l2Head, l1Origin.ID())
	if err != nil {
		if errors.Is(err, derive.ErrTemporary) {
			d.emitter.Emit(rollup.EngineTemporaryErrorEvent{Err: err})
			return
		} else if errors.Is(err, derive.ErrReset) {
			d.emitter.Emit(rollup.ResetEvent{Err: err})
			return
		} else if errors.Is(err, derive.ErrCritical) {
			d.emitter.Emit(rollup.CriticalErrorEvent{Err: err})
			return
		} else {
			d.emitter.Emit(rollup.CriticalErrorEvent{Err: fmt.Errorf("unexpected attributes-preparation error: %w", err)})
			return
		}
	}

	// If our next L2 block timestamp is beyond the Sequencer drift threshold, then we must produce
	// empty blocks (other than the L1 info deposit and any user deposits). We handle this by
	// setting NoTxPool to true, which will cause the Sequencer to not include any transactions
	// from the transaction pool.
	attrs.NoTxPool = uint64(attrs.Timestamp) > l1Origin.Time+d.spec.MaxSequencerDrift(l1Origin.Time)

	// For the Ecotone activation block we shouldn't include any sequencer transactions.
	if d.rollupCfg.IsEcotoneActivationBlock(uint64(attrs.Timestamp)) {
		attrs.NoTxPool = true
		d.log.Info("Sequencing Ecotone upgrade block")
	}

	// For the Fjord activation block we shouldn't include any sequencer transactions.
	if d.rollupCfg.IsFjordActivationBlock(uint64(attrs.Timestamp)) {
		attrs.NoTxPool = true
		d.log.Info("Sequencing Fjord upgrade block")
	}

	d.log.Debug("prepared attributes for new block",
		"num", l2Head.Number+1, "time", uint64(attrs.Timestamp),
		"origin", l1Origin, "origin_time", l1Origin.Time, "noTxPool", attrs.NoTxPool)

	// Start a payload building process.
	withParent := &derive.AttributesWithParent{Attributes: attrs, Parent: l2Head, IsLastInSpan: false}

	// TODO engine event handler builds this on top of pending safe head, not l2Head
	d.emitter.Emit(engine.ProcessAttributesEvent{
		Attributes: withParent,
	})
}

// planNextSequencerAction updates the nextAction time.
func (d *Sequencer) planNextSequencerAction(x EngineBuildingUpdateEvent) {
	// If not running, don't plan any sequencer action
	if !d.active.Load() {
		d.nextActionOK = false
		return
	}

	buildingOnto, buildingID := x.Building.Onto, x.Building.Info.ID
	// If the engine is busy building safe blocks (and thus changing the head that we would sync on top of),
	// then give it time to sync up.
	if x.Unsafe == x.PendingSafe {
		d.log.Warn("delaying sequencing to not interrupt safe-head changes", "onto", buildingOnto, "onto_time", buildingOnto.Time)
		// approximates the worst-case time it takes to build a block, to reattempt sequencing after.
		d.nextAction = d.timeNow().Add(time.Second * time.Duration(d.rollupCfg.BlockTime))
		return
	}

	head := x.Unsafe
	now := d.timeNow()

	// If the head changed we need to respond and will not delay the sequencing.
	if buildingOnto.Hash == head.Hash {
		d.nextAction = now
		return
	}
	// We may have to wait till the next sequencing action, e.g. upon an error.
	// TODO

	blockTime := time.Duration(d.rollupCfg.BlockTime) * time.Second
	payloadTime := time.Unix(int64(head.Time+d.rollupCfg.BlockTime), 0)
	remainingTime := payloadTime.Sub(now)

	// If we started building a block already, and if that work is still consistent,
	// then we would like to finish it by sealing the block.
	if buildingID != (eth.PayloadID{}) && buildingOnto.Hash == head.Hash {
		// if we started building already, then we will schedule the sealing.
		if remainingTime < sealingDuration {
			d.nextAction = now // if there's not enough time for sealing, don't wait.
		} else {
			// finish with margin of sealing duration before payloadTime
			d.nextAction = payloadTime.Add(-sealingDuration)
		}
	} else {
		// if we did not yet start building, then we will schedule the start.
		if remainingTime > blockTime {
			// if we have too much time, then wait before starting the build
			d.nextAction = payloadTime.Add(-blockTime)
		} else {
			// otherwise start instantly
			d.nextAction = now
		}
	}
}

func (d *Sequencer) NextAction() (t time.Time, ok bool) {
	d.l.Lock()
	defer d.l.Unlock()
	return d.nextAction, d.nextActionOK
}

func (d *Sequencer) Active() bool {
	return d.active.Load()
}

func (d *Sequencer) Start(ctx context.Context, head common.Hash) error {
	// must be leading to activate
	if isLeader, err := d.conductor.Leader(ctx); err != nil {
		return fmt.Errorf("sequencer leader check failed: %w", err)
	} else if !isLeader {
		return errors.New("sequencer is not the leader, aborting")
	}

	// Note: leader check happens before locking; this is how the Driver used to work,
	// and prevents the event-processing of the sequencer from being stalled due to a potentially slow conductor call.
	if err := d.l.LockCtx(ctx); err != nil {
		return err
	}
	defer d.l.Unlock()

	if !d.active.Load() {
		return ErrSequencerAlreadyStarted
	}
	if d.latestHead == (eth.L2BlockRef{}) {
		return fmt.Errorf("no prestate, cannot determine if sequencer start at %s is safe", head)
	}
	if head != d.latestHead.Hash {
		return fmt.Errorf("block hash does not match: head %s, received %s", d.latestHead, head)
	}
	return d.forceStart()
}

func (d *Sequencer) Init(ctx context.Context, active bool) error {
	d.l.Lock()
	defer d.l.Unlock()

	d.asyncGossip.Start()

	if active {
		// TODO: should the conductor be checked on startup?
		// The conductor was previously not being checked in this case, but that may be a bug.
		return d.forceStart()
	} else {
		if err := d.listener.SequencerStopped(); err != nil {
			return fmt.Errorf("failed to notify sequencer-state listener of initial stopped state: %w", err)
		}
		return nil
	}
}

// forceStart skips all the checks, and just starts the sequencer
func (d *Sequencer) forceStart() error {
	if err := d.listener.SequencerStarted(); err != nil {
		return fmt.Errorf("failed to notify sequencer-state listener of start: %w", err)
	}
	d.nextActionOK = true
	d.active.Store(true)
	d.log.Info("Sequencer has been started")
	return nil
}

func (d *Sequencer) Stop(ctx context.Context) (hash common.Hash, err error) {
	if err := d.l.LockCtx(ctx); err != nil {
		return common.Hash{}, err
	}
	defer d.l.Unlock()

	if d.active.Load() {
		return common.Hash{}, ErrSequencerAlreadyStopped
	}

	if err := d.listener.SequencerStopped(); err != nil {
		return common.Hash{}, fmt.Errorf("failed to notify sequencer-state listener of stop: %w", err)
	}

	// Cancel any inflight block building. If we don't cancel this, we can resume sequencing an old block
	// even if we've received new unsafe heads in the interim, causing us to introduce a re-org.
	d.latest = BuildingState{} // By wiping this state we cannot continue from it later.

	d.nextActionOK = false
	d.active.Store(false)
	d.log.Info("Sequencer has been stopped")
	return d.latestHead.Hash, nil
}

func (d *Sequencer) SetMaxSafeLag(ctx context.Context, v uint64) error {
	d.maxSafeLag.Store(v)
	return nil
}

func (d *Sequencer) Close() {
	d.conductor.Close()
	d.asyncGossip.Stop()
}
