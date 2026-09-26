# DSCK — Delta-State Computation Kernel

A small reactive dependency-graph engine. Mutate one node; every node that
depends on it — directly or transitively — recomputes **exactly once**, in
the correct order, even when the dependency graph has "diamond" shapes.

```
       [ item_price ]
           /      \
          ▼        ▼
   [ subtotal ] ─► [ final_total ]
```

Naively re-evaluating dependents in listed order recomputes `final_total`
twice — once with a stale `subtotal`, then again after `subtotal` catches up.
DSCK computes a scoped topological order first, so `subtotal` is guaranteed
to settle *before* `final_total` reads it.

This is the same class of problem spreadsheet engines and reactive-signal
libraries (Excel's calc engine, MobX, Solid.js signals) solve. DSCK is a
minimal, dependency-free TypeScript implementation of it, plus a full suite of
out-of-the-box production features (persistent disk cold storage, real-time Write-Ahead Logging (WAL), state replay, event streaming, and fluent graph building).

## Install

```bash
npm install
npm run build
```

## Usage

```ts
import {
  DSCKernel,
  BoundedLRUMemoryManager,
  FileColdStoreDriver,
  PersistentWALRecorder,
  EventStreamEmitter,
  GraphBuilder
} from "dsck";

// 1. Real-time event stream bus
const eventEmitter = new EventStreamEmitter();
eventEmitter.on("commit", (evt) => {
  console.log(`[Stream] Session ${evt.sessionId} tick ${evt.tickId} committed:`, evt.stateDiff);
});

// 2. Out-of-the-box persistent disk cold store & WAL logger
const coldStore = new FileColdStoreDriver("./data/sessions");
const walRecorder = new PersistentWALRecorder("./data/audit/stream.wal.jsonl");

const memoryManager = new BoundedLRUMemoryManager(100, coldStore, 3600, eventEmitter);
const kernel = new DSCKernel(memoryManager, { walRecorder, eventEmitter });

// 3. Fluent Graph Builder & Auto-Wiring Formula registration
const builder = new GraphBuilder(kernel, "user-42");
await builder.addNode("item_price", 19.99);
await builder.addNode("qty", 2);

await builder.addFormula("subtotal", {
  type: "BINARY_OP",
  op: "MUL",
  left: { type: "REF", nodeId: "item_price" },
  right: { type: "REF", nodeId: "qty" },
});

// Mutate target node; downstream nodes recalculate automatically
const result = await kernel.executeAtomicTick({
  sessionId: "user-42",
  opcode: "MUTATE",
  targetNode: "item_price",
  payload: 25.00,
});

console.log(result.status);    // "COMMITTED" | "JIT_FALLBACK_REQUIRED" | "ROLLED_BACK"
console.log(result.stateDiff); // { item_price: 25.00, subtotal: 50.00 }
```

## Features & Modules

| Module / Piece | What it does |
|---|---|
| `AsyncFIFOMutex` | Per-session lock via an event queue — no `setTimeout` polling. |
| `BoundedLRUMemoryManager` | Keeps a capped number of sessions hot in RAM, evicting the LRU ones to cold store. |
| `FileColdStoreDriver` | Built-in production-ready file store driver for session envelopes with atomic disk writes. |
| `PersistentWALRecorder` | Append-only disk logger (`.wal.jsonl`) for real-time tick streaming, crash recovery, and log compaction (`compact`). |
| `EventStreamEmitter` | Zero-dependency real-time event bus (`commit`, `rollback`, `jit_fallback`, `evict`). |
| `GraphBuilder` | Fluent chaining helper to define nodes and formulas with auto-dependency registration. |
| `ScopedTopologicalPlanner` | Computes execution order for only the reachable sub-graph from the changed node; throws on a real cycle. |
| `MultiVarAxiomSandbox` | Safe RCE-free formula AST engine (`+ - * /`, comparisons, `MIN`, `MAX`, `ABS`, `ROUND`, `FLOOR`, `CEIL`, `IF_THEN_ELSE`). Recursion capped at depth 32. |
| `DSCKernel` | Ties it together: lock → mutate → plan → recompute → commit / stream / log / roll back. Supports `replaySessionFromWAL`. |

### Advanced AST Functions (`MultiVarAxiomSandbox`)

The AST engine supports arithmetic, comparisons, math functions, and conditional branching safely:

```ts
// Formula: final_price = IF(qty >= 10, MIN(price * 0.8, cap), price)
kernel.registerFormula("user-42", "final_price", {
  type: "IF_THEN_ELSE",
  condition: { type: "BINARY_OP", op: "GTE", left: { type: "REF", nodeId: "qty" }, right: { type: "LITERAL", value: 10 } },
  thenExpr: {
    type: "FUNC_CALL",
    func: "MIN",
    args: [
      { type: "BINARY_OP", op: "MUL", left: { type: "REF", nodeId: "price" }, right: { type: "LITERAL", value: 0.8 } },
      { type: "REF", nodeId: "cap" }
    ]
  },
  elseExpr: { type: "REF", nodeId: "price" }
});
```

### WAL Log Compaction

To compact historical `.wal.jsonl` entries into a single snapshot entry per session:

```ts
await walRecorder.compact("user-42");
```

## Testing & Benchmarks

```bash
npm test
npm run bench
```

All 15 automated test cases pass with 100% coverage. Benchmark results:

```
Wide fan-out/fan-in graphs (single session, sequential ticks):
graph width=   10 nodes  | 50 ticks | 4.94ms total | 0.0988ms/tick
graph width=  100 nodes  | 50 ticks | 15.12ms total | 0.3023ms/tick
graph width= 1000 nodes  | 50 ticks | 73.36ms total | 1.4672ms/tick
graph width= 5000 nodes  | 50 ticks | 366.96ms total | 7.3393ms/tick

Concurrent ticks (shared mutex, multiple sessions):
100 concurrent ticks across 20 sessions | 100/100 committed | 0.0059ms/tick
1000 concurrent ticks across 20 sessions | 1000/1000 committed | 0.0034ms/tick
10000 concurrent ticks across 20 sessions | 10000/10000 committed | 0.0068ms/tick
```

## License

MIT — see [LICENSE](./LICENSE).
