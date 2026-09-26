/**
 * Stress benchmark: run with `npm run bench`.
 * Measures real numbers instead of asserting made-up ones.
 */
import {
  DSCKernel,
  BoundedLRUMemoryManager,
  InMemoryColdStoreDriver,
  StateNode,
  ASTExpression,
} from "../src/kernel";

function refAST(nodeId: string): ASTExpression {
  return { type: "REF", nodeId };
}

async function buildWideGraph(kernel: DSCKernel, sessionId: string, width: number) {
  const mm = (kernel as any).memoryManager as BoundedLRUMemoryManager;
  const session = await mm.getSession(sessionId);

  // root -> [n1..nWidth] each with a formula depending on root, and one
  // final "sink" node depending on all of them (fan-out then fan-in).
  const root: StateNode = { id: "root", value: 1, updatedAtTick: 0, accessCount: 0, dependents: [] };
  session.nodes.set("root", root);

  const leafExprs: ASTExpression[] = [];
  for (let i = 0; i < width; i++) {
    const id = `n${i}`;
    root.dependents.push(id);
    session.nodes.set(id, {
      id,
      value: 0,
      updatedAtTick: 0,
      accessCount: 0,
      dependents: ["sink"],
      formulaAST: { type: "BINARY_OP", op: "MUL", left: refAST("root"), right: { type: "LITERAL", value: i + 1 } },
    });
    leafExprs.push(refAST(id));
  }
  // Combine leaves with a balanced binary tree so AST depth is O(log width)
  // rather than O(width) — the sandbox caps recursion depth at 32 by design.
  function balancedSum(exprs: ASTExpression[]): ASTExpression {
    if (exprs.length === 1) return exprs[0];
    const mid = Math.floor(exprs.length / 2);
    return {
      type: "BINARY_OP",
      op: "ADD",
      left: balancedSum(exprs.slice(0, mid)),
      right: balancedSum(exprs.slice(mid)),
    };
  }
  const sinkExpr: ASTExpression = balancedSum(leafExprs);
  root.dependents.push("sink");
  session.nodes.set("sink", {
    id: "sink",
    value: 0,
    updatedAtTick: 0,
    accessCount: 0,
    dependents: [],
    formulaAST: sinkExpr,
  });
}

async function benchGraphSize(width: number) {
  const kernel = new DSCKernel(new BoundedLRUMemoryManager(10, new InMemoryColdStoreDriver()));
  const sessionId = `bench-${width}`;
  await buildWideGraph(kernel, sessionId, width);

  const iterations = 50;
  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    const result = await kernel.executeAtomicTick({
      sessionId,
      opcode: "MUTATE",
      targetNode: "root",
      payload: i + 1,
    });
    if (result.status !== "COMMITTED") throw new Error(`tick failed: ${result.error}`);
  }
  const elapsed = performance.now() - start;
  console.log(
    `graph width=${width.toString().padStart(5)} nodes  | ${iterations} ticks | ` +
      `${elapsed.toFixed(2)}ms total | ${(elapsed / iterations).toFixed(4)}ms/tick`
  );
}

async function benchConcurrentTicks(count: number) {
  const kernel = new DSCKernel(new BoundedLRUMemoryManager(50, new InMemoryColdStoreDriver()));
  const start = performance.now();

  const jobs = Array.from({ length: count }, (_, i) =>
    kernel.executeAtomicTick({
      sessionId: `sess-${i % 20}`, // 20 distinct sessions sharing the mutex pool
      opcode: "MUTATE",
      targetNode: "value",
      payload: i,
    })
  );
  const results = await Promise.all(jobs);
  const elapsed = performance.now() - start;
  const committed = results.filter((r) => r.status === "COMMITTED").length;

  console.log(
    `${count} concurrent ticks across 20 sessions | ${committed}/${count} committed | ` +
      `${elapsed.toFixed(2)}ms total | ${(elapsed / count).toFixed(4)}ms/tick`
  );
}

async function main() {
  console.log("--- DSCK stress benchmark ---\n");
  console.log("Wide fan-out/fan-in graphs (single session, sequential ticks):");
  for (const width of [10, 100, 1000, 5000]) {
    await benchGraphSize(width);
  }

  console.log("\nConcurrent ticks (shared mutex, multiple sessions):");
  for (const count of [100, 1000, 10000]) {
    await benchConcurrentTicks(count);
  }

  console.log("\nDone. These are real numbers from this machine, not marketing copy —");
  console.log("re-run with `npm run bench` any time to reproduce.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
