/**
 * Minimal AI Agent + DSCK Integration Example
 * 
 * Why pair AI with DSCK?
 * 1. AI models are great at understanding human text, but BAD at exact math / formula calculations.
 * 2. DSCK handles exact math, formulas, and graph re-computation with 100% precision and zero hallucinations.
 * 3. DSCK's confidenceScore gate automatically protects against low-confidence AI predictions!
 */

import {
  DSCKernel,
  BoundedLRUMemoryManager,
  InMemoryColdStoreDriver,
  GraphBuilder,
  EventStreamEmitter
} from "../src/kernel";

// ----------------------------------------------------------------------------
// 1. Initialize DSCK Kernel
// ----------------------------------------------------------------------------
const eventEmitter = new EventStreamEmitter();
eventEmitter.on("commit", (evt) => {
  console.log(`\n⚡ [DSCK Kernel Event] State updated for session '${evt.sessionId}':`);
  console.log("   Diff:", JSON.stringify(evt.stateDiff));
});

eventEmitter.on("jit_fallback", (evt) => {
  console.log(`\n⚠️ [DSCK Safety Gate] Low confidence write rejected! Fallback required for node '${evt.packet.targetNode}'.`);
});

const kernel = new DSCKernel(
  new BoundedLRUMemoryManager(10, new InMemoryColdStoreDriver()),
  { confidenceThreshold: 0.75, eventEmitter }
);

// ----------------------------------------------------------------------------
// 2. Setup Graph Formulas (Order Total = price * qty - discount)
// ----------------------------------------------------------------------------
async function setupOrderGraph(sessionId: string) {
  const builder = new GraphBuilder(kernel, sessionId);

  await builder.addNode("item_price", 20);
  await builder.addNode("qty", 1);
  await builder.addNode("discount_percent", 0);

  // subtotal = item_price * qty
  await builder.addFormula("subtotal", {
    type: "BINARY_OP",
    op: "MUL",
    left: { type: "REF", nodeId: "item_price" },
    right: { type: "REF", nodeId: "qty" }
  });

  // final_total = subtotal * (1 - discount_percent / 100)
  await builder.addFormula("final_total", {
    type: "BINARY_OP",
    op: "MUL",
    left: { type: "REF", nodeId: "subtotal" },
    right: {
      type: "BINARY_OP",
      op: "SUB",
      left: { type: "LITERAL", value: 1 },
      right: {
        type: "BINARY_OP",
        op: "DIV",
        left: { type: "REF", nodeId: "discount_percent" },
        right: { type: "LITERAL", value: 100 }
      }
    }
  });
}

// ----------------------------------------------------------------------------
// 3. Simulated Lightweight AI Text Parser (Extracts Target Node, Value & Confidence)
// ----------------------------------------------------------------------------
interface AIParsedIntent {
  targetNode: string;
  value: number;
  confidence: number;
}

function processUserTextInput(text: string): AIParsedIntent {
  console.log(`\n🗣️ User Text: "${text}"`);

  const lower = text.toLowerCase();
  
  if (lower.includes("price") || lower.includes("cost")) {
    const match = text.match(/(\d+(\.\d+)?)/);
    const value = match ? parseFloat(match[1]) : 20;
    return { targetNode: "item_price", value, confidence: 0.95 };
  }
  
  if (lower.includes("quantity") || lower.includes("items") || lower.includes("buy")) {
    const match = text.match(/(\d+)/);
    const value = match ? parseInt(match[1], 10) : 1;
    return { targetNode: "qty", value, confidence: 0.92 };
  }

  if (lower.includes("discount")) {
    const match = text.match(/(\d+)/);
    const value = match ? parseInt(match[1], 10) : 10;
    return { targetNode: "discount_percent", value, confidence: 0.88 };
  }

  // Ambiguous text -> Low confidence AI prediction!
  return { targetNode: "item_price", value: 0, confidence: 0.40 };
}

// ----------------------------------------------------------------------------
// 4. Run Demonstration
// ----------------------------------------------------------------------------
async function runAIDemo() {
  const sessionId = "ai-user-session";
  await setupOrderGraph(sessionId);

  console.log("=== AI + DSCK Reactive Kernel Integration Demo ===");

  // Text 1: User changes price
  const intent1 = processUserTextInput("Set the item price to $45.50");
  await kernel.executeAtomicTick({
    sessionId,
    opcode: "MUTATE",
    targetNode: intent1.targetNode,
    payload: intent1.value,
    confidenceScore: intent1.confidence
  });

  // Text 2: User changes quantity
  const intent2 = processUserTextInput("I want to buy 4 items");
  await kernel.executeAtomicTick({
    sessionId,
    opcode: "MUTATE",
    targetNode: intent2.targetNode,
    payload: intent2.value,
    confidenceScore: intent2.confidence
  });

  // Text 3: User applies discount
  const intent3 = processUserTextInput("Apply 20% discount code");
  await kernel.executeAtomicTick({
    sessionId,
    opcode: "MUTATE",
    targetNode: intent3.targetNode,
    payload: intent3.value,
    confidenceScore: intent3.confidence
  });

  // Text 4: Ambiguous user text (Triggers DSCK confidence safety fallback)
  const intent4 = processUserTextInput("maybe change something weird");
  const fallbackRes = await kernel.executeAtomicTick({
    sessionId,
    opcode: "MUTATE",
    targetNode: intent4.targetNode,
    payload: intent4.value,
    confidenceScore: intent4.confidence
  });

  console.log(`\nResult of ambiguous text: status = '${fallbackRes.status}' (rejection score: ${intent4.confidence} < 0.75 threshold)`);
}

runAIDemo();
