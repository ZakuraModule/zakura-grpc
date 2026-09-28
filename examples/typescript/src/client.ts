import {
  Client,
  ClientDuplexStream,
  credentials,
  loadPackageDefinition,
  Metadata,
  ServiceError,
  status,
} from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { parseArgs } from "node:util";
import { fileURLToPath } from "node:url";

type Message = Record<string, unknown>;
type UnaryMethod = (
  request: Message,
  metadata: Metadata,
  callback: (error: ServiceError | null, response?: Message) => void,
) => void;

interface GeyserClient extends Client {
  getVersion: UnaryMethod;
  subscribeReplayInfo: UnaryMethod;
  subscribe(metadata: Metadata): ClientDuplexStream<Message, Message>;
}

interface GeyserConstructor {
  new (
    address: string,
    channelCredentials: ReturnType<typeof credentials.createInsecure>,
  ): GeyserClient;
}

interface ProtoRoot {
  zakura: { geyser: { v1: { Geyser: GeyserConstructor } } };
}

interface CanonicalBlock {
  height: number;
  hash: string;
  previousBlockHash: string;
}

interface FinalizedCheckpoint {
  height: number;
  hash: string;
}

const EVENT_NAMES: Record<string, string> = {
  "block-accepted": "EVENT_TYPE_BLOCK_ACCEPTED",
  "best-chain-changed": "EVENT_TYPE_BEST_CHAIN_CHANGED",
  "block-finalized": "EVENT_TYPE_BLOCK_FINALIZED",
  "mempool-changed": "EVENT_TYPE_MEMPOOL_CHANGED",
  transaction: "EVENT_TYPE_TRANSACTION",
  utxo: "EVENT_TYPE_UTXO",
  "mempool-transaction": "EVENT_TYPE_MEMPOOL_TRANSACTION",
};

const protoPath = fileURLToPath(
  new URL(
    "../../../zakura-grpc-proto/proto/zakura/geyser/v1/geyser.proto",
    import.meta.url,
  ),
);
const packageDefinition = loadSync(protoPath, {
  defaults: true,
  enums: String,
  longs: String,
  oneofs: true,
});
const root = loadPackageDefinition(packageDefinition) as unknown as ProtoRoot;

const { values, positionals } = parseArgs({
  args: process.argv.slice(2),
  allowPositionals: true,
  options: {
    endpoint: { type: "string", default: "http://127.0.0.1:10000" },
    "x-token": { type: "string" },
    "subscription-id": { type: "string", default: "zakura-typescript-example" },
    event: { type: "string", multiple: true },
    "from-height": { type: "string" },
    "mempool-snapshot": { type: "boolean", default: false },
    "max-retries": { type: "string", default: "8" },
  },
});

const command = positionals[0] ?? "subscribe";
const endpoint = new URL(values.endpoint);
const channelCredentials =
  endpoint.protocol === "https:"
    ? credentials.createSsl()
    : credentials.createInsecure();
const client = new root.zakura.geyser.v1.Geyser(
  endpoint.host,
  channelCredentials,
);
const metadata = new Metadata();
const token = values["x-token"] ?? process.env.ZAKURA_GRPC_X_TOKEN;
if (token !== undefined) metadata.set("x-token", token);
metadata.set("x-subscription-id", values["subscription-id"]);

function print(message: Message): void {
  console.log(
    JSON.stringify(
      message,
      (_key, value: unknown) =>
        Buffer.isBuffer(value) ? value.toString("hex") : value,
      2,
    ),
  );
}

function unary(method: UnaryMethod): Promise<void> {
  return new Promise((resolve, reject) => {
    method({}, metadata, (error, response) => {
      if (error !== null) {
        reject(error);
        return;
      }
      print(response ?? {});
      resolve();
    });
  });
}

function asMessage(value: unknown): Message | undefined {
  return typeof value === "object" && value !== null
    ? (value as Message)
    : undefined;
}

function numberField(message: Message, name: string): number | undefined {
  const value = message[name];
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isSafeInteger(parsed)) return parsed;
  }
  return undefined;
}

function stringField(message: Message, name: string): string | undefined {
  const value = message[name];
  return typeof value === "string" ? value : undefined;
}

class ResumeState {
  finalized: FinalizedCheckpoint | undefined;
  readonly partial = new Map<string, CanonicalBlock>();
  latestHeight: number | undefined;
  private readonly seen = new Set<string>();
  private readonly seenByHeight = new Map<number, string[]>();

  observe(update: Message): boolean {
    const height = this.height(update);
    if (height !== undefined) {
      this.latestHeight = height;
      const session = update.sessionId;
      const sessionKey = Buffer.isBuffer(session) ? session.toString("hex") : String(session);
      const key = `${sessionKey}:${String(update.sequence)}`;
      if (this.seen.has(key)) return false;
      this.seen.add(key);
      const keys = this.seenByHeight.get(height) ?? [];
      keys.push(key);
      this.seenByHeight.set(height, keys);
      while (this.seenByHeight.size > 250) {
        const oldest = this.seenByHeight.keys().next().value as number | undefined;
        if (oldest === undefined) break;
        for (const oldKey of this.seenByHeight.get(oldest) ?? []) this.seen.delete(oldKey);
        this.seenByHeight.delete(oldest);
      }
    }

    const block = asMessage(update.block);
    if (block !== undefined) {
      const blockHeight = numberField(block, "height");
      const hash = stringField(block, "hash");
      if (blockHeight !== undefined && hash !== undefined) {
        if (block.finalized === true) {
          this.finalized = { height: blockHeight, hash };
          for (const [key, partial] of this.partial) {
            if (partial.height <= blockHeight) this.partial.delete(key);
          }
        } else {
          this.addPartial({ height: blockHeight, hash, previousBlockHash: "" });
        }
      }
    }

    for (const field of ["transaction", "utxo"]) {
      const payload = asMessage(update[field]);
      if (payload === undefined) continue;
      const blockHeight = numberField(payload, "height");
      const hash = stringField(payload, "blockHash");
      if (blockHeight === undefined || hash === undefined) continue;
      if (payload.commitment === "BLOCK_COMMITMENT_FINALIZED") {
        this.finalized = { height: blockHeight, hash };
        for (const [key, partial] of this.partial) {
          if (partial.height <= blockHeight) this.partial.delete(key);
        }
      } else {
        this.addPartial({ height: blockHeight, hash, previousBlockHash: "" });
      }
    }

    const bestChain = asMessage(update.bestChain);
    if (bestChain !== undefined) {
      const chainHeight = numberField(bestChain, "height");
      const hash = stringField(bestChain, "hash");
      const grow = asMessage(bestChain.grow);
      if (chainHeight !== undefined && hash !== undefined && grow !== undefined) {
        this.addPartial({
          height: chainHeight,
          hash,
          previousBlockHash: stringField(grow, "previousBlockHash") ?? "",
        });
      }
      const reset = asMessage(bestChain.reset);
      if (reset !== undefined) {
        for (const item of (reset.disconnectedBlocks as Message[] | undefined) ?? []) {
          const h = numberField(item, "height");
          const blockHash = stringField(item, "hash");
          if (h !== undefined && blockHash !== undefined) this.partial.delete(`${h}:${blockHash}`);
        }
        for (const item of (reset.connectedBlocks as Message[] | undefined) ?? []) {
          const h = numberField(item, "height");
          const blockHash = stringField(item, "hash");
          if (h !== undefined && blockHash !== undefined) {
            this.addPartial({
              height: h,
              hash: blockHash,
              previousBlockHash: stringField(item, "previousBlockHash") ?? "",
            });
          }
        }
      }
    }

    const reconnect = asMessage(update.reconnect);
    for (const item of (reconnect?.discardedBlocks as Message[] | undefined) ?? []) {
      const h = numberField(item, "height");
      const hash = stringField(item, "hash");
      if (h !== undefined && hash !== undefined) this.partial.delete(`${h}:${hash}`);
    }
    return true;
  }

  reconnectRequest(base: Message): Message {
    const request: Message = { ...base };
    if (this.finalized !== undefined) {
      request.fromHeight = this.finalized.height;
      request.resume = {
        finalizedHeight: this.finalized.height,
        finalizedBlockHash: this.finalized.hash,
        partialBlocks: [...this.partial.values()].sort(
          (left, right) => right.height - left.height,
        ),
      };
    } else if (this.latestHeight !== undefined) {
      request.fromHeight = Math.max(0, this.latestHeight - 2);
    }
    return request;
  }

  private addPartial(block: CanonicalBlock): void {
    if (this.finalized === undefined || block.height > this.finalized.height) {
      this.partial.set(`${block.height}:${block.hash}`, block);
    }
  }

  private height(update: Message): number | undefined {
    for (const field of ["block", "bestChain", "transaction", "utxo"]) {
      const payload = asMessage(update[field]);
      const height = payload === undefined ? undefined : numberField(payload, "height");
      if (height !== undefined) return height;
    }
    return undefined;
  }
}

function writeWithBackpressure(
  stream: ClientDuplexStream<Message, Message>,
  message: Message,
): Promise<void> {
  if (stream.write(message)) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const cleanup = (): void => {
      stream.off("drain", onDrain);
      stream.off("error", onError);
    };
    const onDrain = (): void => {
      cleanup();
      resolve();
    };
    const onError = (error: Error): void => {
      cleanup();
      reject(error);
    };
    stream.once("drain", onDrain);
    stream.once("error", onError);
  });
}

const RECOVERABLE = new Set([
  status.CANCELLED,
  status.UNKNOWN,
  status.DEADLINE_EXCEEDED,
  status.RESOURCE_EXHAUSTED,
  status.ABORTED,
  status.INTERNAL,
  status.UNAVAILABLE,
  status.DATA_LOSS,
]);

async function delay(milliseconds: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function subscribe(): Promise<void> {
  const eventTypes = (values.event ?? []).map((event) => {
    const normalized = EVENT_NAMES[event] ?? event;
    if (!normalized.startsWith("EVENT_TYPE_")) throw new Error(`unknown event type: ${event}`);
    return normalized;
  });
  const initial: Message = {
    eventTypes,
    includeMempoolSnapshot: values["mempool-snapshot"],
  };
  if (values["from-height"] !== undefined) {
    const height = Number.parseInt(values["from-height"], 10);
    if (!Number.isSafeInteger(height) || height < 0) {
      throw new Error("--from-height must be a non-negative integer");
    }
    initial.fromHeight = height;
  }
  const maxRetries = Number.parseInt(values["max-retries"], 10);
  if (!Number.isSafeInteger(maxRetries) || maxRetries < 0) {
    throw new Error("--max-retries must be a non-negative integer");
  }

  const state = new ResumeState();
  let active: ClientDuplexStream<Message, Message> | undefined;
  let stopping = false;
  process.once("SIGINT", () => {
    stopping = true;
    active?.cancel();
  });

  let reconnecting = false;
  let attempt = 0;
  while (!stopping) {
    const stream = client.subscribe(metadata);
    active = stream;
    const request = reconnecting ? state.reconnectRequest(initial) : initial;
    try {
      await writeWithBackpressure(stream, request);
      for await (const update of stream as AsyncIterable<Message>) {
        attempt = 0;
        const ping = asMessage(update.ping);
        const pingId = ping === undefined ? undefined : numberField(ping, "id");
        if (pingId !== undefined) {
          await writeWithBackpressure(stream, { ping: { id: pingId } });
          continue;
        }
        if (state.observe(update)) print(update);
      }
      if (stopping) return;
      throw Object.assign(new Error("subscription ended"), { code: status.UNAVAILABLE });
    } catch (error: unknown) {
      if (stopping) return;
      const serviceError = error as ServiceError;
      if (!RECOVERABLE.has(serviceError.code) || attempt >= maxRetries) throw error;
      const wait = Math.min(10_000, 100 * 2 ** attempt);
      attempt += 1;
      reconnecting = true;
      console.error(`subscription disconnected; retrying in ${wait}ms`);
      await delay(wait);
    } finally {
      stream.cancel();
      if (active === stream) active = undefined;
    }
  }
}

async function main(): Promise<void> {
  switch (command) {
    case "get-version":
      await unary(client.getVersion.bind(client));
      break;
    case "replay-info":
      await unary(client.subscribeReplayInfo.bind(client));
      break;
    case "subscribe":
      await subscribe();
      break;
    default:
      throw new Error(
        `unknown command ${JSON.stringify(command)}; use get-version, replay-info, or subscribe`,
      );
  }
}

main()
  .catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(() => client.close());
