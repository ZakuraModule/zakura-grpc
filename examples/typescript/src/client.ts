import {
  Client,
  ClientDuplexStream,
  credentials,
  loadPackageDefinition,
  Metadata,
  ServiceError,
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
  zakura: {
    geyser: {
      v1: {
        Geyser: GeyserConstructor;
      };
    };
  };
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
if (token !== undefined) {
  metadata.set("x-token", token);
}
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

function subscribe(): Promise<void> {
  const eventTypes = (values.event ?? []).map((event) => {
    const normalized = EVENT_NAMES[event] ?? event;
    if (!normalized.startsWith("EVENT_TYPE_")) {
      throw new Error(`unknown event type: ${event}`);
    }
    return normalized;
  });
  const request: Message = { eventTypes };
  if (values["from-height"] !== undefined) {
    const height = Number.parseInt(values["from-height"], 10);
    if (!Number.isSafeInteger(height) || height < 0) {
      throw new Error("--from-height must be a non-negative integer");
    }
    request.fromHeight = height;
  }

  return new Promise((resolve, reject) => {
    const stream = client.subscribe(metadata);
    let stopping = false;
    stream.on("data", (update: Message) => {
      const ping = update.ping as { id?: number } | undefined;
      if (ping?.id !== undefined) {
        stream.write({ ping: { id: ping.id } });
        return;
      }
      print(update);
    });
    stream.on("error", (error: ServiceError) => {
      if (stopping) {
        resolve();
      } else {
        reject(error);
      }
    });
    stream.on("end", resolve);
    process.once("SIGINT", () => {
      stopping = true;
      stream.cancel();
    });
    stream.write(request);
  });
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
