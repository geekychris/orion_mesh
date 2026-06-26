// Sketch — apply to dev_portal as additions inside:
//   mcp-server/src/index.ts
//
// Two new tools (`list_peer_runtimes`, `register_peer_runtime`) following the
// existing pattern of thin stdio wrappers over the REST API at $DEVPORTAL_URL.
//
// Insertion point: in the `server.setRequestHandler(ListToolsRequestSchema, ...)`
// handler, add the two tool definitions. In the `CallToolRequestSchema` handler,
// add the two `case` branches below.

import { z } from "zod";

// ---------- tool descriptors (insert into the existing tools array) ----------

export const PEER_RUNTIME_TOOLS = [
  {
    name: "list_peer_runtimes",
    description:
      "List peer runtime systems registered with Dev Portal (OrionMesh, KQueue, ...). " +
      "Optionally filter by kind.",
    inputSchema: {
      type: "object",
      properties: {
        kind: {
          type: "string",
          description: "Filter by kind ('orionmesh', 'kqueue', ...). Omit for all.",
        },
      },
    },
  },
  {
    name: "register_peer_runtime",
    description:
      "Register (or update) a peer runtime — e.g. an OrionMesh controller or a KQueue instance. " +
      "Idempotent on `name`.",
    inputSchema: {
      type: "object",
      required: ["name", "kind", "baseUrl"],
      properties: {
        name:       { type: "string", description: "Stable slug, e.g. 'orionmesh-belmont'." },
        kind:       { type: "string", description: "'orionmesh' | 'kqueue' | future kinds." },
        baseUrl:    { type: "string", description: "Controller / API base URL." },
        adminUiUrl: { type: "string", description: "Optional UI URL for deep-link/embed." },
        config:     { type: "object", description: "Peer-specific config (e.g. natsUrl)." },
      },
    },
  },
];

// ---------- handler implementations (add to the CallTool switch) ------------

const DEVPORTAL_URL = process.env.DEVPORTAL_URL ?? "http://127.0.0.1:8081";

export async function handleListPeerRuntimes(args: { kind?: string }) {
  const qs = args.kind ? `?kind=${encodeURIComponent(args.kind)}` : "";
  const r = await fetch(`${DEVPORTAL_URL}/api/peer-runtimes${qs}`);
  if (!r.ok) throw new Error(`list_peer_runtimes: ${r.status} ${await r.text()}`);
  return await r.json();
}

const RegisterArgs = z.object({
  name: z.string(),
  kind: z.string(),
  baseUrl: z.string(),
  adminUiUrl: z.string().optional(),
  config: z.record(z.any()).optional(),
});

export async function handleRegisterPeerRuntime(raw: unknown) {
  const args = RegisterArgs.parse(raw);
  const r = await fetch(`${DEVPORTAL_URL}/api/peer-runtimes`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(args),
  });
  if (!r.ok) throw new Error(`register_peer_runtime: ${r.status} ${await r.text()}`);
  return await r.json();
}
