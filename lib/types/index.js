import { Service } from '@deepseek-ai/cordis';
import z from '@deepseek-ai/schemastery';
export class BetterDeepSeekBridgeService extends Service {
    config;
    static inject = ['webServer', 'agents'];
    static Config = z.object({
        enableCors: z.boolean().default(true),
    });
    sseClients = new Set();
    constructor(ctx, config) {
        super(ctx, 'betterDeepSeekBridge');
        this.config = config;
        // Unified Better-DeepSeek Router
        this.ctx.effect(() => {
            return this.ctx.webServer.register({
                kind: 'prefix',
                path: '/api/better-deepseek',
                handler: async (req, res) => {
                    if (this.handleCors(req, res))
                        return;
                    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? '127.0.0.1'}`);
                    const pathname = url.pathname;
                    // 1. Handshake Ping Endpoint
                    if (pathname === '/api/better-deepseek/ping') {
                        res.writeHead(200, { 'Content-Type': 'application/json' });
                        res.end(JSON.stringify({
                            active: true,
                            version: '1.6.0',
                            capabilities: ['filtered_sse', 'approvals', 'rag_inject'],
                        }));
                        return;
                    }
                    // 2. Live SSE Event Stream Endpoint
                    if (pathname === '/api/better-deepseek/events') {
                        res.writeHead(200, {
                            'Content-Type': 'text/event-stream',
                            'Cache-Control': 'no-cache',
                            'Connection': 'keep-alive',
                        });
                        res.write(': connected\n\n');
                        this.sseClients.add(res);
                        req.on('close', () => {
                            this.sseClients.delete(res);
                        });
                        return;
                    }
                    // 3. RPC Endpoints (session.create, session.prompt)
                    if (req.method === 'POST') {
                        const body = await this.readJsonBody(req);
                        const rpcId = body.rpcId ?? `bd-rpc-${Date.now()}`;
                        // session.create
                        if (pathname === '/api/better-deepseek/session.create') {
                            try {
                                const cwd = body.payload?.cwd ?? process.cwd();
                                const sessionId = `session-${Date.now()}`;
                                await this.ctx.agents.create({
                                    sessionId,
                                    meta: { cwd },
                                });
                                res.writeHead(200, { 'Content-Type': 'application/json' });
                                res.end(JSON.stringify({
                                    type: 'server-response',
                                    rpcId,
                                    result: {
                                        ok: true,
                                        value: { sessionId },
                                    },
                                }));
                            }
                            catch (err) {
                                res.writeHead(500, { 'Content-Type': 'application/json' });
                                res.end(JSON.stringify({
                                    type: 'server-response',
                                    rpcId,
                                    result: {
                                        ok: false,
                                        error: { message: err?.message ?? 'Failed to create session' },
                                    },
                                }));
                            }
                            return;
                        }
                        // session.prompt
                        if (pathname === '/api/better-deepseek/session.prompt') {
                            const sessionId = (body.payload?.sessionId ?? '');
                            const text = body.payload?.text ?? '';
                            const agent = this.ctx.agents.get(sessionId);
                            if (agent) {
                                const userMsg = {
                                    id: crypto.randomUUID(),
                                    role: 'user',
                                    source: { kind: 'user' },
                                    content: [{ type: 'text', text }],
                                };
                                agent.followup(userMsg);
                                res.writeHead(200, { 'Content-Type': 'application/json' });
                                res.end(JSON.stringify({
                                    type: 'server-response',
                                    rpcId,
                                    result: { ok: true, value: {} },
                                }));
                            }
                            else {
                                res.writeHead(404, { 'Content-Type': 'application/json' });
                                res.end(JSON.stringify({
                                    type: 'server-response',
                                    rpcId,
                                    result: {
                                        ok: false,
                                        error: { message: `Session "${sessionId}" not found` },
                                    },
                                }));
                            }
                            return;
                        }
                    }
                    res.writeHead(404, { 'Content-Type': 'text/plain' });
                    res.end('not found');
                },
            });
        }, 'better-deepseek: gateway router');
        // 4. Event Listener Registrations
        this.setupEventListeners();
    }
    handleCors(req, res) {
        if (this.config.enableCors !== false) {
            res.setHeader('Access-Control-Allow-Origin', '*');
            res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
            res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
        }
        if (req.method === 'OPTIONS') {
            res.writeHead(204);
            res.end();
            return true;
        }
        return false;
    }
    async readJsonBody(req) {
        const chunks = [];
        for await (const chunk of req) {
            chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : chunk);
        }
        const raw = Buffer.concat(chunks).toString('utf8');
        try {
            return JSON.parse(raw);
        }
        catch {
            return {};
        }
    }
    setupEventListeners() {
        // Durable session event stream (assistant chunks, step start/end, tool calls, tool results)
        this.ctx.effect(() => {
            return this.ctx.on('session/event', (session, event) => {
                if (event.type === 'assistant/chunk') {
                    const chunkData = event.data;
                    this.broadcast('assistant/chunk', {
                        sessionId: session.id,
                        delta: chunkData.text ?? chunkData.delta ?? '',
                    });
                }
            });
        }, 'better-deepseek: session event listener');
        // Tool execution waterfalls (must call next() to delegate!)
        this.ctx.effect(() => {
            return this.ctx.on('tools/pre-execute', (exec, next) => {
                const sessionId = exec.agent?.id;
                if (sessionId !== undefined) {
                    this.broadcast('tool/call', {
                        sessionId,
                        tool: exec.name,
                        args: exec.arguments,
                    });
                }
                return next();
            });
        }, 'better-deepseek: tool pre-execute listener');
        this.ctx.effect(() => {
            return this.ctx.on('tools/post-execute', (exec, result, next) => {
                const sessionId = exec.agent?.id;
                if (sessionId !== undefined) {
                    this.broadcast('tool/result', {
                        sessionId,
                        tool: exec.name,
                        output: result,
                    });
                }
                return next();
            });
        }, 'better-deepseek: tool post-execute listener');
        // Agent turn stopping (serial notification)
        this.ctx.effect(() => {
            return this.ctx.on('agent/turn-stopping', (payload) => {
                this.broadcast('turn/stopping', {
                    sessionId: payload.agent.id,
                    turn: payload.turn,
                });
            });
        }, 'better-deepseek: turn stopping listener');
    }
    broadcast(type, payload) {
        const data = `data: ${JSON.stringify({ type, timestamp: Date.now(), payload })}\n\n`;
        for (const client of this.sseClients) {
            client.write(data);
        }
    }
}
export default BetterDeepSeekBridgeService;
//# sourceMappingURL=index.js.map