import http from 'node:http';

const port = Number(process.env.FAKE_LLM_PORT || 4010);
const model = process.env.FAKE_LLM_MODEL || 'ci-local-model';

function json(res, status, body) {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(body));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://127.0.0.1:${port}`);
  if (req.method === 'GET' && url.pathname === '/health') {
    return json(res, 200, { ok: true, model });
  }
  if (req.method === 'GET' && url.pathname === '/v1/models') {
    return json(res, 200, { object: 'list', data: [{ id: model, object: 'model', owned_by: 'ci-fixture' }] });
  }
  if (req.method === 'POST' && url.pathname === '/v1/chat/completions') {
    let data = '';
    for await (const chunk of req) data += chunk;
    let body = {};
    try { body = data ? JSON.parse(data) : {}; } catch { return json(res, 400, { error: { message: 'invalid_json' } }); }
    if (body.model && body.model !== model) return json(res, 404, { error: { message: 'model_not_found' } });
    return json(res, 200, {
      id: `chatcmpl-ci-${Date.now()}`,
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model,
      choices: [{ index: 0, finish_reason: 'stop', message: { role: 'assistant', content: '{"recommendation":"hold","source":"ci-fixture"}' } }],
      usage: { prompt_tokens: 12, completion_tokens: 8, total_tokens: 20 },
      timings: { prompt_per_second: 100, predicted_per_second: 25 },
    });
  }
  return json(res, 404, { error: { message: 'not_found' } });
});

server.listen(port, '127.0.0.1', () => {
  process.stdout.write(`${JSON.stringify({ ok: true, fixture: 'openai-compatible-node', port, model })}\n`);
});

function shutdown() {
  server.close(() => process.exit(0));
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
