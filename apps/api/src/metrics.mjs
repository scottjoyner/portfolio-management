const counters = {
  requests_total: 0,
  responses_total: {},
  errors_total: 0
};

export function recordResponse(status) {
  counters.requests_total += 1;
  const bucket = String(status || 0);
  counters.responses_total[bucket] = (counters.responses_total[bucket] || 0) + 1;
  if (Number(status) >= 500) counters.errors_total += 1;
}

export function metricsSnapshot(extra = {}) {
  return {
    ...counters,
    uptime_seconds: Math.round(process.uptime()),
    memory_rss_bytes: process.memoryUsage().rss,
    ...extra
  };
}

export function renderPrometheusMetrics(extra = {}) {
  const snap = metricsSnapshot(extra);
  const lines = [];
  lines.push('# HELP portfolio_requests_total Total HTTP requests observed by this process.');
  lines.push('# TYPE portfolio_requests_total counter');
  lines.push(`portfolio_requests_total ${snap.requests_total}`);
  lines.push('# HELP portfolio_responses_total HTTP responses by status code.');
  lines.push('# TYPE portfolio_responses_total counter');
  for (const [status, count] of Object.entries(snap.responses_total)) lines.push(`portfolio_responses_total{status="${status}"} ${count}`);
  lines.push('# HELP portfolio_errors_total HTTP 5xx responses.');
  lines.push('# TYPE portfolio_errors_total counter');
  lines.push(`portfolio_errors_total ${snap.errors_total}`);
  lines.push('# HELP portfolio_uptime_seconds Process uptime.');
  lines.push('# TYPE portfolio_uptime_seconds gauge');
  lines.push(`portfolio_uptime_seconds ${snap.uptime_seconds}`);
  lines.push('# HELP portfolio_memory_rss_bytes Resident set memory.');
  lines.push('# TYPE portfolio_memory_rss_bytes gauge');
  lines.push(`portfolio_memory_rss_bytes ${snap.memory_rss_bytes}`);
  return `${lines.join('\n')}\n`;
}

export function logRequest({ requestId, method, path, status, actor, role, durationMs }) {
  const event = { at: new Date().toISOString(), event: 'http_request', requestId, method, path, status, actor, role, durationMs };
  console.log(JSON.stringify(event));
}
