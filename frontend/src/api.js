// Thin client for the FastAPI backend (/api/*).

async function getJson(url) {
  const res = await fetch(url)
  if (!res.ok) throw new Error(await errorText(res))
  return res.json()
}

// FastAPI errors: {detail: "text"} for our own errors, {detail: [...]} for validation
async function errorText(res) {
  const body = await res.json().catch(() => ({}))
  return typeof body.detail === 'string' ? body.detail : `Request failed (${res.status})`
}

async function postJson(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body ?? {}),
  })
  if (!res.ok) throw new Error(await errorText(res))
  return res.json()
}

export const getHealth = () => getJson('/api/health')
export const getShowcaseList = () => getJson('/api/showcase')
export const getShowcase = (slug) => getJson(`/api/showcase/${encodeURIComponent(slug)}`)
export const login = (password) => postJson('/api/login', { password })
export const logout = () => postJson('/api/logout')
export const getMonitor = () => getJson('/api/monitor')
export const runMonitor = () => postJson('/api/monitor/run')
export const getThreads = () => getJson('/api/threads')
export const getThread = (id) => getJson(`/api/threads/${encodeURIComponent(id)}`)

// POST /api/chat answers with Server-Sent Events: "data: {json}\n\n" per step.
// EventSource only supports GET, so we read the response stream ourselves.
export async function streamChat({ message, threadId, onEvent }) {
  const res = await fetch('/api/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message, thread_id: threadId ?? null }),
  })
  if (!res.ok) throw new Error(await errorText(res))

  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  for (;;) {
    const { value, done } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    // A network chunk can hold several events, or half of one: only parse complete ones
    let end
    while ((end = buffer.indexOf('\n\n')) !== -1) {
      const block = buffer.slice(0, end)
      buffer = buffer.slice(end + 2)
      for (const line of block.split('\n')) {
        if (line.startsWith('data: ')) onEvent(JSON.parse(line.slice(6)))
      }
    }
  }
}
