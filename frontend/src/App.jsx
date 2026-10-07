import { useEffect, useMemo, useRef, useState } from 'react'
import { getHealth, getThread, getThreads, streamChat } from './api.js'
import { Lock, Logo, Plus, Send } from './Icons.jsx'
import Turn from './Turn.jsx'

const EXAMPLES = [
  { tag: 'Freshness', q: 'Is the curated data up to date?' },
  { tag: 'Airflow', q: 'When did daily_lake_maintenance last succeed?' },
  { tag: 'Data', q: 'How many earthquakes were recorded yesterday?' },
  { tag: 'Schema', q: 'Has the raw_seismic schema changed recently?' },
]

// thread ids look like chat-YYYYMMDD-HHMMSS (UTC): group them by day for the sidebar
function threadDate(id) {
  const m = id.match(/^chat-(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})/)
  return m ? new Date(Date.UTC(+m[1], m[2] - 1, +m[3], +m[4], +m[5])) : null
}

function groupThreads(threads) {
  const startOfToday = new Date(); startOfToday.setHours(0, 0, 0, 0)
  const startOfYesterday = new Date(startOfToday); startOfYesterday.setDate(startOfToday.getDate() - 1)
  const groups = { Today: [], Yesterday: [], Earlier: [] }
  for (const t of threads) {
    const d = threadDate(t.thread_id)
    const key = !d ? 'Earlier' : d >= startOfToday ? 'Today' : d >= startOfYesterday ? 'Yesterday' : 'Earlier'
    groups[key].push({ ...t, time: d })
  }
  return Object.entries(groups).filter(([, list]) => list.length)
}

export default function App() {
  const [health, setHealth] = useState(null)
  const [threads, setThreads] = useState([])
  const [threadId, setThreadId] = useState(null)   // null = a new conversation
  const [turns, setTurns] = useState([])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)
  const bottomRef = useRef(null)
  const inputRef = useRef(null)

  const refreshThreads = () => getThreads().then(setThreads).catch(() => {})

  useEffect(() => {
    getHealth().then(setHealth).catch((e) => setError(`API not reachable: ${e.message}`))
    refreshThreads()
  }, [])

  // Keep the newest step in view while the agent works
  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' }) }, [turns])

  // Grow the composer with its content (up to a limit set in CSS)
  useEffect(() => {
    const el = inputRef.current
    if (el) { el.style.height = 'auto'; el.style.height = `${el.scrollHeight}px` }
  }, [input])

  // tool name -> MCP server, for coloring evidence chips
  const toolServers = useMemo(() => {
    const map = {}
    for (const [server, tools] of Object.entries(health?.servers ?? {})) for (const t of tools) map[t] = server
    return map
  }, [health])

  const groups = useMemo(() => groupThreads(threads), [threads])
  const title = turns[0]?.question ?? 'New conversation'

  async function openThread(id) {
    if (busy) return
    setError(null)
    try {
      const data = await getThread(id)
      setThreadId(id)
      setTurns(data.turns)
    } catch (e) {
      setError(e.message)
    }
  }

  function newChat() {
    if (busy) return
    setThreadId(null)
    setTurns([])
    setError(null)
    inputRef.current?.focus()
  }

  const updateLastTurn = (fn) => setTurns((ts) => ts.map((t, i) => (i === ts.length - 1 ? fn(t) : t)))

  async function send(text) {
    const message = text.trim()
    if (!message || busy) return
    setInput('')
    setError(null)
    setBusy(true)
    setTurns((ts) => [...ts, { question: message, events: [], pending: true, startedAt: Date.now() }])
    try {
      await streamChat({
        message,
        threadId,
        onEvent: (ev) => {
          if (ev.type === 'thread') setThreadId(ev.thread_id)
          else if (ev.type !== 'done') updateLastTurn((t) => ({ ...t, events: [...t.events, ev] }))
        },
      })
    } catch (e) {
      updateLastTurn((t) => ({ ...t, events: [...t.events, { type: 'error', message: e.message }] }))
    } finally {
      updateLastTurn((t) => ({ ...t, pending: false, finishedAt: Date.now() }))
      setBusy(false)
      refreshThreads()
    }
  }

  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="brand">
          <Logo size={34} />
          <div>
            <div className="brand-name">Pipeline Copilot</div>
            <div className="brand-sub">On-call for data pipelines</div>
          </div>
        </div>

        <button className="new" onClick={newChat} disabled={busy}><Plus size={16} /> New conversation</button>

        <nav>
          {groups.map(([label, list]) => (
            <div key={label} className="group">
              <div className="group-label">{label}</div>
              {list.map((t) => (
                <button
                  key={t.thread_id}
                  className={`thread ${t.thread_id === threadId ? 'active' : ''}`}
                  onClick={() => openThread(t.thread_id)}
                  disabled={busy}
                  title={t.thread_id}
                >
                  <span className="thread-title">{t.title}</span>
                  {t.time && (
                    <span className="thread-time">
                      {t.time.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                    </span>
                  )}
                </button>
              ))}
            </div>
          ))}
        </nav>

        <div className="status-card">
          <div className="status-row">
            <span className={`status-dot ${health ? 'on' : 'off'}`} />
            {health ? 'Connected' : 'API offline'}
            {health && <span className="model">{health.model}</span>}
          </div>
          {health && (
            <div className="servers">
              {Object.entries(health.servers ?? {}).map(([server, tools]) => (
                <span key={server} className={`server s-${server}`}>{server} · {tools.length}</span>
              ))}
            </div>
          )}
          <div className="readonly"><Lock size={12} /> Read-only: it investigates, you decide</div>
        </div>
      </aside>

      <main>
        <header className="topbar">
          <h2 title={title}>{title}</h2>
          {threadId && <span className="thread-id">{threadId}</span>}
        </header>

        <div className="conversation">
          {turns.length === 0 && (
            <div className="empty">
              <Logo size={56} />
              <h1>What looks wrong in your pipeline?</h1>
              <p>
                Ask about DAG runs, task logs, tables or data quality. Pipeline Copilot investigates with
                read-only tools, checks the runbooks, and verifies every piece of evidence it cites.
              </p>
              <div className="examples">
                {EXAMPLES.map(({ tag, q }) => (
                  <button key={q} onClick={() => send(q)} disabled={busy || !health}>
                    <span className="ex-tag">{tag}</span>
                    <span>{q}</span>
                  </button>
                ))}
              </div>
            </div>
          )}
          {turns.map((t, i) => <Turn key={i} turn={t} toolServers={toolServers} />)}
          {error && <p className="error banner">{error}</p>}
          <div ref={bottomRef} />
        </div>

        <form className="composer" onSubmit={(e) => { e.preventDefault(); send(input) }}>
          <div className="composer-box">
            <textarea
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                // Enter sends, Shift+Enter adds a new line
                if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(input) }
              }}
              placeholder={busy ? 'Investigating…' : 'Ask about your pipeline…'}
              rows={1}
              maxLength={4000}
              disabled={busy}
            />
            <button type="submit" disabled={busy || !input.trim()} aria-label="Send"><Send size={18} /></button>
          </div>
          <p className="hint">Enter to send · Shift+Enter for a new line · answers can take a minute on real incidents</p>
        </form>
      </main>
    </div>
  )
}
