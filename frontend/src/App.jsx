import { useEffect, useRef, useState } from 'react'
import { getHealth, getThread, getThreads, streamChat } from './api.js'
import Turn from './Turn.jsx'

const EXAMPLES = [
  'Is the curated data up to date?',
  'When did daily_lake_maintenance last succeed?',
  'How many earthquakes were recorded yesterday?',
  'Has the raw_seismic schema changed recently?',
]

export default function App() {
  const [health, setHealth] = useState(null)
  const [threads, setThreads] = useState([])
  const [threadId, setThreadId] = useState(null)   // null = a new conversation
  const [turns, setTurns] = useState([])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)
  const bottomRef = useRef(null)

  const refreshThreads = () => getThreads().then(setThreads).catch(() => {})

  useEffect(() => {
    getHealth().then(setHealth).catch((e) => setError(`API not reachable: ${e.message}`))
    refreshThreads()
  }, [])

  // Keep the newest step in view while the agent works
  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth' }) }, [turns])

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
  }

  const updateLastTurn = (fn) => setTurns((ts) => ts.map((t, i) => (i === ts.length - 1 ? fn(t) : t)))

  async function send(text) {
    const message = text.trim()
    if (!message || busy) return
    setInput('')
    setError(null)
    setBusy(true)
    setTurns((ts) => [...ts, { question: message, events: [], pending: true }])
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
      updateLastTurn((t) => ({ ...t, pending: false }))
      setBusy(false)
      refreshThreads()
    }
  }

  return (
    <div className="layout">
      <aside className="sidebar">
        <h1>Pipeline Copilot</h1>
        <button className="new" onClick={newChat} disabled={busy}>+ New conversation</button>
        <nav>
          {threads.map((t) => (
            <button
              key={t.thread_id}
              className={`thread ${t.thread_id === threadId ? 'active' : ''}`}
              onClick={() => openThread(t.thread_id)}
              disabled={busy}
              title={t.thread_id}
            >
              {t.title}
            </button>
          ))}
        </nav>
        {health && (
          <footer className="muted">
            {health.model} · {health.tools.length} MCP tools · read-only
          </footer>
        )}
      </aside>

      <main>
        <div className="conversation">
          {turns.length === 0 && (
            <div className="empty">
              <h2>What looks wrong?</h2>
              <p className="muted">
                Ask about DAG runs, task logs, tables or data quality. Pipeline Copilot investigates
                with read-only tools and verifies every piece of evidence it cites.
              </p>
              <div className="examples">
                {EXAMPLES.map((q) => (
                  <button key={q} onClick={() => send(q)} disabled={busy || !health}>{q}</button>
                ))}
              </div>
            </div>
          )}
          {turns.map((t, i) => <Turn key={i} turn={t} />)}
          {error && <p className="error">{error}</p>}
          <div ref={bottomRef} />
        </div>

        <form className="composer" onSubmit={(e) => { e.preventDefault(); send(input) }}>
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              // Enter sends, Shift+Enter adds a new line
              if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(input) }
            }}
            placeholder={busy ? 'Investigating…' : 'Ask about your pipeline'}
            rows={2}
            maxLength={4000}
            disabled={busy}
          />
          <button type="submit" disabled={busy || !input.trim()}>Send</button>
        </form>
      </main>
    </div>
  )
}
