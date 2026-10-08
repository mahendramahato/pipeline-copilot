import { useEffect, useRef, useState } from 'react'

// A small pixel-art bot that lives above the composer and behaves a bit like a person
// killing time: it strolls somewhere, stops, looks around, waves, hops, dozes off. It
// stops to watch while you type and paces with a "thinking" face while the agent works.
// The sprite is drawn from the grids below as SVG pixels (one character = one pixel).

const HEAD = [
  '......OOOO......',
  '......OCCO......',
  '.......OO.......',
  '.....OOOOOO.....',
  '...OOBBBBLLOO...',
  '..OBBBBBBBBLLO..',
  '.OBBBOOOOOOBBLO.',
  'OLBBOSSSSSSOBBDO',
  'OLBOSSSSSSSSOBDO',
  'OLBOSSSSSSSSOBDO',
  'OLBOSSSSSSSSOBDO',
  'OLBBOSSSSSSOBBDO',
  '.OBBBOOOOOOBBDO.',
  '..ODBBBBBBBBDO..',
  '...OOOOOOOOOO...',
]
// Rows 15-18: shoulders and arms, with two frames of a wave
const ARMS = [
  '....OBBBBBBO....',
  '...OBBOGGOBDO...',
  '..OBOBBBBBBODBO.',
  '..OOOBBBBBBDOOO.',
]
const WAVE_UP = [
  '....OBBBBBBO.OBO',
  '...OBBOGGOBDOBO.',
  '..OBOBBBBBBODO..',
  '..OOOBBBBBBDO...',
]
const WAVE_OUT = [
  '....OBBBBBBO....',
  '...OBBOGGOBDOOOO',
  '..OBOBBBBBBODBBO',
  '..OOOBBBBBBDOOOO',
]
const TORSO = [
  '....OBBBBBBO....',
  '....OOOOOOOO....',
]
// Rows 21-25: legs, standing
const LEGS = [
  '....OBO..OBO....',
  '....OBO..OBO....',
  '....OBO..OBO....',
  '....ODO..ODO....',
  '...OOOO..OOOO...',
]
// Faces drawn on the 8x5 screen (columns 4-11, rows 7-11); they can shift a pixel to look around
const FACES = {
  prompt: ['........', '.G......', '..G.....', '.G..KKK.', '........'],
  happy: ['........', '.G....G.', 'G.G..G.G', '........', '........'],
  thinking: ['........', '........', '........', '.G.G.G..', '........'],
  sleepy: ['........', '........', 'GGG..GGG', '........', '........'],
}

// Side view for walking (facing right; flipped to walk left). A walk cycle is four frames:
// stride, legs passing, the other stride, passing again. The far leg is darker, the near arm
// swings against the near leg, and the body dips on a stride and rises as the legs pass.
const SIDE_HEAD = [
  '........OOOO....',
  '........OCCO....',
  '.........OO.....',
  '.....OOOOOOO....',
  '...OODBBBBBLOO..',
  '..ODBBBBBBBBBLO.',
  '.ODBBBBBBBOOOOO.',
  'ODBOOOBBBOSSSSOO',
  'ODOLLLOBBOSSGSSO',
  'ODOLGLOBBOSSSGSO',
  'ODOLLLOBBOSSGSSO',
  'ODBOOOBBBOSSSSOO',
  '.ODBBBBBBBOOOOO.',
  '..ODDBBBBBBBBO..',
  '....OOOOOOOOO...',
]
const SIDE_TORSO = [
  '.....OBBBBO.....',
  '....OBBBBBBO....',
  '....OBBBBBBGO...',
  '....OBBBBBBO....',
  '.....ODDDDO.....',
]
const SIDE_ARM = {
  down: ['......OLO.......', '......OLO.......', '......OLO.......', '......OLO.......'],
  fwd: ['......OLO.......', '.......OLO......', '........OLO.....', '.........OLO....'],
  back: ['......OLO.......', '.....OLO........', '....OLO.........', '...OLO..........'],
}
const SIDE_LEG = {
  straight: ['......OBO.......', '......OBO.......', '......OBO.......', '......OBO.......', '......OBO.......', '......OBBO......'],
  fwd: ['......OBO.......', '.......OBO......', '........OBO.....', '.........OBO....', '.........OBBO...'],
  back: ['......OBO.......', '.....OBO........', '....OBO.........', '...OBO..........', '..OBBO..........'],
}
const ROWS = 26

// Lay transparent ('.') layers over each other into one grid
function sideFrame(near, far, arm) {
  const g = Array.from({ length: ROWS }, () => Array(16).fill('.'))
  const put = (top, layer) => layer.forEach((row, y) => [...row].forEach((c, x) => { if (c !== '.') g[top + y][x] = c }))
  const top = near === 'straight' ? 0 : 1
  put(ROWS - SIDE_LEG[far].length, SIDE_LEG[far].map((r) => r.replaceAll('B', 'D')))
  put(ROWS - SIDE_LEG[near].length, SIDE_LEG[near])
  put(top, SIDE_HEAD)
  put(top + 15, SIDE_TORSO)
  put(top + 16, SIDE_ARM[arm])
  return g.map((r) => r.join(''))
}
const WALK_CYCLE = [
  sideFrame('fwd', 'back', 'back'),
  sideFrame('straight', 'straight', 'down'),
  sideFrame('back', 'fwd', 'fwd'),
  sideFrame('straight', 'straight', 'down'),
]

const COLORS = { O: '#1e1b4b', B: '#6366f1', L: '#a5b4fc', D: '#4338ca', S: '#111433', G: '#67e8f9', C: '#67e8f9', K: '#67e8f9' }

// Runs of the same color in a row become one rect
function pixels(rows, top = 0, left = 0) {
  const rects = []
  rows.forEach((row, y) => {
    let x = 0
    while (x < row.length) {
      const c = row[x]
      let end = x
      while (row[end + 1] === c) end++
      if (COLORS[c]) {
        rects.push(
          <rect key={`${y}-${x}`} x={x + left} y={y + top} width={end - x + 1} height={1} fill={COLORS[c]}
            className={c === 'K' ? 'pb-cursor' : undefined} />,
        )
      }
      x = end + 1
    }
  })
  return rects
}

const SPRITE = {
  head: pixels(HEAD),
  arms: pixels(ARMS, 15),
  waveUp: pixels(WAVE_UP, 15),
  waveOut: pixels(WAVE_OUT, 15),
  torso: pixels(TORSO, 19),
  legs: pixels(LEGS, 21),
  walk: WALK_CYCLE.map((rows) => pixels(rows)),
  faces: Object.fromEntries(Object.entries(FACES).map(([k, rows]) => [k, pixels(rows, 7, 4)])),
}

const BOT_W = 64                                      // px, matches .pixelbot in styles.css
const SPEED = { stroll: 44, walk: 70, hurry: 140 }    // px per second
const CYCLE_PX = 54                                   // ground covered by one walk cycle (two steps)
const rand = (a, b) => a + Math.random() * (b - a)
const pick = (weighted) => {
  let r = Math.random() * weighted.reduce((s, [w]) => s + w, 0)
  for (const [w, v] of weighted) if ((r -= w) <= 0) return v
  return weighted[0][1]
}

const REST = { act: 'stand', face: 'prompt', look: 0, lookDown: 0, dur: 0, cycle: null }

export default function PixelBot({ busy = false, typing = false }) {
  const laneRef = useRef(null)
  // The current walk (or standing spot): position is worked out from it, not read from the page
  const path = useRef({ from: 0, to: 0, start: 0, ms: 0 })
  const [s, setS] = useState({ ...REST, x: 0, dir: 1 })

  useEffect(() => {
    if (window.matchMedia?.('(prefers-reduced-motion: reduce)').matches) return
    let timer
    let queue = []
    let last = 'stand'
    let prev = { act: 'stand', dir: 1 }

    const maxX = () => Math.max(0, (laneRef.current?.clientWidth ?? 0) - BOT_W)
    // Where the bot is right now, even halfway through a walk
    const here = () => {
      const { from, to, start, ms } = path.current
      return ms ? from + (to - from) * Math.min(1, (performance.now() - start) / ms) : to
    }

    // Each beat: apply a pose for some time, then decide again
    const beat = (pose, ms) => {
      const from = here()
      const to = pose.x ?? from
      path.current = { from, to, start: performance.now(), ms: (pose.dur ?? 0) * 1000 }
      prev = { act: pose.act ?? 'stand', dir: pose.dir ?? prev.dir }
      setS((p) => ({ ...p, ...REST, ...pose, x: to }))
      timer = setTimeout(next, ms)
    }
    const walkTo = (x, speed, face = 'prompt') => {
      const from = here()
      x = Math.min(Math.max(x, 0), maxX())
      const dur = Math.abs(x - from) / speed
      const dir = x >= from ? 1 : -1
      const walk = [{ act: 'walk', x, dir, dur, face, cycle: CYCLE_PX / speed }, dur * 1000 + 50]
      // Turning around: face the front for a moment instead of flipping mid-stride
      if (prev.act === 'walk' && prev.dir !== dir) { queue.unshift(walk); return beat({ look: dir, dir }, 260) }
      beat(...walk)
    }
    // A stroll to somewhere at least a little way off
    const stroll = (speed) => {
      const from = here(), w = maxX()
      let x = rand(0, w)
      if (Math.abs(x - from) < w * 0.15) x = from < w / 2 ? rand(w * 0.5, w) : rand(0, w * 0.5)
      walkTo(x, speed)
    }

    function next() {
      if (queue.length) { const [pose, ms] = queue.shift(); return beat(pose, ms) }

      if (busy) {
        // Pacing while the agent investigates: to one side, pause to think, back again
        const w = maxX(), from = here()
        if (last === 'walk') { last = 'stand'; return beat({ face: 'thinking', look: 0 }, rand(500, 1300)) }
        last = 'walk'
        return walkTo(from < w / 2 ? rand(w * 0.7, w) : rand(0, w * 0.3), SPEED.hurry, 'thinking')
      }
      if (typing) {
        // Stands and watches you type, with the odd glance away
        return beat(Math.random() < 0.8 ? { lookDown: 1 } : { look: pick([[1, -1], [1, 1]]) }, rand(1200, 2600))
      }

      // Idle life: mostly strolling and standing, with the occasional flourish
      const act = pick([
        [last === 'walk' ? 15 : 40, 'walk'],
        [last === 'walk' ? 30 : 12, 'stand'],
        [last === 'walk' ? 22 : 8, 'look'],
        [8, 'wave'],
        [6, 'hop'],
        [4, 'doze'],
      ])
      last = act
      switch (act) {
        case 'walk': return stroll(Math.random() < 0.6 ? SPEED.stroll : SPEED.walk)
        case 'stand': return beat({}, rand(1200, 3500))
        case 'look': {
          const first = pick([[1, -1], [1, 1]])
          queue = [[{ look: -first }, rand(700, 1100)], [{}, 400]]
          return beat({ look: first }, rand(700, 1100))
        }
        case 'wave': return beat({ act: 'wave', face: 'happy' }, 1800)
        case 'hop':
          queue = [[{ face: 'happy' }, 700]]
          return beat({ act: 'hop' }, 650)
        case 'doze':
          queue = [[{}, 500]]
          return beat({ act: 'doze', face: 'sleepy' }, rand(3000, 5000))
      }
    }

    // Busy or typing changed: stop where it stands (mid-walk too), then carry on
    const x = here()
    path.current = { from: x, to: x, start: 0, ms: 0 }
    setS((p) => ({ ...p, ...REST, x }))
    timer = setTimeout(next, typing || busy ? 80 : 600)
    return () => clearTimeout(timer)
  }, [busy, typing])

  return (
    <div className="walker" ref={laneRef} aria-hidden="true">
      <div
        className={`pixelbot act-${s.act}`}
        style={{
          transform: `translateX(${s.x}px)`, transition: `transform ${s.dur}s linear`,
          '--dir': s.dir, '--cycle': `${s.cycle ?? 1}s`,
        }}
      >
        <svg className="pb-sprite" viewBox={`0 0 16 ${ROWS}`} shapeRendering="crispEdges">
          {s.act === 'walk' ? (
            SPRITE.walk.map((frame, i) => <g key={i} className={`pb-frame f${i}`}>{frame}</g>)
          ) : (
            <>
              {SPRITE.head}
              <g transform={`translate(${s.look} ${s.lookDown})`}>{SPRITE.faces[s.face]}</g>
              {s.act === 'wave'
                ? <><g className="pb-wave-a">{SPRITE.waveUp}</g><g className="pb-wave-b">{SPRITE.waveOut}</g></>
                : SPRITE.arms}
              {SPRITE.torso}
              {SPRITE.legs}
              {s.act === 'doze' && <text className="pb-z" x="13" y="3">z</text>}
            </>
          )}
        </svg>
      </div>
    </div>
  )
}
