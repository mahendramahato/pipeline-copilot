// A small pixel-art bot that walks back and forth above the composer. The sprite is drawn
// from the grids below as SVG pixels (one character = one pixel), with two leg frames that
// CSS alternates while it walks (.walker in styles.css). The cursor on its screen blinks.
const HEAD = [
  '......OOOO......',
  '......OCCO......',
  '.......OO.......',
  '..OOOOOOOOOOOO..',
  '.OLLLBBBBBBBBBO.',
  'OLLBBBBBBBBBBBDO',
  'OLBOOOOOOOOOOBDO',
  'OBBOSSSSSSSSOBDO',
  'OBBOSGSSSSSSOBDO',
  'OBBOSSGSSSSSOBDO',
  'OBBOSGSSKKKSOBDO',
  'OBBOSSSSSSSSOBDO',
  'OBBOOOOOOOOOOBDO',
  '.OBBBBBBBBBBBDO.',
  '..OOOOOOOOOOOO..',
  '....OBBBBBBO....',
  '...OBBOGGOBDO...',
  '..OBOBBBBBBODBO.',
  '..OOOBBBBBBDOOO.',
  '....OBBBBBBO....',
  '....OOOOOOOO....',
]
const LEGS_TOGETHER = [
  '....OBO..OBO....',
  '....ODO..ODO....',
  '...OOOO..OOOO...',
]
const LEGS_APART = [
  '...OBO....OBO...',
  '..OBO......ODO..',
  '..OOOO....OOOO..',
]

const COLORS = { O: '#1e1b4b', B: '#6366f1', L: '#a5b4fc', D: '#4338ca', S: '#111433', G: '#67e8f9', C: '#67e8f9', K: '#67e8f9' }

// Runs of the same color in a row become one rect: ~100 rects instead of ~300
function pixels(rows, top = 0) {
  const rects = []
  rows.forEach((row, y) => {
    let x = 0
    while (x < row.length) {
      const c = row[x]
      let end = x
      while (row[end + 1] === c) end++
      if (COLORS[c]) {
        rects.push(<rect key={`${y}-${x}`} x={x} y={y + top} width={end - x + 1} height={1} fill={COLORS[c]} className={c === 'K' ? 'pb-cursor' : undefined} />)
      }
      x = end + 1
    }
  })
  return rects
}

const HEAD_RECTS = pixels(HEAD)
const LEGS_A = pixels(LEGS_TOGETHER, HEAD.length)
const LEGS_B = pixels(LEGS_APART, HEAD.length)
const ROWS = HEAD.length + LEGS_TOGETHER.length

export default function PixelBot({ busy = false }) {
  return (
    <div className={`walker ${busy ? 'busy' : ''}`} aria-hidden="true">
      <div className="pixelbot">
        <svg className="pb-sprite" viewBox={`0 0 16 ${ROWS}`} shapeRendering="crispEdges">
          {HEAD_RECTS}
          <g className="pb-legs-a">{LEGS_A}</g>
          <g className="pb-legs-b">{LEGS_B}</g>
        </svg>
      </div>
    </div>
  )
}
