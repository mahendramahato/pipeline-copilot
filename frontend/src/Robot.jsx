import arm from './assets/robot-arm.webp'
import body from './assets/robot-body.webp'

// The mascot: two layers of one picture (the body, and the raised arm with the wrench) so the
// arm can turn the wrench around its elbow, with sparks at its jaw. All motion is CSS (.robot in
// styles.css) and stops for people who ask for reduced motion.
//   mode="idle"     a slow tightening now and then (landing page, empty chat)
//   mode="working"  fast tightening with sparks (while an investigation runs)
//   mode="found"    one hop and a wrench flourish (the investigation found a problem)
//   mode="rest"     still (finished turns, so a long thread is not all motion)
export default function Robot({ size = 160, mode = 'idle' }) {
  return (
    <div className={`robot ${mode}`} style={{ width: size, height: size * (520 / 465), fontSize: size / 10 }} aria-hidden="true">
      <div className="robot-figure">
        <img className="robot-body" src={body} alt="" draggable="false" />
        <span className="robot-lid left" />
        <span className="robot-lid right" />
        <div className="robot-tool">
          <img className="robot-arm" src={arm} alt="" draggable="false" />
          <span className="robot-sparks"><i /><i /><i /><i /><i /></span>
        </div>
      </div>
    </div>
  )
}
