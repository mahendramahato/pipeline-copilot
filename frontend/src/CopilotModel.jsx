import { Component, useEffect, useRef } from 'react'
import { Canvas } from '@react-three/fiber'
import { ContactShadows, OrbitControls, useAnimations, useGLTF } from '@react-three/drei'
import * as THREE from 'three'

// The robot modelled in Blender (docs/copilot-3d.png), exported as GLB with a
// 4-second idle loop baked in: it floats, sways a little and blinks.
const MODEL_URL = '/models/copilot.glb'

function Robot() {
  const group = useRef()
  const { scene, animations } = useGLTF(MODEL_URL)
  const { actions } = useAnimations(animations, group)

  useEffect(() => {
    const idle = Object.values(actions)[0]
    idle?.reset().setLoop(THREE.LoopRepeat, Infinity).play()
    return () => idle?.stop()
  }, [actions])

  return <group ref={group}><primitive object={scene} /></group>
}

// If WebGL isn't available, show the flat icon instead of an empty box
class GLBoundary extends Component {
  state = { failed: false }
  static getDerivedStateFromError() { return { failed: true } }
  render() { return this.state.failed ? this.props.fallback : this.props.children }
}

// Drag to turn it around (OrbitControls, no zoom or pan).
// Lights roughly match the Blender studio: warm key, lavender fill, cool rim.
export default function CopilotModel({ size, fallback }) {
  return (
    <GLBoundary fallback={fallback}>
      <div className="robot-gl" style={{ width: size, height: size }} role="img"
        aria-label="Pipeline Copilot robot. Drag to turn it around.">
        <Canvas
          dpr={[1, 2]}
          camera={{ position: [2.7, 1.15, 5.6], fov: 29 }}
          gl={{ alpha: true, antialias: true, toneMapping: THREE.AgXToneMapping }}
        >
          <ambientLight intensity={0.55} color="#e9e3ff" />
          <directionalLight position={[-3.2, 3.6, 3.6]} intensity={2.4} color="#fff6ec" />
          <directionalLight position={[3.8, 0.6, 2.6]} intensity={0.9} color="#e3dcff" />
          <directionalLight position={[1.8, 2.8, -3.2]} intensity={2.0} color="#7dd3fc" />
          <Robot />
          <ContactShadows position={[0, -1.15, 0]} scale={4} blur={2.6} far={1.6} opacity={0.4} color="#2e1065" />
          <OrbitControls
            target={[0.06, -0.08, 0.25]}
            enablePan={false}
            enableZoom={false}
            enableDamping
            minPolarAngle={Math.PI * 0.32}
            maxPolarAngle={Math.PI * 0.6}
          />
        </Canvas>
      </div>
    </GLBoundary>
  )
}

useGLTF.preload(MODEL_URL)
