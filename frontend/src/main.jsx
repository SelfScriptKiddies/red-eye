import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import { RedEye } from './components/RedEye.jsx'


createRoot(document.getElementById('root')).render(
  <StrictMode>
    <RedEye/>
  </StrictMode>,
);