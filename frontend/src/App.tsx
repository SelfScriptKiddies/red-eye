// import { StrictMode } from 'react';
// import { createRoot } from 'react-dom/client';
import './index.css';
import { RedEye } from './components/RedEye';
import { AlertToast } from './components/AlertToast';
import { Transit, PagesProvider, RenderPage } from "./PagesProvider";

export function App() {
  return (
    <PagesProvider>  
      <div className='section'>
        {window.location.pathname !== '/login' ? (
          <div className='header'>
          <RedEye></RedEye>
          <nav className='navMenu'>
            <Transit to={"/agents"}>Агенты</Transit>
            <Transit to={"/settings"}>Настройки</Transit>
            <Transit to={"/packets"}>Пакеты</Transit>
          </nav>
        </div>
        ) : ''}    
        <main className='content'>
          <RenderPage />
        </main>
      </div>

      {window.location.pathname !== '/login' && <AlertToast />}
    </PagesProvider>
  )
}