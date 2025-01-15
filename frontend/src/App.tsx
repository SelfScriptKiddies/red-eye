// import { StrictMode } from 'react';
// import { createRoot } from 'react-dom/client';
import './index.css';
import { RedEye } from './components/RedEye';
import { Transit, PagesProvider, RenderPage } from "./PagesProvider";

export function App() {
  return (
    <PagesProvider>
      {window.location.pathname !== '/login' ? (
        <div className='header'>
        <RedEye></RedEye>
        <nav className='navMenu'>
          <Transit to={"/"}>Главная</Transit>
          <Transit to={"/packets"}>Пакеты</Transit>
        </nav>
      </div>
      ) : ''}      
      <div className='section'>
        <main className='content'>
          <RenderPage />
        </main>
      </div>

    </PagesProvider>
  )
}