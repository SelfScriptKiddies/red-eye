import './index.css';
import { RedEye } from './components/RedEye';
import { AlertToast } from './components/AlertToast';
import { Transit, PagesProvider, RenderPage, usePages } from "./PagesProvider";

function NavIcon({ to, label, children }: { to: string; label: string; children: React.ReactNode }) {
  const { name } = usePages();
  const isActive = name === to || (to === '/' && name === '/');

  return (
    <Transit to={to}>
      <div className={`nav-card ${isActive ? 'nav-card--active' : ''}`}>
        <div className="nav-card-icon">{children}</div>
        <span className="nav-card-label">{label}</span>
      </div>
    </Transit>
  );
}

export function App() {
  return (
    <PagesProvider>
      <div className='section'>
        {window.location.pathname !== '/login' ? (
          <div className='header'>
          <RedEye></RedEye>
          <nav className='navMenu'>
            <NavIcon to="/" label="Dashboard">
              <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M3 3v18h18"/><path d="M7 16l4-8 4 4 5-6"/>
              </svg>
            </NavIcon>
            <NavIcon to="/agents" label="Agents">
              <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
              </svg>
            </NavIcon>
            <NavIcon to="/settings" label="Settings">
              <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/>
              </svg>
            </NavIcon>
            <NavIcon to="/packets" label="Packets">
              <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>
              </svg>
            </NavIcon>
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
