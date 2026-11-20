// Dedicated AI page: MCP access, JEV stream analysis and SOC auto-triage,
// moved out of Settings. Three visually separated sections, terse text.

import './AiPage.css';
import { JevBlock } from './JevBlock';
import { McpTokensBlock } from './McpTokensBlock';
import { SocBlock } from './SocBlock';

export function AiPage() {
    return (
        <div className="settings-column">
            <div className="column-header">
                <h2>AI</h2>
            </div>
            <div className="ai-section">
                <h3>MCP</h3>
                <McpTokensBlock />
            </div>
            <div className="ai-section">
                <h3>JEV</h3>
                <JevBlock />
            </div>
            <div className="ai-section">
                <h3>Авто-триаж</h3>
                <SocBlock />
            </div>
        </div>
    );
}
