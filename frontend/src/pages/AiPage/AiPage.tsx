import './AiPage.css';
import { JevBlock } from './JevBlock';
import { McpTokensBlock } from './McpTokensBlock';
import { SocBlock } from './SocBlock';

export function AiPage() {
    return (
        <div className="AiPage">
            <div className="ai-columns">
                <div className="ai-col">
                    <div className="ai-col-header">
                        <h2>MCP</h2>
                    </div>
                    <div className="ai-col-body">
                        <McpTokensBlock />
                    </div>
                </div>
                <div className="ai-col">
                    <div className="ai-col-header">
                        <h2>JEV</h2>
                    </div>
                    <div className="ai-col-body">
                        <JevBlock />
                    </div>
                </div>
                <div className="ai-col">
                    <div className="ai-col-header">
                        <h2>Авто-триаж</h2>
                    </div>
                    <div className="ai-col-body">
                        <SocBlock />
                    </div>
                </div>
            </div>
        </div>
    );
}
