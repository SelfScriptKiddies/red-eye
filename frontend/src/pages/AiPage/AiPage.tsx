import './AiPage.css';
import { JevColumn } from './JevColumn';
import { McpColumn } from './McpColumn';
import { SocColumn } from './SocColumn';

export function AiPage() {
    return (
        <div className="AiPage">
            <div className="ai-columns">
                <McpColumn />
                <JevColumn />
                <SocColumn />
            </div>
        </div>
    );
}
