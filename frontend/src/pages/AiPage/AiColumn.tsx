import { ReactNode } from 'react';

export function AiColumn({ title, action, children }: { title: string; action?: ReactNode; children: ReactNode }) {
    return (
        <section className="ai-col" aria-label={title}>
            <div className="ai-col-header">
                <h2>{title}</h2>
                {action}
            </div>
            <div className="ai-col-body">{children}</div>
        </section>
    );
}
