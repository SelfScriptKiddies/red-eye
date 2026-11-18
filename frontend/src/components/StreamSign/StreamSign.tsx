import "./StreamSign.css"

interface StreamSignProps {
    containsFlag: boolean;
    incoming?: boolean;
}

export const StreamSign = (props: StreamSignProps) => {
    if (!props.containsFlag)
        return null;
    return (
        <svg
            className="stream-flag"
            viewBox="0 0 16 16"
            width="14"
            height="14"
            role="img"
            aria-label="Stream contains flag"
            focusable="false"
        >
            <rect x="3" y="1" width="1.8" height="14" rx="0.9" fill="currentColor" />
            <path d="M4.8 2.5 H13.2 L11.2 5.9 L13.2 9.3 H4.8 Z" fill="currentColor" />
        </svg>
    );
};
