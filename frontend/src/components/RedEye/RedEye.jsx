import "./RedEye.css"
import { useState, useEffect } from 'react';

export const RedEye = () => {
    const [state, setEyeState] = useState(0);

    const stateClasses = ["eye__apple__center", "eye__apple__right", "eye__apple__left", "eye__apple__center", "eye__apple__focus", "eye__apple__center"];
    useEffect(() => {
        const interval = setInterval(() => {
            setEyeState((state) => (state + 1) % stateClasses.length);
        }, 1500);

        return () => clearInterval(interval);
    }, []);

    const stateClass = stateClasses[state];
    return (
        <div className="eye" id="RedEye">
            <div className={`eye__apple ${stateClass}`} >
                <div className={`eye__apple__apple`} />
            </div>
            <div className="bloodrop">
                <svg viewBox="0 0 30 42">
                    <path fill="transparent" stroke="#000" strokeWidth="1.5"
                        d="M15 3
           Q16.5 6.8 25 18
           A12.8 12.8 0 1 1 5 18
           Q13.5 6.8 15 3z" />
                </svg>
            </div>
        </div>
    );
};