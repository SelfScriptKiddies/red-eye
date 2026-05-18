import "./RedEye.css"
import { useState, useEffect } from 'react';
import { useRedEye } from '../../context/RedEyeContext';

export const RedEye = () => {
    const { isActive, toggle } = useRedEye();
    const [state, setEyeState] = useState(0);

    const stateClasses = ["eye__apple__center", "eye__apple__right", "eye__apple__left", "eye__apple__center", "eye__apple__focus", "eye__apple__center"];

    useEffect(() => {
            const interval = setInterval(() => {
                if(window.location.pathname !== '/packets')
                {
                    setEyeState((state) => (state + 1) % stateClasses.length);
                }
                else if(!isActive) {
                    setEyeState((state) => (state + 1) % 4);
                }
            }, 1500);

            return () => clearInterval(interval);
    }, [isActive]);

    const handleClick = () => {
        if(window.location.pathname === '/packets')
        {
            toggle();
            setEyeState(isActive ? 0 : 4);
        }
    }
    const stateClass = isActive ? 'eye__apple__focus' : stateClasses[state];
    return (
        <div className="eye" id="RedEye" onClick={handleClick}>
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
