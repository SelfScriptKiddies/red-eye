import { createContext, useContext, useState, useCallback, ReactNode } from 'react';

interface RedEyeContextType {
    isActive: boolean;
    toggle: () => void;
}

const RedEyeContext = createContext<RedEyeContextType>({ isActive: false, toggle: () => {} });

export function RedEyeProvider({ children }: { children: ReactNode }) {
    const [isActive, setIsActive] = useState(false);
    const toggle = useCallback(() => setIsActive(prev => !prev), []);
    return (
        <RedEyeContext.Provider value={{ isActive, toggle }}>
            {children}
        </RedEyeContext.Provider>
    );
}

export function useRedEye() {
    return useContext(RedEyeContext);
}
