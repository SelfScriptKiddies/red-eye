import { ReactNode } from "react";
import { usePages } from "./PagesProvider";
import { LoginPage } from '../pages/LoginPage';
import { MainPage } from '../pages/MainPage';

export const RenderPage = (): ReactNode => {
    const { name } = usePages();
    const pages = {
        "/": <MainPage />,
        "/login": <LoginPage />,
    };
    if(!localStorage.getItem('token_RedEye') && name !== "/login")
        window.location.href = '/login';
    if (pages.hasOwnProperty(name))
        return pages[name];
    return;
}