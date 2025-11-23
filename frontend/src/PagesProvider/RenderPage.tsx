import { ReactNode } from "react";
import { usePages } from "./PagesProvider";
import { LoginPage } from '../pages/LoginPage';
import { MainPage } from '../pages/MainPage';
import { PacketsPage } from "../pages/PacketsPage";
import { SettingsPage } from "../pages/SettingsPage";

function getCookie(name) {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop().split(';').shift();
  }

export const RenderPage = (): ReactNode => {
    const { name } = usePages();
    const pages = {
        "/": <MainPage />,
        "/login": <LoginPage />,
        "/packets": <PacketsPage />,
        "/settings": <SettingsPage />,
    };
    if(!getCookie('id') && name !== "/login")
        window.location.href = '/login';
    if (pages.hasOwnProperty(name))
        return pages[name];
    return;
}