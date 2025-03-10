import { ReactElement } from "react";
import { RedEye } from "../../components/RedEye";
import { LoginForm } from "../../components/LoginForm";
import './LoginPage.css';

export function LoginPage() {
    return (
        <div className="loginPage">
            <RedEye></RedEye>
            <h2>Узреть <span className="RedEyeSpan">RedEye</span></h2>
            <LoginForm/>
        </div>
    );
}