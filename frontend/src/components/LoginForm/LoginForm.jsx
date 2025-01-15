import "./LoginForm.css"

const LoginBtn = () => {

    return (
        <button className="btn btn-danger" type="btn">Войти</button>
    );
}
export const LoginForm = () => {
    return (
        <form autoComplete="off">
            <div className="">
                <label htmlFor="login">Логин</label>
                <input className="text-input" name="login" id="login" type="text"></input>
            </div>
            <div className="">
                <label htmlFor="password">Пароль</label>
                <input className="text-input" name="password" id="password" type="password"></input>
            </div>
            <LoginBtn />
        </form>
    );
};