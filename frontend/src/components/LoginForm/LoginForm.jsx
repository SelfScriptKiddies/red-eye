import "./LoginForm.css"

const LoginBtn = () => {
    const handleClick = (event) => {
        document.querySelector(".errorMessage").innerText = "";
        event.preventDefault();
        const data = new URLSearchParams(new FormData(event.target.parentElement)).toString();
        console.log(data);
        fetch("/api/login",{
            method: 'POST',
            headers: {'Content-Type': "application/x-www-form-urlencoded"},
            body: data
        })
        .then((response) => {
            if (parseInt(response.status / 100) == 5)
            {
                document.querySelector(".errorMessage").innerText = "Что-то с сервером";
                return;
            }
            else if(response.status == 403)
            {
                document.querySelector(".errorMessage").innerText = "Запрос блокирован";
                return;
            }
            else
                return response.json();
        })
        .then((resp_json) => {
            if(!resp_json)
                return;
            if(resp_json.hasOwnProperty('status') && resp_json['status'] == "Logged in")
            {
                window.location.pathname = "/";
            }
            else if(resp_json.hasOwnProperty('error'))
            {
                document.querySelector(".errorMessage").innerText = resp_json['error'];
            }
        });
    }
    return (
        <button className="btn btn-danger" type="submit" onClick={handleClick}>Войти</button>
    );
}
export const LoginForm = () => {
    return (
        <form autoComplete="off">
            <div className="">
                <label htmlFor="password">Пароль</label>
                <input className="text-input" name="password" id="password" type="password"></input>
            </div>
            <div className="errorMessage"/>
            <LoginBtn />
        </form>
    );
};