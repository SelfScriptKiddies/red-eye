import { MouseEventHandler, ReactElement } from "react";
import {ILinkProps} from "./interfaces";
import { usePages } from "./PagesProvider";
// import "./styles.css";

export function Transit({ children, to }: ILinkProps): ReactElement {
    const { navigate } = usePages();

    const handleClick: MouseEventHandler<HTMLAnchorElement> = (event) => {
        event.preventDefault();
        navigate(to);
    };

    return (
        <a className="link" href={to} onClick={handleClick}>
            {children}
        </a>
    );
}