import { ReactNode } from "react";

export interface IPage {
	name: string;
}

export interface IPageProvider extends IPage {
	navigate: (name: string) => void;
}

export interface IPageProviderProps {
	children: ReactNode;
}

export interface ILinkProps {
	to: string;
	children: ReactNode;
}
