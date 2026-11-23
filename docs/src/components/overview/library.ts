export type PackageId =
	| 'types'
	| 'utils'
	| 'query'
	| 'sources'
	| 'persistence'
	| 'store'
	| 'projection'
	| 'processing'
	| 'testing';

export const base = '/event.store';

export type JourneyStep = {
	key: string;
	title: string;
	pkgs: PackageId[];
	text: string;
	code: string;
	inspector: string;
};
