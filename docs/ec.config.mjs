// @ts-check
import { defineEcConfig } from '@astrojs/starlight/expressive-code';
import { readFileSync } from 'node:fs';

// Expressive Code needs literal colours to check contrast, so they're read from the brand tokens.
const atomic = Object.fromEntries(
	[
		...readFileSync(new URL('./src/styles/tokens.css', import.meta.url), 'utf8').matchAll(
			/--atomic-([\w-]+):\s*(#[\da-f]{6})\b/gi,
		),
	].map(([, name, hex]) => [name, hex.toLowerCase()]),
);

/** @param {string} hex @param {number} alpha */
const alpha = (hex, alpha) =>
	hex +
	Math.round(alpha * 255)
		.toString(16)
		.padStart(2, '0');

// Night Owl's colours, by role, replaced with the brand accent for that role.
const syntax = {
	dark: {
		background: atomic['night-3'],
		roles: {
			keyword: [['#c792ea', '#d3423e', '#ec5f67', '#ff6363'], atomic['red-2']],
			string: [['#ecc48d', '#c789d6'], atomic['pastel-green']],
			func: [['#82aaff', '#82b1ff', '#b2ccd6', '#caece6', '#78ccf0'], atomic.malibu],
			type: [['#c5e478', '#ffcb8b'], atomic['purple-light']],
			constant: [['#f78c6c', '#ff5874', '#ffeb95', '#fad430'], atomic['cream-can']],
			operator: [['#7fdbca', '#7fdbcaff', '#80cbc4', '#8eace3', '#7986e7'], atomic['indigo-tint']],
			comment: [['#637777', '#697098', '#5f7e97'], atomic.gray],
			plain: [['#d6deeb', '#d9f5dd', '#d7dbe0', '#bec5d4', '#dddddd'], atomic.ash],
		},
	},
	light: {
		background: atomic.bone,
		roles: {
			keyword: [['#994cc3', '#d3423e', '#ec5f67'], atomic['red-deep']],
			string: [['#c96765', '#c789d6'], atomic['pastel-green-deep']],
			func: [['#4876d6', '#4876d6ff', '#0c969b', '#0c969bff', '#78ccf0'], atomic['malibu-deep']],
			type: [['#c792ea'], atomic['medium-purple-deep']],
			constant: [['#aa0982', '#bc5454'], atomic['cream-can-deep']],
			operator: [['#7986e7'], atomic.indigo],
			comment: [['#989fb1', '#939dbb', '#697098', '#5f7e97'], atomic.slate],
			plain: [['#403f53', '#111111'], atomic.ink],
		},
	},
};

// Night Owl light shares one colour across these roles, so they're pinned in both themes.
const pinnedScopes = {
	string: ['string', 'string.unquoted'],
	func: ['entity.name.function', 'support.function', 'meta.function-call.generic'],
	type: [
		'support.type',
		'entity.name.type.class',
		'entity.name.type.class.python',
		'entity.name.class',
		'entity.other.inherited-class',
	],
	constant: ['constant.language', 'constant.numeric'],
};

export default defineEcConfig({
	styleOverrides: {
		borderRadius: 'var(--atomic-radius-card)',
		codeFontSize: 'var(--sl-text-code-sm)',
		frames: {
			editorTabBarBorderBottomColor: 'var(--brand-code-border)',
			shadowColor: 'transparent',
		},
	},
	// Brand accents meet AA on the code surfaces as they are, so Expressive Code needn't shift them.
	minSyntaxHighlightingColorContrast: 4.5,
	customizeTheme: (theme) => {
		const { background, roles } = syntax[theme.type];
		const replacements = new Map(
			Object.values(roles).flatMap(([from, to]) => from.map((colour) => [colour, to])),
		);
		for (const { settings } of theme.settings) {
			const replacement = settings.foreground && replacements.get(settings.foreground.toLowerCase());
			if (replacement) settings.foreground = replacement;
		}
		for (const [role, scope] of Object.entries(pinnedScopes)) {
			theme.settings.push({ scope, settings: { foreground: roles[role][1] } });
		}
		theme.bg = background;
		theme.fg = roles.plain[1];
		theme.colors['editor.background'] = background;
		theme.colors['editor.foreground'] = roles.plain[1];

		const isDark = theme.type === 'dark';
		const [ins, del, mark] = isDark
			? [atomic['pastel-green'], atomic['red-2'], atomic['indigo-tint']]
			: [atomic['pastel-green-deep'], atomic['red-deep'], atomic.indigo];
		theme.styleOverrides.frames = {
			...theme.styleOverrides.frames,
			editorBackground: 'var(--brand-code-surface)',
			terminalBackground: 'var(--brand-code-surface)',
			editorActiveTabBackground: 'var(--brand-code-surface)',
			editorTabBarBackground: 'var(--brand-code-bar)',
			terminalTitlebarBackground: 'var(--brand-code-bar)',
			terminalTitlebarBorderBottomColor: 'var(--brand-code-border)',
			terminalTitlebarForeground: 'var(--sl-color-gray-3)',
			terminalTitlebarDotsForeground: 'var(--sl-color-gray-4)',
			terminalTitlebarDotsOpacity: '1',
			editorActiveTabForeground: 'var(--sl-color-white)',
			editorActiveTabIndicatorTopColor: 'var(--sl-color-accent)',
			editorActiveTabIndicatorBottomColor: 'transparent',
			editorActiveTabBorderColor: 'var(--brand-code-border)',
			inlineButtonForeground: 'var(--sl-color-gray-2)',
			inlineButtonBorder: 'var(--brand-code-border)',
		};
		theme.colors['titleBar.border'] = 'var(--brand-code-border)';
		theme.colors['editorGroupHeader.tabsBorder'] = 'var(--brand-code-border)';
		theme.styleOverrides.borderColor = 'var(--brand-code-border)';
		theme.styleOverrides.textMarkers = {
			...theme.styleOverrides.textMarkers,
			markBackground: alpha(mark, isDark ? 0.14 : 0.1),
			markBorderColor: alpha(mark, 0.6),
			insBackground: alpha(ins, isDark ? 0.12 : 0.1),
			insBorderColor: alpha(ins, 0.7),
			insDiffIndicatorColor: ins,
			delBackground: alpha(del, isDark ? 0.14 : 0.08),
			delBorderColor: alpha(del, 0.7),
			delDiffIndicatorColor: del,
		};
		return theme;
	},
});
