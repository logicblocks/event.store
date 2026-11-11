const SIN_60 = Math.sqrt(3) / 2;

export type Point = readonly [x: number, y: number];

export function hexVertices(cx: number, cy: number, radius: number): Point[] {
	const dx = radius * SIN_60;
	const dy = radius / 2;
	return [
		[cx, cy - radius],
		[cx + dx, cy - dy],
		[cx + dx, cy + dy],
		[cx, cy + radius],
		[cx - dx, cy + dy],
		[cx - dx, cy - dy],
	];
}

export function toPoints(vertices: readonly Point[]): string {
	return vertices.map(([x, y]) => `${round(x)},${round(y)}`).join(' ');
}

export function hexPoints(cx: number, cy: number, radius: number): string {
	return toPoints(hexVertices(cx, cy, radius));
}

function round(value: number): number {
	return Math.round(value * 100) / 100;
}
