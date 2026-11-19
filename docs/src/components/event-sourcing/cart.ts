import { animate } from 'motion';

export type Sku = 'beans' | 'mug' | 'grinder' | 'filters';

export const products: Record<Sku, { name: string; price: number }> = {
	beans: { name: 'Coffee beans', price: 12 },
	mug: { name: 'Mug', price: 8 },
	grinder: { name: 'Grinder', price: 45 },
	filters: { name: 'Filters', price: 4 },
};

export const skus = Object.keys(products) as Sku[];

export type CartEvent =
	| { name: 'item-added'; payload: { sku: Sku } }
	| { name: 'item-removed'; payload: { sku: Sku } }
	| { name: 'checked-out'; payload: Record<string, never> };

export type CartState = {
	items: Partial<Record<Sku, number>>;
	status: 'open' | 'checked-out';
};

export const initialState = (): CartState => ({ items: {}, status: 'open' });

export function apply(state: CartState, event: CartEvent): CartState {
	switch (event.name) {
		case 'item-added': {
			const { sku } = event.payload;
			return { ...state, items: { ...state.items, [sku]: (state.items[sku] ?? 0) + 1 } };
		}
		case 'item-removed': {
			const { sku } = event.payload;
			const items = { ...state.items };
			const quantity = (items[sku] ?? 0) - 1;
			if (quantity > 0) items[sku] = quantity;
			else delete items[sku];
			return { ...state, items };
		}
		case 'checked-out':
			return { ...state, status: 'checked-out' };
	}
}

export const fold = (events: readonly CartEvent[]) => events.reduce(apply, initialState());

export const total = (state: CartState) =>
	Object.entries(state.items).reduce(
		(sum, [sku, quantity]) => sum + products[sku as Sku].price * (quantity ?? 0),
		0,
	);

export const itemCount = (state: CartState) =>
	Object.values(state.items).reduce((sum, quantity) => sum + (quantity ?? 0), 0);

export const added = (sku: Sku): CartEvent => ({ name: 'item-added', payload: { sku } });
export const removed = (sku: Sku): CartEvent => ({ name: 'item-removed', payload: { sku } });
export const checkedOut = (): CartEvent => ({ name: 'checked-out', payload: {} });

export const money = (amount: number) => `£${amount}`;

export const describe = (event: CartEvent) =>
	'sku' in event.payload ? products[event.payload.sku].name : '';

export const payloadText = (event: CartEvent) =>
	'sku' in event.payload ? `{ sku: "${event.payload.sku}" }` : '{ }';

export function eventCard(
	event: CartEvent,
	position: number,
	options: { stream?: string; payload?: boolean } = {},
) {
	const card = document.createElement('li');
	card.className = 'es-event';
	card.dataset.kind = event.name;
	card.innerHTML = `
		<span class="es-event__position">#${position}</span>
		<span class="es-event__body">
			<span class="es-event__name">${event.name}</span>
			${options.stream ? `<span class="es-event__stream">${options.stream}</span>` : ''}
			${
				options.payload === false
					? describe(event)
						? `<span class="es-event__detail">${describe(event)}</span>`
						: ''
					: `<code class="es-event__payload">${payloadText(event)}</code>`
			}
		</span>`;
	return card;
}

export function renderCart(state: CartState, options: { empty?: string } = {}) {
	const lines = Object.entries(state.items)
		.map(([sku, quantity]) => {
			const product = products[sku as Sku];
			return `<li><span>${quantity} × ${product.name}</span><span>${money(product.price * (quantity ?? 0))}</span></li>`;
		})
		.join('');
	return `
		<ul class="es-cart__lines">${lines || `<li class="es-cart__empty">${options.empty ?? 'Empty cart'}</li>`}</ul>
		<div class="es-cart__total"><span>Total</span><span>${money(total(state))}</span></div>
		<div class="es-cart__status" data-status="${state.status}">${state.status === 'open' ? 'Open' : 'Checked out'}</div>`;
}

export const enter = (element: Element) =>
	animate(
		element,
		{ opacity: [0, 1], transform: ['translateY(-0.75rem) scale(0.96)', 'translateY(0rem) scale(1)'] },
		{ type: 'spring', stiffness: 380, damping: 26 },
	);

export const pulse = (element: Element) =>
	animate(element, { transform: ['scale(1)', 'scale(1.04)', 'scale(1)'] }, { duration: 0.35 });

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export function whileVisible(element: Element, start: () => () => void) {
	let stop: (() => void) | undefined;
	new IntersectionObserver(
		([entry]) => {
			if (entry.isIntersecting && !stop) stop = start();
			else if (!entry.isIntersecting && stop) {
				stop();
				stop = undefined;
			}
		},
		{ threshold: 0.35 },
	).observe(element);
}

export function loop(body: (signal: { cancelled: boolean }) => Promise<void>) {
	return () => {
		const signal = { cancelled: false };
		(async () => {
			while (!signal.cancelled) await body(signal);
		})();
		return () => {
			signal.cancelled = true;
		};
	};
}
