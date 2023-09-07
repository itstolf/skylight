import { akas, mutuals, whois } from '@/lib/client';
import { error } from '@sveltejs/kit';

import type { PageLoad } from './$types';
export const load: PageLoad = async ({ params }) => {
	let actor1: { alsoKnownAs: string[]; did: string };
	let actor2: { alsoKnownAs: string[]; did: string };

	try {
		const [actor1R, actor2R] = await Promise.all([
			(async () => {
				try {
					return await whois(params.actor1);
				} catch (e) {
					throw { who: params.actor1, e };
				}
			})(),
			(async () => {
				try {
					return await whois(params.actor2);
				} catch (e) {
					throw { who: params.actor2, e };
				}
			})()
		]);
		actor1 = actor1R;
		actor2 = actor2R;
	} catch (wrappedE) {
		const { e } = wrappedE as { who: string; e: unknown };
		if (e instanceof Response) {
			switch (e.status) {
				case 404:
					if (e instanceof Response && e.status == 404) {
						throw error(404, 'not found');
					}
					throw e;
			}
		}
		throw e;
	}

	const m = await akas(await mutuals([actor1.did, actor2.did]));

	return {
		actor1,
		actor2,
		mutuals: m
	};
};
