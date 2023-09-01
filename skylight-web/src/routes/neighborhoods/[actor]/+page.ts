import { akas, mutuals, whois } from '@/lib/client';
import { error } from '@sveltejs/kit';

import type { PageLoad } from './$types';
export const load: PageLoad = async ({ params }) => {
	let alsoKnownAs: string[];
	let did: string;
	try {
		const r = await whois(params.actor);
		alsoKnownAs = r.alsoKnownAs;
		did = r.did;
	} catch (e) {
		if (e instanceof Response && e.status == 404) {
			throw error(404, 'not found');
		}
		throw e;
	}
	const raw = await mutuals(did);
	const resolved = await akas(raw);

	return {
		did,
		alsoKnownAs,
		mutuals: raw,
		resolved
	};
};
