import { akas, mutuals, whois } from '@/lib/client';
import { error } from '@sveltejs/kit';

import type { PageLoad } from './$types';
export const load: PageLoad = async ({ params }) => {
	const r = await whois([params.actor]);
	if (!Object.prototype.hasOwnProperty.call(r, params.actor)) {
		throw error(404, 'not found');
	}
	const alsoKnownAs = r[params.actor].alsoKnownAs;
	const did = r[params.actor].did;
	const raw = await mutuals([did]);
	const resolved = await akas(raw);

	return {
		did,
		alsoKnownAs,
		mutuals: raw,
		resolved
	};
};
