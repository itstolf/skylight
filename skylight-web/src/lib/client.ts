import { throwForStatus } from './fetch';

const HOST = 'https://bsky-stuff.tolf.gay';

function makeURLSearchParams(args: Record<string, string | string[] | null>): URLSearchParams {
	const q = new URLSearchParams();
	for (const k in args) {
		if (!Object.prototype.hasOwnProperty.call(args, k)) {
			continue;
		}
		let vs = args[k];
		if (vs == null) {
			continue;
		}
		if (!Array.isArray(vs)) {
			vs = [vs];
		}
		for (const v of vs) {
			q.append(k, v);
		}
	}
	return q;
}

async function call<R>(
	endpoint: string,
	args: Record<string, string | string[] | null>,
	init?: RequestInit
): Promise<R> {
	return await throwForStatus(
		await fetch(`${HOST}/_/${endpoint}?${makeURLSearchParams(args)}`, init)
	).json();
}

export async function whois(
	actors: string[],
	init?: RequestInit
): Promise<Record<string, { alsoKnownAs: string[]; did: string }>> {
	return (
		await call<{ whois: Record<string, { alsoKnownAs: string[]; did: string }> }>(
			'whois',
			{ actor: actors },
			init
		)
	).whois;
}

export async function neighborhood(
	dids: string[],
	ignoreDids: string[] = [],
	init?: RequestInit
): Promise<{ n: string[]; e: number[][]; t: number }> {
	return await call('neighborhood', { did: dids, ignoreDid: ignoreDids }, init);
}

export async function akas(dids: string[], init?: RequestInit): Promise<Record<string, string[]>> {
	if (dids.length == 0) {
		return {};
	}
	const promises = [];
	const CHUNK_SIZE = 100;
	for (let i = 0; i < dids.length; i += CHUNK_SIZE) {
		promises.push(
			call<{ akas: Record<string, string[]> }>(
				'akas',
				{
					did: dids.slice(i, i + CHUNK_SIZE)
				},
				init
			)
		);
	}
	let r = {};
	for (const resp of await Promise.all(promises)) {
		r = { ...r, ...resp.akas };
	}
	return r;
}

export async function mutuals(dids: string[], init?: RequestInit): Promise<string[]> {
	return (await call<{ mutuals: string[] }>('mutuals', { did: dids }, init)).mutuals;
}

const TEXT_DECODER = new TextDecoder();

export async function* paths(
	sourceDid: string,
	targetDid: string,
	ignoreDids: string[] = [],
	init?: RequestInit
): AsyncIterable<string[]> {
	const resp = throwForStatus(
		await fetch(
			`${HOST}/_/paths?${makeURLSearchParams({
				sourceDid: sourceDid,
				targetDid: targetDid,
				ignoreDid: ignoreDids
			})}`,
			init
		)
	);
	// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
	const reader = resp.body!.getReader();
	let buf = '';
	while (true) {
		const { value, done } = await reader.read();
		if (done) {
			break;
		}
		buf += TEXT_DECODER.decode(value);

		if (buf.indexOf('\n') == -1) {
			continue;
		}

		const parts = buf.split('\n');
		// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
		buf = parts.pop()!;

		for (const part of parts) {
			yield JSON.parse(part);
		}
	}
}
