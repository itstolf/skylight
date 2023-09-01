import { throwForStatus } from './fetch';

const HOST = 'https://bsky-stuff.tolf.gay';

async function call<R>(
	endpoint: string,
	args: Record<string, string | string[] | null>,
	init?: RequestInit
): Promise<R> {
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
	return await throwForStatus(await fetch(`${HOST}/_/${endpoint}?${q}`, init)).json();
}

export async function whois(
	actor: string,
	init?: RequestInit
): Promise<{ alsoKnownAs: string[]; did: string }> {
	return await call('whois', { actor }, init);
}

export async function neighborhood(
	did: string,
	ignoreDids: string[] = [],
	init?: RequestInit
): Promise<{ n: string[]; e: number[][]; t: number }> {
	return await call('neighborhood', { did, ignoreDid: ignoreDids }, init);
}

export async function akas(dids: string[], init?: RequestInit): Promise<Record<string, string[]>> {
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

export async function path(
	sourceDid: string,
	targetDid: string,
	ignoreDids: string[] = [],
	maxMutuals: number | null = null,
	init?: RequestInit
): Promise<string[]> {
	return (
		await call<{ path: string[] }>(
			'path',
			{
				sourceDid: sourceDid,
				targetDid: targetDid,
				ignoreDid: ignoreDids,
				maxMutuals: maxMutuals != null ? maxMutuals.toString() : null
			},
			init
		)
	).path;
}

export async function mutuals(did: string, init?: RequestInit): Promise<string[]> {
	return (await call<{ mutuals: string[] }>('mutuals', { did }, init)).mutuals;
}

export async function mutuals2(did1: string, did2: string, init?: RequestInit): Promise<string[]> {
	return (await call<{ mutuals: string[] }>('mutuals2', { did1, did2 }, init)).mutuals;
}
