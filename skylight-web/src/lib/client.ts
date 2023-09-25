import { streamingCall, unaryCall } from "./transport";

export async function whois(
	actors: string[],
	init?: RequestInit
): Promise<Record<string, { alsoKnownAs: string[]; did: string }>> {
	return (
		await unaryCall<{ whois: Record<string, { alsoKnownAs: string[]; did: string }> }>(
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
	return await unaryCall('neighborhood', { did: dids, ignoreDid: ignoreDids }, init);
}

export async function akas(dids: string[], init?: RequestInit): Promise<Record<string, string[]>> {
	if (dids.length == 0) {
		return {};
	}
	const promises = [];
	const CHUNK_SIZE = 100;
	for (let i = 0; i < dids.length; i += CHUNK_SIZE) {
		promises.push(
			unaryCall<{ akas: Record<string, string[]> }>(
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
	return (await unaryCall<{ mutuals: string[] }>('mutuals', { did: dids }, init)).mutuals;
}

export async function* paths(
	sourceDid: string,
	targetDid: string,
	ignoreDids: string[] = [],
	init?: RequestInit
): AsyncIterable<string[]> {
	yield* streamingCall<string[]>(
		'paths',
		{
			sourceDid: sourceDid,
			targetDid: targetDid,
			ignoreDid: ignoreDids
		},
		init
	);
}
