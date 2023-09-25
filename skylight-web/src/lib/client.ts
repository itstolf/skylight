const HOST = 'https://bsky-stuff.tolf.gay';

function throwForStatus(resp: Response): Response {
	if (!resp.ok) {
		throw resp;
	}
	return resp;
}

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

function makeURL(endpoint: string, args: Record<string, string | string[] | null>): string {
	return `${HOST}/_/${endpoint}?${makeURLSearchParams(args)}`;
}

async function doRequest(
	endpoint: string,
	args: Record<string, string | string[] | null>,
	init?: RequestInit
): Promise<Response> {
	return throwForStatus(await fetch(makeURL(endpoint, args), init));
}

async function unaryCall<R>(
	endpoint: string,
	args: Record<string, string | string[] | null>,
	init?: RequestInit
): Promise<R> {
	return await (await doRequest(endpoint, args, init)).json();
}

const TEXT_DECODER = new TextDecoder();
async function* streamingCall<R>(
	endpoint: string,
	args: Record<string, string | string[] | null>,
	init?: RequestInit
): AsyncIterable<R> {
	const resp = await doRequest(endpoint, args, init);
	// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
	const reader = resp.body!.getReader();
	let buf = '';
	while (true) {
		const { value, done } = await reader.read();
		if (done) {
			break;
		}
		buf += TEXT_DECODER.decode(value);

		const parts = buf.split('\n');
		// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
		buf = parts.pop()!;

		for (const part of parts) {
			yield JSON.parse(part);
		}
	}
}

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
	for await (const part of streamingCall<string[]>(
		'paths',
		{
			sourceDid: sourceDid,
			targetDid: targetDid,
			ignoreDid: ignoreDids
		},
		init
	)) {
		yield part;
	}
}
