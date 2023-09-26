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

async function* jsonl<R>(resp: Response): AsyncIterable<R> {
	const textDecoder = new TextDecoder();

	// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
	const reader = resp.body!.getReader();
	let buf = '';
	while (true) {
		const { value, done } = await reader.read();
		if (done) {
			break;
		}
		buf += textDecoder.decode(value);

		const parts = buf.split('\n');
		// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
		buf = parts.pop()!;

		for (const part of parts) {
			yield JSON.parse(part);
		}
	}
}

export async function unaryCall<R>(
	endpoint: string,
	args: Record<string, string | string[] | null>,
	init?: RequestInit
): Promise<R> {
	return await (await doRequest(endpoint, args, init)).json();
}

export async function* streamingCall<R>(
	endpoint: string,
	args: Record<string, string | string[] | null>,
	init?: RequestInit
): AsyncIterable<R> {
	yield* jsonl<R>(await doRequest(endpoint, args, init));
}
