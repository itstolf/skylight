<script lang="ts">
	import { Collapse } from 'sveltestrap';
	import { akas, paths, whois } from '@/lib/client';

	const MAX_RESULTS = 100;

	let source: string;
	let target: string;
	let ignore: string;
	let ignores: { did: string; alsoKnownAs: string[] }[] = [];
	let showMoreSettings: boolean = false;

	function toggleMoreSettings() {
		showMoreSettings = !showMoreSettings;
	}

	function addIgnore(entry: { did: string; alsoKnownAs: string[] }) {
		showMoreSettings = true;
		if (ignores.some(({ did }) => did == entry.did)) {
			return;
		}
		ignores.push(entry);
		ignores = ignores;
	}

	function removeIgnore(i: number) {
		ignores.splice(i, 1);
		ignores = ignores;
	}

	function clearIgnores() {
		ignores = [];
	}

	async function submitIgnore() {
		if (ignore.indexOf('.') == -1 && !ignore.match(/^did:/)) {
			ignore += '.bsky.social';
		}
		const r = await whois(ignore);
		addIgnore(r);
		ignore = '';
	}

	let knownDids: Record<string, string[]> = {};
	async function resolveDids(dids: string[]): Promise<string[][]> {
		knownDids = {
			...knownDids,
			...(await akas(dids.filter((did) => !Object.prototype.hasOwnProperty.call(knownDids, did))))
		};
		return dids.map((did) => knownDids[did]);
	}

	let state: { type: 'idle' } | { type: 'running' } | { type: 'error'; why: string } = {
		type: 'idle'
	};
	let ps: { did: string; alsoKnownAs: string[] }[][] | null = null;

	function cmpArray<T>(l: T[], r: T[]): -1 | 0 | 1 {
		if (l.length < r.length) {
			return -1;
		}
		if (l.length > r.length) {
			return 1;
		}
		for (let i = 0; i < l.length; ++i) {
			if (l[i] < r[i]) {
				return -1;
			}
			if (l[i] > r[i]) {
				return 1;
			}
		}
		return 0;
	}

	async function submit() {
		if (source.indexOf('.') == -1 && !source.match(/^did:/)) {
			source += '.bsky.social';
		}
		if (target.indexOf('.') == -1 && !target.match(/^did:/)) {
			target += '.bsky.social';
		}

		let sourceDid: string;
		let targetDid: string;

		try {
			const [sourceR, targetR] = await Promise.all([
				(async () => {
					try {
						return await whois(source);
					} catch (e) {
						throw { who: source, e };
					}
				})(),
				(async () => {
					try {
						return await whois(target);
					} catch (e) {
						throw { who: target, e };
					}
				})()
			]);
			sourceDid = sourceR.did;
			targetDid = targetR.did;
		} catch (wrappedE) {
			const { who, e } = wrappedE as { who: string; e: any };
			let msg = 'sorry, something broke :(';
			if (e instanceof Response) {
				switch (e.status) {
					case 404:
						msg = `sorry, i don't know who ${who} is :(`;
						break;
				}
			}
			state = { type: 'error', why: msg };
			return;
		}

		ps = [];
		const controller = new AbortController();
		let i = 0;
		try {
			const seen = new Set();
			state = { type: 'running' };
			for await (const path of paths(
				sourceDid,
				targetDid,
				ignores.map(({ did }) => did),
				{ signal: controller.signal }
			)) {
				const k = path.join(' ');
				if (seen.has(k)) {
					continue;
				}
				seen.add(k);

				const akas = await resolveDids(path);
				ps.push(path.map((did, i) => ({ did, alsoKnownAs: akas[i] })));
				ps.sort((l, r) =>
					cmpArray(
						l.map((v) => v.alsoKnownAs),
						r.map((v) => v.alsoKnownAs)
					)
				);
				ps = ps;
				i++;
				if (i >= MAX_RESULTS) {
					break;
				}
			}
			state = { type: 'idle' };
		} catch (e) {
			state = { type: 'error', why: 'sorry, something broke :(' };
			console.error(e);
		} finally {
			controller.abort();
		}
	}
</script>

<svelte:head>
	<title>six degrees of bluesky</title>
</svelte:head>

<div class="container mt-5">
	<h1>six degrees of bluesky</h1>
	<p>
		how many mutuals away are you from anyone else on bluesky? put in your username and the other
		person's username and check it out!
	</p>

	<form class="mb-3 row" on:submit|preventDefault={submit}>
		<div class="col-auto">
			<input type="text" class="form-control" placeholder="from" bind:value={source} required />
		</div>
		<div class="col-auto">
			<input type="text" class="form-control" placeholder="to" bind:value={target} required />
		</div>
		<div class="col-auto">
			<button class="btn btn-primary" type="submit" disabled={state.type == 'running'}>find</button>
		</div>
		<div class="col-auto">
			<button
				class="btn btn-outline-secondary"
				type="button"
				on:click|preventDefault={toggleMoreSettings}
				>{showMoreSettings ? 'hide more settings' : 'show more settings'}</button
			>
		</div>
	</form>

	<Collapse isOpen={showMoreSettings}>
		<div class="card mb-3">
			<div class="card-body">
				<form class="row" on:submit|preventDefault={submitIgnore}>
					<h3 class="card-title">exclude list</h3>
					<p>you can put usernames here to avoid searching through them</p>
					<ul class="list-unstyled">
						{#each ignores as ignore, i}
							{@const handle = (() => {
								let h = ignore.did;
								if (ignore.alsoKnownAs && ignore.alsoKnownAs.length > 0) {
									h = ignore.alsoKnownAs[0].replace(/^at:\/\//, '');
								}
								return h;
							})()}
							<li class="mb-2">
								<button
									type="button"
									class="btn btn-sm btn-outline-secondary"
									style="user-select: none"
									on:click={removeIgnore.bind(null, i)}>X</button
								>
								<a href="https://bsky.app/profile/{handle}" target="_blank">{handle}</a>
							</li>
						{/each}
						<li>
							<div class="row">
								<div class="col-auto">
									<input
										class="form-control form-control-sm"
										placeholder="ignore"
										bind:value={ignore}
									/>
								</div>
								<div class="col-auto">
									<button
										class="btn btn-sm btn-primary"
										type="submit"
										disabled={state.type == 'running'}>add</button
									>
								</div>
								<div class="col-auto">
									<button
										class="btn btn-sm btn-danger"
										type="button"
										disabled={state.type == 'running'}
										on:click|preventDefault={clearIgnores}>clear</button
									>
								</div>
							</div>
						</li>
					</ul>
				</form>
			</div>
		</div>
	</Collapse>

	{#if state.type == 'error'}
		<div class="alert alert-danger">{state.why}</div>
	{/if}
	{#if ps != null}
		{#if state.type == 'running'}
			<p>
				finding up to {MAX_RESULTS} paths ({ps.length} so far...)
			</p>
		{/if}
		{#if state.type == 'idle'}
			<p>
				found {ps.length} paths (out of a limit of {MAX_RESULTS})
			</p>
		{/if}
		<table class="table">
			{#each ps as path}
				<tr>
					{#each path as segment}
						{@const handle = (() => {
							let h = segment.did;
							if (segment.alsoKnownAs && segment.alsoKnownAs.length > 0) {
								h = segment.alsoKnownAs[0].replace(/^at:\/\//, '');
							}
							return h;
						})()}
						<td class="text-nowrap"
							><a href="https://bsky.app/profile/{handle}" target="_blank">{handle}</a></td
						>
					{/each}
				</tr>
			{/each}
		</table>
	{/if}
</div>
