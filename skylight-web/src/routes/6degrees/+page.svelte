<script lang="ts">
	import { Collapse } from 'sveltestrap';
	import { akas, path, whois } from '@/lib/client';

	let source: string;
	let target: string;
	let ignore: string;
	let ignores: { did: string; alsoKnownAs: string[] }[] = [];
	let maxMutuals: number;
	let showMoreSettings: boolean = false;

	function toggleMoreSettings() {
		showMoreSettings = !showMoreSettings;
	}

	let state:
		| { type: 'pending' }
		| { type: 'ready'; path: string[]; resolved: Record<string, string[]> }
		| { type: 'error'; error: string }
		| null = null;

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

	async function submit() {
		state = { type: 'pending' };

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
			state = { type: 'error', error: msg };
			return;
		}

		let p: string[];
		const controller = new AbortController();
		setTimeout(() => controller.abort(), 10 * 1000);
		try {
			p = await path(
				sourceDid,
				targetDid,
				ignores.map(({ did }) => did),
				maxMutuals,
				{ signal: controller.signal }
			);
		} catch (e) {
			let msg = 'sorry, something broke :(';
			if (e instanceof Response) {
				switch (e.status) {
					case 404:
						msg = 'sorry, you might be too far apart :(';
						break;
					case 408:
						msg = 'sorry, took too long and gave up :(';
						break;
				}
			} else if (e instanceof DOMException && e.name == 'AbortError') {
				msg = 'sorry, took too long and gave up';
			}
			state = { type: 'error', error: msg };
			return;
		}

		if (p.length > 0) {
			const resolved = await akas(p);
			state = { type: 'ready', path: p, resolved };
		} else {
			state = { type: 'error', error: "sorry, couldn't find anything :(" };
		}
		return;
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
			<input
				type="text"
				class="form-control"
				placeholder="from"
				bind:value={source}
				required
				disabled={state != null && state.type == 'pending'}
			/>
		</div>
		<div class="col-auto">
			<input
				type="text"
				class="form-control"
				placeholder="to"
				bind:value={target}
				required
				disabled={state != null && state.type == 'pending'}
			/>
		</div>
		<div class="col-auto">
			<button
				class="btn btn-primary"
				type="submit"
				disabled={state != null && state.type == 'pending'}>find</button
			>
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
									disabled={state != null && state.type == 'pending'}
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
										disabled={state != null && state.type == 'pending'}
										bind:value={ignore}
									/>
								</div>
								<div class="col-auto">
									<button
										class="btn btn-sm btn-primary"
										type="submit"
										disabled={state != null && state.type == 'pending'}>add</button
									>
								</div>
								<div class="col-auto">
									<button
										class="btn btn-sm btn-danger"
										type="button"
										on:click|preventDefault={clearIgnores}>clear</button
									>
								</div>
							</div>
						</li>
					</ul>
				</form>

				<form on:submit|preventDefault={submit}>
					<div class="row">
						<div class="col-auto">
							<input
								class="form-control form-control-sm"
								placeholder="max mutuals"
								disabled={state != null && state.type == 'pending'}
								inputmode="numeric"
								bind:value={maxMutuals}
							/>
						</div>
					</div>
				</form>
			</div>
		</div>
	</Collapse>

	{#if state != null}
		{#if state.type == 'pending'}
			<p>hold on...</p>
		{/if}
		{#if state.type == 'error'}
			<div class="alert alert-danger">{state.error}</div>
		{/if}
		{#if state.type == 'ready'}
			<ol id="path">
				{#each state.path as did}
					{@const handle = (() => {
						let h = did;
						const akas = state.resolved[did];
						if (akas && akas.length > 0) {
							h = akas[0].replace(/^at:\/\//, '');
						}
						return h;
					})()}
					<li>
						<a href="https://bsky.app/profile/{handle}" target="_blank">{handle}</a>
						<sup>
							<button
								class="btn btn-sm btn-outline-secondary py-0 px-1"
								on:click|preventDefault={addIgnore.bind(null, {
									did,
									alsoKnownAs: state.resolved[did]
								})}
							>
								<small>+exclude</small>
							</button>
						</sup>
					</li>
				{/each}
			</ol>
		{/if}
	{/if}
</div>
