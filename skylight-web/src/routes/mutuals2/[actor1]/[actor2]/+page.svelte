<script lang="ts">
	import type { PageData } from './$types';

	export let data: PageData;

	const handles = Object.entries(data.mutuals).map(([did, alsoKnownAs]) => {
		let handle = did;
		if (alsoKnownAs && alsoKnownAs.length > 0) {
			handle = alsoKnownAs[0].replace(/^at:\/\//, '');
		}
		return handle;
	});
	handles.sort();

	let actor1 = data.actor1.did;
	if (data.actor1.alsoKnownAs && data.actor1.alsoKnownAs.length > 0) {
		actor1 = data.actor1.alsoKnownAs[0].replace(/^at:\/\//, '');
	}

	let actor2 = data.actor2.did;
	if (data.actor2.alsoKnownAs && data.actor2.alsoKnownAs.length > 0) {
		actor2 = data.actor2.alsoKnownAs[0].replace(/^at:\/\//, '');
	}
</script>

<svelte:head>
	<title>mutual mututals</title>
</svelte:head>

<div class="container mt-5">
	<h1>mutual mutuals</h1>
	<p>which of your mutuals share mutuals with someone else?</p>

	<table class="table table-striped">
		<thead>
			<tr>
				<th class="text-start" colspan="3">
					<a href="https://bsky.app/profile/{actor1}" target="_blank">{actor1}</a>
				</th>
				<th class="text-end" colspan="3">
					<a href="https://bsky.app/profile/{actor2}" target="_blank">{actor2}</a>
				</th>
			</tr>
		</thead>
		<tbody>
			{#each handles as handle}
				<tr>
					<td class="text-start align-middle" colspan="2">
						<a class="btn btn-outline-primary btn-sm" href="/mutuals2/{handle}/{actor2}">←</a>
					</td>

					<td class="text-center align-middle" colspan="2">
						<a href="https://bsky.app/profile/{handle}" target="_blank">{handle}</a>
					</td>

					<td class="text-end align-middle" colspan="2">
						<a class="btn btn-outline-primary btn-sm" href="/mutuals2/{actor1}/{handle}">→</a>
					</td>
				</tr>
			{/each}
		</tbody>
	</table>
</div>
