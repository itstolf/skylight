<script lang="ts">
	let handle: string = '';

	function submit() {
		if (handle.indexOf('.') == -1 && !handle.match(/^did:/)) {
			handle += '.bsky.social';
		}
		window.location.pathname = `/neighborhoods/${handle}`;
	}
</script>

<svelte:head>
	<title>neighborhoods</title>
</svelte:head>

<div class="container mt-5">
	<h1>neighborhoods</h1>
	<p>which of your mutuals are mutuals with each other?</p>

	<form class="mb-3 row" on:submit|preventDefault={submit}>
		<div class="col-auto">
			<input type="text" class="form-control" placeholder="username" bind:value={handle} required />
		</div>
		<div class="col-auto">
			<button class="btn btn-primary" type="submit" disabled={handle == ''}>go</button>
		</div>
	</form>
</div>
