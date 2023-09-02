<script lang="ts">
	import { Offcanvas } from 'sveltestrap';
	import { UndirectedGraph } from 'graphology';
	import louvain from 'graphology-communities-louvain';
	import forceatlas2 from 'graphology-layout-forceatlas2';
	import FA2LayoutSupervisor from 'graphology-layout-forceatlas2/worker';

	import palette from '@/lib/palette';

	import { onMount, onDestroy } from 'svelte';
	import type { PageData } from './$types';
	import { neighborhood } from '@/lib/client';
	import type Sigma from 'sigma';

	export let data: PageData;

	const IGNORE_DIDS = [
		'did:plc:jdkvwye2lf4mingzk7qdebzc' // furryli.st
	];

	const me = (() => {
		let h = data.did;
		const akas = data.alsoKnownAs;
		if (akas && akas.length > 0) {
			h = akas[0].replace(/^at:\/\//, '');
		}
		return h;
	})();

	let selectedNode: string | null = null;
	let neighbors: { n: string[]; e: number[][] } | null = null;
	let setHighlight: ((nodes: string[]) => void) | null = null;
	type NodeAttributes = {
		label: string;
		did: string;
		x: number;
		y: number;
		color?: string;
		size?: number;
	};
	let graph: UndirectedGraph<NodeAttributes> | null = null;

	async function renderGraph(
		container: HTMLElement,
		raw: { n: string[]; e: number[][] },
		resolved: Record<string, string[]>
	) {
		const { Sigma } = await import('sigma');

		const g = new UndirectedGraph<NodeAttributes>();

		for (let i = 0; i < raw.n.length; ++i) {
			let label = raw.n[i];
			const aka = resolved[raw.n[i]];
			if (aka && aka.length > 0) {
				label = aka[0].replace(/^at:\/\//, '');
			}
			g.addNode(i.toString(), {
				label,
				did: raw.n[i],
				x: (Math.random() - 0.5) * 10,
				y: (Math.random() - 0.5) * 10
			});
		}
		for (let i = 0; i < raw.e.length; ++i) {
			for (const j of raw.e[i]) {
				if (g.hasEdge(i, j)) {
					continue;
				}
				g.addEdge(i, j);
			}
		}

		graph = g;

		const communities = louvain(g);

		g.forEachNode((node) => {
			g.setNodeAttribute(node, 'size', g.degree(node) ** 0.5 * 5);
			g.setNodeAttribute(node, 'color', palette[communities[node]]);
		});

		const layoutSupervisor = new FA2LayoutSupervisor(g, {
			settings: forceatlas2.inferSettings(g)
		});
		layoutSupervisor.start();

		const renderer = new Sigma(g, container, {
			labelFont: '"Comic Sans MS", sans-serif'
		});

		setHighlight = (nodes: string[]) => {
			if (nodes.length == 0) {
				g.forEachNode((node) => {
					g.setNodeAttribute(node, 'color', palette[communities[node]]);
				});
				g.forEachEdge((edge) => {
					g.removeEdgeAttribute(edge, 'size');
					g.removeEdgeAttribute(edge, 'color');
				});
				return;
			}

			g.forEachNode((node) => {
				g.setNodeAttribute(node, 'color', 'rgba(0, 0, 0, 0.1)');
			});
			g.forEachEdge((edge) => {
				g.removeEdgeAttribute(edge, 'size');
				g.setEdgeAttribute(edge, 'color', 'rgba(0, 0, 0, 0.1)');
			});

			nodes.reverse();
			for (const node of nodes) {
				g.setNodeAttribute(node, 'color', palette[communities[node]]);
				for (const neighbor of nodes.length > 1 ? nodes : g.neighbors(node)) {
					const edge = g.edge(node, neighbor);
					if (edge == null) {
						continue;
					}
					g.setEdgeAttribute(edge, 'color', palette[communities[neighbor]]);
					g.setEdgeAttribute(edge, 'size', 5);
					g.setNodeAttribute(neighbor, 'color', palette[communities[neighbor]]);
				}
			}
		};

		renderer.addListener('clickNode', (e) => {
			if (selectedNode == e.node) {
				const selectedHandle = g.getNodeAttribute(e.node, 'label');
				window.location.pathname = `/neighborhoods/${selectedHandle}`;
				return;
			}
			selectedNode = e.node;
			setHighlight!([selectedNode]);
			container.style.cursor = 'pointer';
			renderer.refresh();
		});
		renderer.addListener('clickStage', (_e) => {
			selectedNode = null;
			setHighlight!([]);
			renderer.refresh();
		});
		renderer.addListener('enterNode', function (e) {
			setHighlight!(selectedNode != null ? [selectedNode, e.node] : [e.node]);
			container.style.cursor = selectedNode == e.node ? 'pointer' : 'default';
		});
		renderer.addListener('leaveNode', function (_e) {
			setHighlight!(selectedNode != null ? [selectedNode] : []);
			container.style.cursor = 'default';
		});
		return renderer;
	}

	function onMutualHover(did: string | null) {
		if (setHighlight == null || neighbors == null) {
			return;
		}
		const highlights = selectedNode != null ? [selectedNode] : [];
		setHighlight([
			...highlights,
			...(did != null ? [neighbors.n.findIndex((v) => v == did).toString()] : [])
		]);
	}

	let renderer: Sigma | null = null;
	onMount(async () => {
		neighbors = await neighborhood(data.did, IGNORE_DIDS);
		renderer = await renderGraph(document.getElementById('map')!, neighbors!, data.resolved);
	});
	onDestroy(() => {
		if (renderer != null) {
			renderer.kill();
		}
	});

	const mutuals: { handle: string; did: string }[] = [];
	const ignoreDidsSet = new Set(IGNORE_DIDS);
	for (const did of data.mutuals) {
		if (ignoreDidsSet.has(did)) {
			continue;
		}
		let handle = did;
		const akas = data.resolved[did];
		if (akas && akas.length > 0) {
			handle = akas[0].replace(/^at:\/\//, '');
		}
		mutuals.push({ handle, did });
	}
	mutuals.sort((a, b) => (a.handle < b.handle ? -1 : a.handle > b.handle ? 1 : 0));

	let offcanvasOpen = false;
	function toggleOffcanvas() {
		offcanvasOpen = !offcanvasOpen;
	}
</script>

<svelte:head>
	<title>{me}'s neighborhood</title>
</svelte:head>

<Offcanvas
	isOpen={offcanvasOpen}
	toggle={toggleOffcanvas}
	placement="end"
	class="text-bg-dark d-flex flex-column"
	header="{mutuals.length} neighbors"
>
	<table class="text-bg-dark table table-sm table-borderless" style="width: 100%">
		<thead>
			<tr>
				<th>who</th>
				<th class="text-end" style="width: 100px"># mutuals</th>
			</tr>
		</thead>
		<tbody>
			{#each mutuals as mutual}
				<tr>
					<td>
						<a
							class="text-bg-dark text-break"
							href="/neighborhoods/{mutual.handle}"
							on:focus={onMutualHover.bind(null, mutual.did)}
							on:mouseover={onMutualHover.bind(null, mutual.did)}
							on:mouseleave={onMutualHover.bind(null, null)}
						>
							{mutual.handle}
						</a>
					</td>
					<td class="text-end"
						>{graph != null
							? graph.degree(graph.findNode((_, { did }) => did == mutual.did))
							: '?'}</td
					>
				</tr>
			{/each}
		</tbody>
	</table>
</Offcanvas>

<div id="map" class="vh-100 vw-100 position-absolute top-0 start-0" />
<h1 class="position-absolute top-0 start-0 m-2 h6">
	<a href="https://bsky.app/profile/{me}" target="_blank">{me}</a>'s neighborhood
</h1>

<button class="position-absolute top-0 end-0 m-2 btn btn-primary btn-sm" on:click={toggleOffcanvas}
	>show mutuals</button
>
