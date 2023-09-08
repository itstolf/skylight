export default function sleep(ms: number, signal?: AbortSignal): Promise<void> {
	return new Promise((resolve, reject) => {
		signal?.throwIfAborted();
		signal?.addEventListener('abort', function stop() {
			reject(signal?.reason);
			clearTimeout(handle);
		});
		const handle = setTimeout(() => {
			resolve();
			signal?.removeEventListener('abort', stop);
		}, ms);
	});
}
