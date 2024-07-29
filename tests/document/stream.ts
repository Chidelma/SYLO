import { generateHeapSnapshot } from "bun";

const heap = generateHeapSnapshot();

await Bun.write(Bun.file("heap.json"), JSON.stringify(heap, null, 2));