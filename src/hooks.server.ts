import { track, writeEvents } from "$lib/jetstream.js";

// Start tracking when the server starts
track().catch(console.error);
process.on("SIGINT", writeEvents);
process.on("SIGTERM", writeEvents);
process.on("SIGQUIT", writeEvents);
