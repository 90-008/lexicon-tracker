import { startTracking } from "$lib/jetstream.js";

// Start tracking when the server starts
startTracking().catch(console.error);
