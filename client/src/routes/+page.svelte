<script lang="ts">
    import { dev } from "$app/environment";
    import type { EventRecord } from "$lib/types";
    import { onMount, onDestroy } from "svelte";
    import { get, writable } from "svelte/store";
    import StatsCard from "$lib/components/StatsCard.svelte";
    import StatusBadge from "$lib/components/StatusBadge.svelte";
    import EventCard from "$lib/components/EventCard.svelte";
    import FilterControls from "$lib/components/FilterControls.svelte";
    import { PUBLIC_API_URL } from "$env/static/public";

    const events = writable(new Map<string, EventRecord>());
    let eventsList: { nsid: string; event: EventRecord }[] = $state([]);
    events.subscribe((value) => {
        eventsList = value
            .entries()
            .map(([nsid, event]) => ({
                nsid,
                event,
            }))
            .toArray();
        eventsList.sort((a, b) => b.event.count - a.event.count);
    });

    // Backpressure system
    let eventBuffer: { nsid: string; event: EventRecord }[] = [];
    let updateTimer: number | null = null;
    let bufferedEventsCount = $state(0);
    const BATCH_SIZE = 10;
    const UPDATE_INTERVAL = 100; // ms

    const flushEventBuffer = () => {
        if (eventBuffer.length === 0) return;

        events.update((map) => {
            for (const { nsid, event } of eventBuffer) {
                map.set(nsid, event);
            }
            return map;
        });

        eventBuffer = [];
        bufferedEventsCount = 0;
    };

    const scheduleUpdate = () => {
        if (updateTimer !== null) return;

        updateTimer = window.setTimeout(() => {
            flushEventBuffer();
            updateTimer = null;
        }, UPDATE_INTERVAL);
    };
    let all: EventRecord = $derived(
        eventsList.reduce(
            (acc, { nsid, event }) => {
                return {
                    last_seen:
                        acc.last_seen > event.last_seen
                            ? acc.last_seen
                            : event.last_seen,
                    count: acc.count + event.count,
                    deleted_count: acc.deleted_count + event.deleted_count,
                };
            },
            {
                last_seen: 0,
                count: 0,
                deleted_count: 0,
            },
        ),
    );
    let error: string | null = $state(null);
    let filterRegex = $state("");
    let dontShowBsky = $state(false);

    let websocket: WebSocket | null = null;
    let isStreamOpen = $state(false);
    let websocketStatus = $state<
        "connecting" | "connected" | "disconnected" | "error"
    >("disconnected");
    const connectToStream = async () => {
        if (isStreamOpen) return;
        websocketStatus = "connecting";
        websocket = new WebSocket(`ws://${PUBLIC_API_URL}/stream_events`);
        websocket.binaryType = "arraybuffer";
        websocket.onopen = () => {
            console.log("ws opened");
            isStreamOpen = true;
            websocketStatus = "connected";
        };
        websocket.onmessage = async (event) => {
            const view = new DataView(event.data);
            const decoder = new TextDecoder("utf-8");
            const jsonStr = decoder.decode(view);
            const jsonData = JSON.parse(jsonStr);

            // Add to buffer instead of immediate update
            eventBuffer.push({ nsid: jsonData.nsid, event: jsonData });
            bufferedEventsCount = eventBuffer.length;

            // If buffer is full, flush immediately
            if (eventBuffer.length >= BATCH_SIZE) {
                if (updateTimer !== null) {
                    window.clearTimeout(updateTimer);
                    updateTimer = null;
                }
                flushEventBuffer();
            } else {
                // Otherwise schedule a delayed update
                scheduleUpdate();
            }
        };
        websocket.onerror = (error) => {
            console.error("ws error:", error);
            websocketStatus = "error";
        };
        websocket.onclose = () => {
            console.log("ws closed");
            isStreamOpen = false;
            websocketStatus = "disconnected";

            // Flush any remaining events when connection closes
            if (updateTimer !== null) {
                window.clearTimeout(updateTimer);
                updateTimer = null;
            }
            flushEventBuffer();
        };
    };

    const loadData = async () => {
        try {
            error = null;

            const response = await fetch(`http://${PUBLIC_API_URL}/events`);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            events.update((map) => {
                for (const event of data.events) {
                    map.set(event.nsid, event);
                }
                return map;
            });
        } catch (err) {
            error =
                err instanceof Error
                    ? err.message
                    : "an unknown error occurred";
            console.error("error loading data:", err);
        }
    };

    onMount(() => {
        loadData();
        connectToStream();
    });

    onDestroy(() => {
        // Clean up timer and flush any remaining events
        if (updateTimer !== null) {
            window.clearTimeout(updateTimer);
            updateTimer = null;
        }
        flushEventBuffer();

        // Close WebSocket connection
        if (websocket) {
            websocket.close();
        }
    });

    const formatNumber = (num: number): string => {
        return num.toLocaleString();
    };

    const formatTimestamp = (timestamp: number): string => {
        return new Date(timestamp / 1000).toLocaleString();
    };

    const createRegexFilter = (pattern: string): RegExp | null => {
        if (!pattern) return null;

        try {
            // Check if pattern contains regex metacharacters
            const hasRegexChars = /[.*+?^${}()|[\]\\]/.test(pattern);

            if (hasRegexChars) {
                // Use as regex with case-insensitive flag
                return new RegExp(pattern, "i");
            } else {
                // Smart case: case-insensitive unless pattern has uppercase
                const hasUppercase = /[A-Z]/.test(pattern);
                const flags = hasUppercase ? "" : "i";
                // Escape the pattern for literal matching
                const escapedPattern = pattern.replace(
                    /[.*+?^${}()|[\]\\]/g,
                    "\\$&",
                );
                return new RegExp(escapedPattern, flags);
            }
        } catch (e) {
            // Invalid regex, return null
            return null;
        }
    };

    const filterEvents = (events: { nsid: string; event: EventRecord }[]) => {
        let filtered = events;

        // Apply regex filter
        if (filterRegex) {
            const regex = createRegexFilter(filterRegex);
            if (regex) {
                filtered = filtered.filter((e) => regex.test(e.nsid));
            }
        }

        // Apply app.bsky filter
        if (dontShowBsky) {
            filtered = filtered.filter((e) => !e.nsid.startsWith("app.bsky."));
        }

        return filtered;
    };
</script>

<svelte:head>
    <title>bluesky event tracker</title>
    <meta name="description" content="tracks bluesky events by collection" />
</svelte:head>

<div class="md:max-w-[60vw] mx-auto p-2">
    <header class="text-center mb-8">
        <h1 class="text-4xl font-bold mb-2 text-gray-900">
            bluesky event tracker
        </h1>
        <p class="text-gray-600">
            tracking of bluesky events by collection from the jetstream
        </p>
    </header>

    <div
        class="mx-auto w-fit grid grid-cols-2 md:grid-cols-3 gap-2 md:gap-5 mb-8"
    >
        <StatsCard
            title="total creation"
            value={all.count}
            colorScheme="green"
            {formatNumber}
        />
        <StatsCard
            title="total deletion"
            value={all.deleted_count}
            colorScheme="red"
            {formatNumber}
        />
        <StatsCard
            title="unique collections"
            value={eventsList.length}
            colorScheme="orange"
            {formatNumber}
        />
    </div>

    {#if error}
        <div
            class="bg-red-100 border border-red-300 text-red-700 px-4 py-3 rounded-lg mb-6"
        >
            <p>Error: {error}</p>
        </div>
    {/if}

    {#if eventsList.length > 0}
        <div class="mb-8">
            <div class="flex flex-wrap items-center gap-3 mb-3">
                <h2 class="text-2xl font-bold text-gray-900">
                    events by collection
                </h2>
                <StatusBadge status={websocketStatus} />
            </div>
            <FilterControls
                {filterRegex}
                {dontShowBsky}
                onFilterChange={(value) => (filterRegex = value)}
                onBskyToggle={() => (dontShowBsky = !dontShowBsky)}
            />
            <div class="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                {#each filterEvents(eventsList) as { nsid, event }, index (nsid)}
                    <EventCard
                        {nsid}
                        {event}
                        {index}
                        {formatNumber}
                        {formatTimestamp}
                    />
                {/each}
            </div>
        </div>
    {:else}
        <div class="text-center py-12 bg-gray-50 rounded-lg">
            <div class="text-gray-400 text-4xl mb-4">📊</div>
            <p class="text-gray-600">no events tracked yet.</p>
        </div>
    {/if}
</div>

<footer class="py-2 border-t border-gray-200 text-center">
    <p class="text-gray-600 text-sm">
        source code <a
            href="https://tangled.sh/@poor.dog/nsid-tracker"
            target="_blank"
            rel="noopener noreferrer"
            class="text-blue-600 hover:text-blue-800 underline"
            >@poor.dog/nsid-tracker</a
        >
    </p>
</footer>
