<script lang="ts">
    import { dev } from "$app/environment";
    import type { EventRecord, NsidCount, SortOption } from "$lib/types";
    import { onMount, onDestroy } from "svelte";
    import { writable } from "svelte/store";
    import { PUBLIC_API_URL } from "$env/static/public";
    import { fetchEvents, fetchTrackingSince } from "$lib/api";
    import { createRegexFilter } from "$lib/filter";
    import StatsCard from "$lib/components/StatsCard.svelte";
    import StatusBadge from "$lib/components/StatusBadge.svelte";
    import EventCard from "$lib/components/EventCard.svelte";
    import FilterControls from "$lib/components/FilterControls.svelte";
    import SortControls from "$lib/components/SortControls.svelte";
    import BskyToggle from "$lib/components/BskyToggle.svelte";
    import RefreshControl from "$lib/components/RefreshControl.svelte";
    import { formatTimestamp } from "$lib/format";

    const events = writable(new Map<string, EventRecord>());
    const pendingUpdates = new Map<string, EventRecord>();
    let eventsList: NsidCount[] = $state([]);
    let updateTimer: NodeJS.Timeout | null = null;
    events.subscribe((value) => {
        eventsList = value
            .entries()
            .map(([nsid, event]) => ({
                nsid,
                ...event,
            }))
            .toArray();
    });
    let per_second = $state(0);
    let tracking_since = $state(0);

    let all: EventRecord = $derived(
        eventsList.reduce(
            (acc, event) => {
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
    let sortBy: SortOption = $state("total");
    let refreshRate = $state("");
    let changedByUser = $state(false);

    let websocket: WebSocket | null = null;
    let isStreamOpen = $state(false);
    let websocketStatus = $state<
        "connecting" | "connected" | "disconnected" | "error"
    >("disconnected");
    const connectToStream = async () => {
        if (isStreamOpen) return;
        websocketStatus = "connecting";
        websocket = new WebSocket(
            `${dev ? "ws" : "wss"}://${PUBLIC_API_URL}/stream_events`,
        );
        websocket.binaryType = "arraybuffer";
        websocket.onopen = () => {
            console.log("ws opened");
            isStreamOpen = true;
            websocketStatus = "connected";
        };
        websocket.onmessage = async (event) => {
            const jsonData = JSON.parse(event.data);

            if (jsonData.per_second > 0) {
                per_second = jsonData.per_second;
            }

            // Store updates in pending map if refresh rate is set
            if (refreshRate) {
                for (const [nsid, event] of Object.entries(jsonData.events)) {
                    pendingUpdates.set(nsid, event as EventRecord);
                }
            } else {
                // Apply updates immediately if no refresh rate
                events.update((map) => {
                    for (const [nsid, event] of Object.entries(
                        jsonData.events,
                    )) {
                        map.set(nsid, event as EventRecord);
                    }
                    return map;
                });
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
        };
    };

    const loadData = async () => {
        try {
            error = null;
            const data = await fetchEvents();
            per_second = data.per_second;
            events.update((map) => {
                for (const [nsid, event] of Object.entries(data.events)) {
                    map.set(nsid, event);
                }
                return map;
            });
            tracking_since = (await fetchTrackingSince()).since;
        } catch (err) {
            error =
                err instanceof Error
                    ? err.message
                    : "an unknown error occurred";
            console.error("error loading data:", err);
        }
    };

    // Update the refresh timer when refresh rate changes
    $effect(() => {
        if (updateTimer) {
            clearInterval(updateTimer);
            updateTimer = null;
        }

        if (refreshRate) {
            const rate = parseInt(refreshRate, 10) * 1000; // Convert to milliseconds
            if (!isNaN(rate) && rate > 0) {
                updateTimer = setInterval(() => {
                    if (pendingUpdates.size > 0) {
                        events.update((map) => {
                            for (const [nsid, event] of pendingUpdates) {
                                map.set(nsid, event);
                            }
                            pendingUpdates.clear();
                            return map;
                        });
                    }
                }, rate);
            }
        }
    });

    onMount(() => {
        loadData();
        connectToStream();
    });

    onDestroy(() => {
        // Clear refresh timer
        if (updateTimer) {
            clearInterval(updateTimer);
            updateTimer = null;
        }
        // Close WebSocket connection
        if (websocket) {
            websocket.close();
        }
    });

    const sortEvents = (events: NsidCount[], sortBy: SortOption) => {
        const sorted = [...events];
        switch (sortBy) {
            case "total":
                sorted.sort(
                    (a, b) =>
                        b.count + b.deleted_count - (a.count + a.deleted_count),
                );
                break;
            case "created":
                sorted.sort((a, b) => b.count - a.count);
                break;
            case "deleted":
                sorted.sort((a, b) => b.deleted_count - a.deleted_count);
                break;
            case "date":
                sorted.sort((a, b) => b.last_seen - a.last_seen);
                break;
        }
        return sorted;
    };

    const filterEvents = (events: NsidCount[]) => {
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
    <title>lexicon tracker</title>
    <meta
        name="description"
        content="tracks bluesky jetstream events by collection"
    />
</svelte:head>

<header class="border-gray-300 border-b mb-4 pb-2">
    <div
        class="px-2 md:ml-[19vw] mx-auto flex flex-wrap items-center text-center"
    >
        <h1 class="text-4xl font-bold mr-4 text-gray-900">lexicon tracker</h1>
        <p class="text-lg mt-1 text-gray-600">
            tracks lexicons seen on the jetstream {tracking_since === 0
                ? ""
                : `(since: ${formatTimestamp(tracking_since)})`}
        </p>
    </div>
</header>
<div class="md:max-w-[61vw] mx-auto p-2">
    <div
        class="min-w-fit grid grid-cols-2 xl:grid-cols-4 gap-2 2xl:gap-6 2xl:mx-16 mb-8"
    >
        <StatsCard
            title="total creation"
            value={all.count}
            colorScheme="green"
        />
        <StatsCard
            title="total deletion"
            value={all.deleted_count}
            colorScheme="red"
        />
        <StatsCard
            title="per second"
            value={per_second}
            colorScheme="turqoise"
        />
        <StatsCard
            title="unique collections"
            value={eventsList.length}
            colorScheme="orange"
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
                <h2 class="text-2xl font-bold text-gray-900">seen lexicons</h2>
                <StatusBadge status={websocketStatus} />
            </div>
            <div class="flex flex-wrap items-center gap-1.5 mb-6">
                <FilterControls
                    {filterRegex}
                    onFilterChange={(value) => (filterRegex = value)}
                />
                <BskyToggle
                    {dontShowBsky}
                    onBskyToggle={() => (dontShowBsky = !dontShowBsky)}
                />
                <SortControls
                    {sortBy}
                    onSortChange={(value: SortOption) => {
                        sortBy = value;
                        if (refreshRate === "" && sortBy === "date")
                            refreshRate = "2";
                        else if (refreshRate === "2" && changedByUser === false)
                            refreshRate = "";
                    }}
                />
                <RefreshControl
                    {refreshRate}
                    onRefreshChange={(value) => {
                        refreshRate = value;
                        changedByUser = refreshRate !== "";
                    }}
                />
            </div>
            <div
                class="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-4"
            >
                {#each sortEvents(filterEvents(eventsList), sortBy) as event, index (event.nsid)}
                    <EventCard {event} {index} />
                {/each}
            </div>
        </div>
    {:else}
        <div class="text-center py-12 bg-gray-50 rounded-lg">
            <div class="text-gray-400 text-4xl mb-4">📊</div>
            <p class="text-gray-600">no events tracked yet. try refreshing?</p>
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
