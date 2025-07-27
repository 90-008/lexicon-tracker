<script lang="ts">
    import { formatNumber, formatTimestamp } from "$lib/format";
    import type { NsidCount } from "$lib/types";
    import { onMount, onDestroy } from "svelte";

    interface Props {
        event: NsidCount;
        index: number;
    }

    let { event, index }: Props = $props();

    // Border animation state
    let borderThickness = $state(0);
    let lastEventTime = $state(0);
    let lastCount = $state(0);
    let lastDeletedCount = $state(0);
    let decayTimer: ReturnType<typeof setTimeout> | null = null;
    let isAnimating = $state(false);

    // Constants for border behavior
    const MAX_BORDER_THICKNESS = 6; // Maximum border thickness in pixels
    const INITIAL_THICKNESS_ADD = 2; // How much thickness to add for first/slow events
    const RAPID_SUCCESSION_THRESHOLD = 50; // ms - events faster than this are considered rapid
    const DECAY_RATE = 0.1; // How much thickness to remove per decay tick
    const DECAY_INTERVAL = 45; // ms between decay ticks

    // Track when event data changes
    $effect(() => {
        const currentTime = Date.now();
        const hasChanged =
            event.count !== lastCount ||
            event.deleted_count !== lastDeletedCount;

        if (hasChanged && (lastCount > 0 || lastDeletedCount > 0)) {
            const timeSinceLastUpdate = currentTime - lastEventTime;

            // Calculate how much thickness to add based on timing
            let thicknessToAdd;
            if (timeSinceLastUpdate < RAPID_SUCCESSION_THRESHOLD) {
                // Rapid succession - add less thickness with a decay factor
                const rapidnessFactor = Math.max(
                    0.1,
                    timeSinceLastUpdate / RAPID_SUCCESSION_THRESHOLD,
                );
                thicknessToAdd = INITIAL_THICKNESS_ADD * rapidnessFactor * 0.5;
            } else {
                // Normal/slow event - add full thickness
                thicknessToAdd = INITIAL_THICKNESS_ADD;
            }

            // Add thickness but cap at maximum
            borderThickness = Math.min(
                MAX_BORDER_THICKNESS,
                borderThickness + thicknessToAdd,
            );
            isAnimating = true;
            lastEventTime = currentTime;

            // Start/restart continuous decay
            if (decayTimer) {
                clearTimeout(decayTimer);
            }
            decayTimer = setTimeout(startDecay, DECAY_INTERVAL);
        }

        lastCount = event.count;
        lastDeletedCount = event.deleted_count;
    });

    const startDecay = () => {
        if (borderThickness <= 0) {
            isAnimating = false;
            decayTimer = null;
            return;
        }

        borderThickness = Math.max(0, borderThickness - DECAY_RATE);

        if (borderThickness > 0) {
            decayTimer = setTimeout(startDecay, DECAY_INTERVAL);
        } else {
            isAnimating = false;
            decayTimer = null;
        }
    };

    onMount(() => {
        // Initialize with current values to avoid triggering animation on mount
        lastCount = event.count;
        lastDeletedCount = event.deleted_count;
        lastEventTime = Date.now();

        // Start continuous decay immediately
        decayTimer = setTimeout(startDecay, DECAY_INTERVAL);
    });

    onDestroy(() => {
        // Clean up decay timer
        if (decayTimer) {
            clearTimeout(decayTimer);
        }
    });
</script>

<div
    class="group flex flex-col gap-2 p-1.5 md:p-3 min-h-64 bg-white border border-gray-200 rounded-lg hover:shadow-lg md:hover:-translate-y-1 transition-all duration-200 transform"
    class:has-activity={isAnimating}
    style="--border-thickness: {borderThickness}px"
>
    <div class="flex items-start gap-2">
        <div
            class="text-sm font-bold text-blue-600 bg-blue-100 px-3 py-1 rounded-full"
        >
            #{index + 1}
        </div>
        <div
            title={event.nsid}
            class="font-mono text-sm text-gray-700 mt-0.5 leading-relaxed rounded-full text-nowrap text-ellipsis overflow-hidden group-hover:overflow-visible group-hover:bg-gray-50 border-gray-100 group-hover:border transition-all px-1"
        >
            {event.nsid}
        </div>
    </div>
    <div class="mt-auto flex flex-col gap-1">
        <div class="text-3xl font-bold text-green-600">
            {formatNumber(event.count)}
            <div class="text-xl">created</div>
        </div>
        <div class="text-3xl font-bold text-red-600">
            {formatNumber(event.deleted_count)}
            <div class="text-xl">deleted</div>
        </div>
    </div>
    <div class="text-xs text-gray-500 mt-auto">
        last: {formatTimestamp(event.last_seen)}
    </div>
</div>

<style>
    .has-activity {
        position: relative;
        transition: all 0.2s ease-out;
    }

    .has-activity::before {
        content: "";
        position: absolute;
        top: calc(-1 * var(--border-thickness));
        left: calc(-1 * var(--border-thickness));
        right: calc(-1 * var(--border-thickness));
        bottom: calc(-1 * var(--border-thickness));
        border: var(--border-thickness) solid rgba(59, 130, 246, 0.8);
        border-radius: calc(0.5rem + var(--border-thickness));
        pointer-events: none;
        transition: all 0.3s ease-out;
    }
</style>
