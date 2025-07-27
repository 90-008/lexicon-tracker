<script lang="ts">
    import type { SortOption } from "$lib/types";

    interface Props {
        sortBy: SortOption;
        onSortChange: (value: SortOption) => void;
    }

    let { sortBy, onSortChange }: Props = $props();

    const sortOptions = [
        { value: "total" as const, label: "total count" },
        { value: "created" as const, label: "created count" },
        { value: "deleted" as const, label: "deleted count" },
        { value: "date" as const, label: "newest first" },
    ];
</script>

<div
    class="wsbadge !pl-2 !px-1 !mt-0 !font-normal bg-purple-100 hover:bg-purple-200 border-purple-300"
>
    <label for="sort-by" class="text-purple-800 mr-1"> sort by: </label>
    <select
        id="sort-by"
        value={sortBy}
        onchange={(e) =>
            onSortChange((e.target as HTMLSelectElement).value as SortOption)}
        class="bg-purple-50 text-purple-900 border border-purple-200 rounded-full px-1 outline-none focus:bg-white focus:border-purple-400 min-w-0"
    >
        {#each sortOptions as option}
            <option value={option.value}>{option.label}</option>
        {/each}
    </select>
</div>

<style lang="postcss">
    @reference "../../app.css";
    .wsbadge {
        @apply text-sm font-semibold mt-1.5 px-2.5 py-0.5 rounded-full border;
    }
</style>
