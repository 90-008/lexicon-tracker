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
    class="wsbadge !pl-2 !px-1 !mt-0 !font-normal bg-purple-100 dark:bg-purple-800 hover:bg-purple-200 dark:hover:bg-purple-700 border-purple-300 dark:border-purple-700"
>
    <label for="sort-by" class="text-purple-800 dark:text-purple-300 mr-1">
        sort by:
    </label>
    <select
        id="sort-by"
        value={sortBy}
        onchange={(e) =>
            onSortChange((e.target as HTMLSelectElement).value as SortOption)}
        class="bg-purple-50 dark:bg-purple-900 text-purple-900 dark:text-purple-300 border border-purple-200 dark:border-purple-700 rounded-full px-1 outline-none focus:border-purple-400 min-w-0"
    >
        {#each sortOptions as option}
            <option value={option.value}>{option.label}</option>
        {/each}
    </select>
</div>
