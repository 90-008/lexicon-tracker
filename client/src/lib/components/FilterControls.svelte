<script lang="ts">
    interface Props {
        filterRegex: string;
        onFilterChange: (value: string) => void;
    }

    let { filterRegex, onFilterChange }: Props = $props();
</script>

<div
    class="wsbadge !pl-2 !px-1 !mt-0 !font-normal bg-blue-100 dark:bg-blue-900 hover:bg-blue-200 dark:hover:bg-blue-800 border-blue-300 dark:border-blue-700"
>
    <label for="filter-regex" class="text-blue-800 dark:text-gray-200 mr-1">
        filter:
    </label>
    <input
        id="filter-regex"
        value={filterRegex}
        oninput={(e) => onFilterChange((e.target as HTMLInputElement).value)}
        type="text"
        placeholder="regex..."
        class="bg-blue-50 dark:bg-blue-950 text-blue-900 dark:text-gray-400 placeholder-blue-400 dark:placeholder-blue-700 border border-blue-200 dark:border-blue-700 rounded-full px-1 outline-none focus:border-blue-400 min-w-0 w-24"
    />
</div>
