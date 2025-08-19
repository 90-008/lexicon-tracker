<script lang="ts">
    interface Props {
        filterRegex: string;
        onFilterChange: (value: string) => void;
    }

    let { filterRegex, onFilterChange }: Props = $props();
</script>

<div
    class="wsbadge !pl-2 !px-1 !mt-0 !font-normal bg-blue-100 hover:bg-blue-200 border-blue-300"
>
    <label for="filter-regex" class="text-blue-800 mr-1"> filter: </label>
    <input
        id="filter-regex"
        value={filterRegex}
        oninput={(e) => onFilterChange((e.target as HTMLInputElement).value)}
        type="text"
        placeholder="regex..."
        class="bg-blue-50 text-blue-900 placeholder-blue-400 border border-blue-200 rounded-full px-1 outline-none focus:bg-white focus:border-blue-400 min-w-0 w-24"
    />
</div>
