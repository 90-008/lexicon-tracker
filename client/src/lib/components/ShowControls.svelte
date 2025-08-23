<script lang="ts">
    import type { ShowOption } from "$lib/types";

    interface Props {
        show: ShowOption;
        onShowChange: (value: ShowOption) => void;
    }

    let { show, onShowChange }: Props = $props();

    const showOptions: ShowOption[] = ["server init", "stream start"];
</script>

<div
    class="wsbadge !pl-2 !px-1 !mt-0 !font-normal bg-pink-100 hover:bg-pink-200 border-pink-300"
>
    <label for="show" class="text-pink-800 mr-1"> show since: </label>
    <select
        id="show"
        value={show}
        onchange={(e) =>
            onShowChange((e.target as HTMLSelectElement).value as ShowOption)}
        class="bg-pink-50 text-pink-900 border border-pink-200 rounded-full px-1 outline-none focus:bg-white focus:border-pink-400 min-w-0"
    >
        {#each showOptions as option}
            <option value={option}>{option}</option>
        {/each}
    </select>
</div>
