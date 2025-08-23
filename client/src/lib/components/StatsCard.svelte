<script lang="ts">
    import { formatNumber } from "$lib/format";

    const colorClasses = {
        green: {
            bg: "from-green-50 to-green-100 dark:from-green-900 dark:to-green-800",
            border: "border-green-200 dark:border-green-800",
            titleText: "text-green-700 dark:text-green-400",
            valueText: "text-green-900 dark:text-green-200",
        },
        red: {
            bg: "from-red-50 to-red-100 dark:from-red-900 dark:to-red-800",
            border: "border-red-200 dark:border-red-800",
            titleText: "text-red-700 dark:text-red-400",
            valueText: "text-red-900 dark:text-red-200",
        },
        turqoise: {
            bg: "from-teal-50 to-teal-100 dark:from-teal-900 dark:to-teal-800",
            border: "border-teal-200 dark:border-teal-800",
            titleText: "text-teal-700 dark:text-teal-400",
            valueText: "text-teal-900 dark:text-teal-200",
        },
        orange: {
            bg: "from-orange-50 to-orange-100 dark:from-orange-900 dark:to-orange-800",
            border: "border-orange-200 dark:border-orange-800",
            titleText: "text-orange-700 dark:text-orange-400",
            valueText: "text-orange-900 dark:text-orange-200",
        },
    };

    interface Props {
        title: string;
        value: number;
        colorScheme: keyof typeof colorClasses;
    }

    let { title, value, colorScheme }: Props = $props();

    const colors = $derived(colorClasses[colorScheme]);
</script>

<div class="bg-gradient-to-r {colors.bg} p-3 rounded-lg border {colors.border}">
    <h3 class="text-base font-medium {colors.titleText} mb-2">
        {title}
    </h3>
    <p class="text-xl md:text-2xl font-bold {colors.valueText}">
        {formatNumber(value)}
    </p>
</div>
