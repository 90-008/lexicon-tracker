<script lang="ts">
    import { formatNumber } from "$lib/format";

    const colorClasses = {
        green: {
            bg: "from-green-50 to-green-100",
            border: "border-green-200",
            titleText: "text-green-700",
            valueText: "text-green-900",
        },
        red: {
            bg: "from-red-50 to-red-100",
            border: "border-red-200",
            titleText: "text-red-700",
            valueText: "text-red-900",
        },
        turqoise: {
            bg: "from-teal-50 to-teal-100",
            border: "border-teal-200",
            titleText: "text-teal-700",
            valueText: "text-teal-900",
        },
        orange: {
            bg: "from-orange-50 to-orange-100",
            border: "border-orange-200",
            titleText: "text-orange-700",
            valueText: "text-orange-900",
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

<div
    class="bg-gradient-to-r {colors.bg} p-3 md:p-6 rounded-lg border {colors.border}"
>
    <h3 class="text-base font-medium {colors.titleText} mb-2">
        {title}
    </h3>
    <p class="text-3xl font-bold {colors.valueText}">
        {formatNumber(value)}
    </p>
</div>
